(async function(){
  'use strict';
  const main=document.querySelector('#shop-content');
  if(!main)return;
  const full=document.body?.dataset.storefront==='expanded',home=main.cloneNode(true);
  const HANDLE=/^[a-z0-9]+(?:-[a-z0-9]+)*$/,PRODUCT=/^gid:\/\/shopify\/Product\/[1-9][0-9]{0,19}$/,VARIANT=/^gid:\/\/shopify\/ProductVariant\/[1-9][0-9]{0,19}$/;
  const SORTS=new Set(['featured','price-asc','price-desc','title-asc','title-desc']);
  const SEED_CATEGORIES=['regular-necklaces','beady-necklaces','stud-earrings','hoop-earrings','charm-only'];
  const FILTERS=new Set(['all','necklaces','earrings','bracelets','rings','charms','available',...SEED_CATEGORIES]);
  const CATEGORY_NAMES={necklaces:/\b(?:necklaces?|pendants?)\b/i,earrings:/\b(?:earrings?|studs?|huggies?|hoops?)\b/i,bracelets:/\bbracelets?\b/i,rings:/\brings?\b/i,charms:/\bcharms?\b/i};
  const SEARCH_CONTEXT=new Set(searchWords('a an the for and or my me you your our of on in to with from at by is are be that this these those please show find search look looking want would like can could will give get pull up all any piece pieces jewelry jewellery gift gifts option options pair pairs set sets under below over above within around about budget price prices cost costs cheap cheapest cheaper affordable expensive less more than between maximum minimum max min dollars dollar usd cad eur gbp silver sterling gold filled plated rose solid white yellow metal metals 14k 18k 24k themed'));
  const SECTIONS=new Set(['price','details','options','story','shipping','gifts','customize','catalogue','image','bag','checkout','offers']);
  const PAGE_SIZE=24,MAX_PIECES=1200;
  const state={pageKind:'catalogue',currentHandle:'',focusedHandle:'',search:'',checkedSearch:'',collectionSource:'browse',sort:'featured',filter:'all',contextRevision:0,discoveryRevision:0,activeSection:'catalogue',loading:false,products:[],browse:[],browsePageInfo:{hasNextPage:false,endCursor:null},browseVerifiedAt:0,seedInfo:null,seedFailure:false,seedBrowseStarted:false,limit:PAGE_SIZE,pageInfo:{hasNextPage:false,endCursor:null},current:null,services:null,servicesPending:null,selectedImage:0,verifiedAt:0};
  let navigationVersion=0,request=null,noticeTimer=0,highlightTimer=0,focusTimer=0,storyVersion=0,collectionControls=null,productUI=null,checkoutUI=null,cartLines=null,lineSequence=0;
  let guideBusy=false,guideCommandKey='';
  const CONTROL_CAPABILITIES=Object.freeze({controlVersion:1,mode:'sandbox',actions:Object.freeze(['search','sort','filter','open','highlight','scroll','zoom','bag','checkout','gift','customize','options','select-option','product-quantity','review-add','bag-quantity','bag-remove','gift-preferences','checkout-step','checkout-option','checkout-complete']),quantity:Object.freeze({minimum:1,maximum:20}),reviewBeforeAdd:true,finalOrder:false,addMode:'shopper-confirmation',checkoutMode:'simulation-only',notesMode:'local-session'});
  const known=new Map(),identities=new Map();
  const notice=document.querySelector('#storefront-status')||document.body.appendChild(node('aside','','storefront-notice'));
  notice.setAttribute('role','status');notice.setAttribute('aria-live','polite');
  function node(tag,value,className){const e=document.createElement(tag);if(value!=null)e.textContent=value;if(className)e.className=className;return e;}
  function button(label,className,run){const b=node('button',label,className);b.type='button';if(run)b.addEventListener('click',run);return b;}
  function clean(value,max){return typeof value==='string'?value.replace(/\u0000/g,'').trim().slice(0,max):'';}
  function validHandle(handle){return typeof handle==='string'&&handle.length<=180&&HANDLE.test(handle);}
  function money(value,currency){try{return new Intl.NumberFormat('en',{style:'currency',currency}).format(value)+' '+currency;}catch{return value+' '+currency;}}
  function safeImage(value){try{const u=new URL(value);return u.protocol==='https:'&&!u.username&&!u.password&&!u.port&&['cdn.shopify.com','britesjewelry.com','www.britesjewelry.com'].includes(u.hostname)?u.href:null;}catch{return null;}}
  function picture(p,className){const src=safeImage(p.image);if(!src)return null;const img=document.createElement('img');img.src=src;img.alt=clean(p.imageAlt||p.altText||p.title,300);img.loading='lazy';img.decoding='async';if(className)img.className=className;return img;}
  function validProduct(p,handle){
    if(!p||p.handle!==handle||!validHandle(handle)||!PRODUCT.test(p.id||'')||typeof p.title!=='string'||!p.title||typeof p.description!=='string'||!/^[A-Z]{3}$/.test(p.currency||'')||!Array.isArray(p.variants))return false;
    try{const url=new URL(p.url);if(url.protocol!=='https:'||!['britesjewelry.com','www.britesjewelry.com'].includes(url.hostname)||url.username||url.password||url.port||url.search||url.hash||(url.pathname!=='/products/'+handle&&url.pathname!=='/products/'+handle+'/'))return false;}catch{return false;}
    return p.variants.length<=500&&p.variants.every(v=>v&&VARIANT.test(v.id||'')&&typeof v.title==='string'&&Number.isFinite(v.price)&&v.price>=0&&typeof v.available==='boolean');
  }
  function remember(p){
    known.set(p.handle,p);identities.set(p.id,p.handle);
    try{const saved=[...identities],bagIds=new Set(readCart().map(item=>item.productId)),bag=saved.filter(([id])=>bagIds.has(id));sessionStorage.setItem('brites-sandbox-product-identities',JSON.stringify(bag.concat(saved.filter(([id])=>!bagIds.has(id)).slice(-200)).slice(-250)));}catch{}
    return p;
  }
  try{const saved=JSON.parse(sessionStorage.getItem('brites-sandbox-product-identities')||'[]');if(Array.isArray(saved))saved.slice(-250).forEach(v=>{if(Array.isArray(v)&&PRODUCT.test(v[0]||'')&&validHandle(v[1]))identities.set(v[0],v[1]);});}catch{}
  function checkedProducts(data){
    const seen=new Set();
    return (Array.isArray(data?.products)?data.products:[]).slice(0,250).filter(p=>validProduct(p,p?.handle)&&!seen.has(p.id)&&seen.add(p.id)).map(remember);
  }
  function checkedPageInfo(data,requestedCursor=null){
    // Returned rows may exclude studio-only listings. Only the upstream
    // continuation says whether another public catalogue page is available.
    const info=data?.pageInfo||{},hasNextPage=info.hasNextPage===true;
    if(!hasNextPage)return {hasNextPage:false,endCursor:null};
    const cursor=info.endCursor;
    if(typeof cursor!=='string'||!cursor.trim()||cursor.length>2048||/[\u0000-\u001f]/.test(cursor)||cursor===requestedCursor)throw Error('The next collection page could not be confirmed.');
    return {hasNextPage:true,endCursor:cursor};
  }
  function minimum(p){const candidates=p.variants.filter(v=>v.available||v.availabilityKnown===false);return candidates.length?Math.min(...candidates.map(v=>v.price)):null;}
  function projection(p){const available=p.variants.filter(v=>v.available);return {...p,minPrice:minimum(p),suggestedVariantId:available[0]?.id||null};}
  function categoryMatches(p,category){
    if(SEED_CATEGORIES.includes(category)){
      if(Array.isArray(p.storeCategories)&&p.storeCategories.every(c=>SEED_CATEGORIES.includes(c)))return p.storeCategories.includes(category);
      const title=clean(p.title,300),type=clean(p.type,100),text=title+' '+type,finished=/\b(?:necklaces?|earrings?|studs?|huggies?|hoops?|bracelets?|rings?)\b/i.test(title);
      if(category==='stud-earrings')return categoryMatches(p,'earrings')&&/\bstuds?\b/i.test(text);
      if(category==='hoop-earrings')return categoryMatches(p,'earrings')&&/\b(?:hoops?|huggies?)\b/i.test(text);
      if(category==='charm-only')return !finished&&!/\b(?:engraving|design fee|chain extender)\b/i.test(title)&&/\bcharms?\b/i.test(text);
      if(!categoryMatches(p,'necklaces')||/\bnecklace\s+charms?\b/i.test(title))return false;
      const chains=(Array.isArray(p.options)?p.options:[]).filter(o=>/\bchain\b/i.test(o.name||'')).flatMap(o=>Array.isArray(o.values)?o.values:[]),beady=/\b(?:beads?|beady)\b/i.test(text)||chains.some(value=>/\b(?:beads?|beady)\b/i.test(String(value)));
      return category==='beady-necklaces'?beady:!beady;
    }
    const title=clean(p.title,300),type=clean(p.type,100),pattern=CATEGORY_NAMES[category];if(!pattern)return false;
    // Studio is a grouping used by services and physical custom pieces. Its
    // letters are not "stud", and its charm label is not a piece definition.
    const studio=/^(?:custom\s+charm\s+)?studio(?:\s+(?:services?|components?|add[ -]?ons?))?$/i.test(type);
    const service=/^(?:custom\s+)?(?:design\s+fees?\b|engraving(?:\s+(?:on|for|services?)\b|$))/i.test(title);
    const extender=/\bchain\s+extenders?\b/i.test(title)&&!/\b(?:earrings?|studs?|huggies?|hoops?|bracelets?|rings?)\b/i.test(title);
    if(service||extender)return false;
    // A finished source type outranks descriptive words such as "pendant",
    // "hoop" or "stud" in the name. Charm/Studio types still need title
    // evidence, since they also group finished necklaces and custom earrings.
    if(category==='necklaces'||category==='earrings'){
      if(/^(?:necklaces?|pendants?)$/i.test(type))return category==='necklaces';
      if(/^(?:earrings?|(?:studs?|huggies?|hoops?)(?:\s+earrings?)?)$/i.test(type))return category==='earrings';
      if(category==='necklaces'&&/\bearrings?\b/i.test(title)&&!/\bnecklaces?\b/i.test(title))return false;
    }
    // Only an explicit single-category tag can supplement title/type. Broad
    // body copy and unstructured promotional tags never reclassify a piece.
    const tags=(Array.isArray(p.tags)?p.tags:[]).slice(0,80).flatMap(tag=>{const match=clean(tag,100).match(/^(?:product[ _-]?type|category)\s*:\s*(necklaces?|pendants?|earrings?|studs?|huggies?|hoops?|bracelets?|rings?|charms?)$/i);return match?[match[1]]:[];});
    return pattern.test(title+' '+(studio?'':type)+' '+tags.join(' '));
  }
  function searchWords(value){
    return (clean(value,16000).toLowerCase().match(/[\p{L}\p{N}]+/gu)||[]).map(word=>{
      if(/^(?:bunnies|rabbits?|bunny)$/.test(word))return 'bunny';
      if(word==='leaves')return 'leaf';if(word==='wolves')return 'wolf';
      if(word.length>4&&word.endsWith('ies'))return word.slice(0,-3)+'y';
      return word.length>3&&word.endsWith('s')&&!word.endsWith('ss')?word.slice(0,-1):word;
    });
  }
  function searchPlan(query){
    // A shop search may OR every word. Keep ordinary category/price phrasing,
    // but require substantive listing-name terms rather than just "necklace".
    const raw=clean(query,250).toLowerCase(),categories=Object.keys(CATEGORY_NAMES).filter(category=>CATEGORY_NAMES[category].test(raw));
    const withoutPrice=raw.replace(/\b(?:under|below|over|above|within|around|about|budget|max(?:imum)?|min(?:imum)?|up to|less than|more than|at most|at least|between)\s*(?:(?:is|of|to|:)\s*)?(?:(?:usd|cad|eur|gbp|dollars?)\s*|[$£€]\s*)?\d+(?:\.\d+)?(?:\s*(?:and|to|[-–])\s*(?:(?:usd|cad|eur|gbp|dollars?)\s*|[$£€]\s*)?\d+(?:\.\d+)?)?(?:\s*(?:usd|cad|eur|gbp|dollars?))?/gi,' ').replace(/(?:[$£€]\s*|\b(?:usd|cad|eur|gbp)\s+)\d+(?:\.\d+)?/gi,' ');
    const terms=searchWords(withoutPrice).filter(word=>!SEARCH_CONTEXT.has(word)&&!Object.values(CATEGORY_NAMES).some(pattern=>pattern.test(word)));
    return {categories,terms:[...new Set(terms)]};
  }
  function searchMatches(p,plan){
    if(plan.categories.length&&!plan.categories.some(category=>categoryMatches(p,category)))return false;
    // Body copy may cross-sell unrelated symbols. Only names, canonical
    // handles, live type and literal/explicit motif tags establish this match.
    const tags=(Array.isArray(p.tags)?p.tags:[]).slice(0,80).flatMap(tag=>{const value=clean(tag,100),match=value.match(/^(?:motif|symbol|theme)\s*:\s*([\p{L}\p{N}\s-]+)$/iu);return match?[match[1]]:/^[\p{L}\p{N}-]+$/u.test(value)?[value]:[];});
    const words=new Set(searchWords(p.title+' '+p.handle+' '+(p.type||'')+' '+tags.join(' ')));
    return plan.terms.every(term=>words.has(term));
  }
  function filtered(){
    const checked=state.search===state.checkedSearch,plan=checked&&state.search?searchPlan(state.search):null,words=state.search.toLowerCase().match(/[\p{L}\p{N}-]+/gu)||[];
    let pieces=state.products.filter(p=>{
      const text=(p.title+' '+(p.type||'')+' '+p.description).toLowerCase();
      if(!checked&&words.length&&!words.every(word=>text.includes(word)))return false;
      if(plan&&!searchMatches(p,plan))return false;
      if(state.filter==='available')return p.variants.some(v=>v.available);
      if(state.filter==='all')return true;
      return categoryMatches(p,state.filter);
    });
    if(state.sort==='title-asc'||state.sort==='title-desc')pieces=pieces.slice().sort((a,b)=>(state.sort==='title-desc'?-1:1)*a.title.localeCompare(b.title));
    if(state.sort==='price-asc'||state.sort==='price-desc')pieces=pieces.slice().sort((a,b)=>{
      if(a.currency!==b.currency)return a.currency.localeCompare(b.currency);
      const x=minimum(a),y=minimum(b);if(x==null)return y==null?0:1;if(y==null)return -1;return (state.sort==='price-desc'?-1:1)*(x-y)||a.title.localeCompare(b.title);
    });
    return pieces;
  }
  function snapshot(){
    const pieces=state.pageKind==='product'&&state.current?[state.current]:state.pageKind==='catalogue'?filtered().slice(0,state.limit):[];
    const rendered=new Set(),inView=new Set();main.querySelectorAll('[data-product-handle]').forEach(card=>{rendered.add(card.dataset.productHandle);const r=card.getBoundingClientRect();if(r.height>0&&r.bottom>0&&r.top<(window.innerHeight||800))inView.add(card.dataset.productHandle);});
    const actual=pieces.filter(p=>rendered.has(p.handle)),currentHandle=state.pageKind==='product'&&actual.some(p=>p.handle===state.currentHandle)?state.currentHandle:'',focusedHandle=actual.some(p=>p.handle===state.focusedHandle)?state.focusedHandle:'';
    const chosen=(inView.size?actual.filter(p=>inView.has(p.handle)):actual).slice(0,24);
    [currentHandle,focusedHandle].filter(Boolean).forEach(handle=>{const p=actual.find(p=>p.handle===handle);if(p&&!chosen.some(v=>v.handle===handle)){if(chosen.length>=24)chosen.pop();chosen.push(p);}});
    const visiblePieces=chosen.map(p=>({id:p.id,handle:p.handle,title:clean(p.title,300)}));
    const loadedPieces=(state.pageKind==='product'&&state.current?[state.current]:state.pageKind==='catalogue'?filtered():[]).slice(0,200).map(p=>({id:p.id,handle:p.handle,title:clean(p.title,300),...(Array.isArray(p.storeCategories)?{storeCategories:p.storeCategories.filter(c=>FILTERS.has(c)).slice(0,5)}:{})}));
    return {pageKind:state.pageKind==='catalogue'?'collection':state.pageKind,currentHandle,focusedHandle,visiblePieces,loadedPieces,search:state.search,sort:state.sort,filter:state.filter,contextRevision:state.contextRevision,discoveryRevision:state.discoveryRevision,activeSection:state.activeSection,loading:state.loading,...controlSnapshot()};
  }
  function controlSnapshot(){
    const ui=currentProductUI(),cart=readCart(),lines=cartIdentities(cart),visible=new Set([...main.querySelectorAll('[data-bag-line]')].map(row=>row.dataset.bagLine)),checkout=checkoutUI?.page?.isConnected&&state.pageKind==='checkout'?checkoutUI:null;
    const variant=ui?.product.variants.find(v=>v.id===ui.select.value),q=ui?.quantity(),selectedVariant=variant?{id:variant.id,title:variant.title,price:variant.price,currency:ui.product.currency,available:variant.available,options:(variant.options||[]).map(o=>({name:o.name,value:o.value}))}:null;
    return {controlVersion:1,productControls:ui?{handle:ui.product.handle,productId:ui.product.id,productTitle:ui.product.title,productType:ui.product.type||'',variantId:ui.select.value||null,quantity:q,optionsOpen:!ui.menu.hidden,openedOption:ui.openedOption||null,optionGroups:ui.groups.map(g=>({name:g.name,values:g.values.slice()})),selectedOptions:ui.choices(),selectedVariant,...(variant&&validQuantity(q)?{itemTotalPrice:Math.round(variant.price*q*100)/100}:{}),reviewReady:!!ui.review?.isConnected}:null,bagControls:{lines:cart.flatMap((item,index)=>visible.has(lines[index])?[{lineId:lines[index],productId:item.productId,variantId:'gid://shopify/ProductVariant/'+item.variantId,title:item.title,variant:item.variant,quantity:item.quantity||1,price:item.price,currency:item.currency}]:[]),itemCount:cart.reduce((sum,item)=>sum+(item.quantity||1),0)},checkoutControls:checkout?{step:checkout.step,shipping:checkout.shipping,acknowledged:checkout.check.checked,complete:checkout.completed}:null};
  }
  function currentProductUI(){return productUI?.panel?.isConnected&&state.pageKind==='product'&&!state.loading&&state.current===productUI.product&&state.currentHandle===productUI.product.handle?productUI:null;}
  async function sendGuideCommand(command){
    const input=document.querySelector('#guide-request'),reply=document.querySelector('#guide-request-status');
    if(guideBusy||!clean(command,700))return;
    if(typeof window.BritesConcierge?.sendShopperCommand!=='function'){if(reply)reply.textContent='Your guide is still loading. Try again in a moment.';return;}
    guideBusy=true;if(input)input.value=command;renderGuideCommands();if(reply)reply.textContent='Your guide is working on: '+command;
    try{const result=await window.BritesConcierge.sendShopperCommand(command);if(reply)reply.textContent=result?.ok===false?clean(result.reason||result.message,300)||'Check your guide’s reply before continuing.':'See your guide’s reply and the page result below.';}
    catch{if(reply)reply.textContent='The request could not be completed. Try it in the guide’s message box.';}
    finally{guideBusy=false;renderGuideCommands();}
  }
  function renderGuideCommands(){
    const container=document.querySelector('#guide-commands'),summary=document.querySelector('#guide-selection');if(!container)return;
    const ui=currentProductUI(),pc=controlSnapshot().productControls,commands=[],add=(label,command)=>commands.push({label,command});
    if(state.pageKind==='catalogue'){
      [['regular-necklaces','Regular necklaces'],['beady-necklaces','Beady necklaces'],['stud-earrings','Stud earrings'],['hoop-earrings','Hoop earrings'],['charm-only','Charm-only']].forEach(([,label])=>add(label,'Show me '+label.toLowerCase()));
      filtered().slice(0,2).forEach(p=>add('Open '+p.title,'Open '+p.title));add('Lowest price first','Sort by price low to high');
    }else if(ui){
      add('Highlight price','Highlight the price of this piece');add('Show details','Highlight the details of this piece');
      ui.groups.forEach(group=>add('Open '+group.name,'Open the '+group.name+' menu for this piece'));
      const groups=pc.openedOption?ui.groups.filter(g=>g.name===pc.openedOption):ui.groups.filter(g=>/metal|material/i.test(g.name));
      groups.forEach(group=>group.values.slice(0,8).forEach(value=>add('Choose '+value,'Select '+value+' for this piece')));
      if(pc.selectedVariant){add('Quantity 2','Set quantity to 2');add('Review for my bag','Add this piece to my bag');}
      if(ui.groups.some(g=>/metal|material/i.test(g.name)))add('Compare materials','Compare the published materials for this piece and their prices');
      add('Open my bag','Open my bag');
    }else if(state.pageKind==='bag'){
      const cart=readCart();cart.slice(0,3).forEach(item=>{if(cart.filter(row=>row.title===item.title).length===1){add('Quantity 2 · '+item.title,'Set '+item.title+' quantity to 2 in my bag');add('Remove '+item.title,'Remove '+item.title+' from my bag');}});
      add('Request gift wrapping','Enable gift wrapping');if(cart.length)add('Open test checkout','Open checkout');add('Explore all pieces','Show me all pieces');
    }else if(state.pageKind==='checkout'){
      add('Review step','Show checkout review step');add('Shipping step','Show checkout shipping step');add('Demo express','Select express shipping');add('Confirmation step','Show checkout confirmation step');add('Complete test review','Complete test checkout');add('Open my bag','Open my bag');
    }
    const key=JSON.stringify([commands,guideBusy,state.loading]);if(guideCommandKey!==key){guideCommandKey=key;container.replaceChildren(...commands.slice(0,22).map(({label,command})=>{const control=button(label,'guide-command',()=>void sendGuideCommand(command));control.disabled=guideBusy||state.loading;control.title=command;return control;}));}
    const input=document.querySelector('#guide-request'),submit=document.querySelector('#guide-request-send');if(input)input.disabled=guideBusy;if(submit)submit.disabled=guideBusy;
    if(summary)summary.textContent=pc?pc.selectedVariant?pc.productTitle+' · '+pc.selectedVariant.title+' · quantity '+pc.quantity+' · '+money(pc.itemTotalPrice,pc.selectedVariant.currency)+' item subtotal':pc.productTitle+' · choose each published option to see the exact price':state.pageKind==='bag'?readCart().reduce((sum,item)=>sum+(item.quantity||1),0)+' pieces in your test bag':state.pageKind==='checkout'?'Test checkout · '+(checkoutUI?.step||'review')+' · purchases stay simulated':'Move your pointer over a piece, then ask the guide about it. Choose a request below to see it operate the page.';
  }
  // Subject changes are separate from gaze, scrolling and other page context.
  // Commit only completed views so canceled reads cannot revive an old subject.
  function reviseDiscovery(){if(state.discoveryRevision<Number.MAX_SAFE_INTEGER)state.discoveryRevision++;}
  function publish(){state.contextRevision++;renderGuideCommands();document.dispatchEvent(new CustomEvent('brites-storefront:context',{detail:snapshot()}));}
  function status(message){notice.textContent=clean(message,300);notice.dataset.visible=message?'true':'false';clearTimeout(noticeTimer);if(message)noticeTimer=setTimeout(()=>{notice.dataset.visible='false';},3600);}
  function reduced(){return typeof matchMedia==='function'&&matchMedia('(prefers-reduced-motion: reduce)').matches;}
  function focusedElement(){
    let active=document.activeElement;for(let depth=0;depth<8&&active?.shadowRoot?.activeElement;depth++)active=active.shadowRoot.activeElement;return active;
  }
  function sameRevealFocus(initialFocus){
    const active=focusedElement();
    // Chromium blurs a busy disabled composer to BODY. That application-owned
    // transition is not a new shopper focus choice; an actual new control still
    // permanently withdraws the pending reveal through its focus listener.
    return active===initialFocus||active===document.body&&(!initialFocus?.isConnected||initialFocus.matches?.(':disabled')===true);
  }
  function trackRevealFocus(initialFocus){
    let allowed=true;const root=initialFocus?.getRootNode(),changed=()=>{if(!sameRevealFocus(initialFocus))allowed=false;};document.addEventListener('focusin',changed);if(root!==document){root?.addEventListener('focusin',changed);root?.addEventListener('focusout',changed);}return {allowed:()=>allowed,dispose:()=>{document.removeEventListener('focusin',changed);if(root!==document){root?.removeEventListener('focusin',changed);root?.removeEventListener('focusout',changed);}}};
  }
  function mayRevealCollection(initialFocus){
    if(document.hidden)return false;const active=focusedElement();
    if(!active?.isConnected||active===document.body||active===document.documentElement)return true;
    // A completed read must not scroll a retained collection control, or a
    // newer focus choice, offscreen. An unchanged fixed guide composer can
    // still initiate the ordinary website reveal.
    return !collectionControls?.box?.contains(active)&&active===initialFocus;
  }
  function focusSection(section,highlight=true,{immediate=false}={}){
    if(!SECTIONS.has(section))return false;
    const target=main.querySelector('[data-store-section="'+section+'"]');if(!target)return false;
    clearTimeout(highlightTimer);main.querySelectorAll('.store-highlight').forEach(e=>e.classList.remove('store-highlight'));
    if(highlight){target.classList.add('store-highlight');highlightTimer=setTimeout(()=>target.classList.remove('store-highlight'),2600);}
    try{target.scrollIntoView?.({behavior:immediate?'instant':reduced()?'auto':'smooth',block:'center'});}catch{}
    state.activeSection=section;publish();return true;
  }
  function cancelPending(){navigationVersion++;request?.abort();request=null;checkoutUI?.completionController?.abort();clearTimeout(highlightTimer);clearTimeout(focusTimer);focusTimer=0;main.querySelectorAll('.store-highlight').forEach(e=>e.classList.remove('store-highlight'));state.loading=false;}
  async function get(path,signal){if(typeof fetch!=='function')throw Error('The live selection could not be checked.');const response=await fetch(path,{cache:'no-store',...(signal?{signal}:{})}),data=await response.json();if(!response.ok)throw Error('The live selection could not be checked.');return data;}
  function validateSeed(data){
    const seed=data?.seed,rows=data?.products;if(data?.live!==true||!seed||seed.schema!==1||seed.target!==120||seed.minimum!==100||!Array.isArray(rows)||!rows.length||rows.length>160||!Number.isInteger(seed.loaded)||seed.loaded!==rows.length||typeof seed.complete!=='boolean'||typeof seed.partial!=='boolean'||!Number.isInteger(seed.sourcePages)||seed.sourcePages<0||seed.sourcePages>200||data.pageInfo?.hasNextPage!==false||data.pageInfo?.endCursor!==null)throw Error('The balanced collection could not be verified.');
    const ids=new Set(),handles=new Set(),counts=Object.fromEntries(SEED_CATEGORIES.map(c=>[c,0]));for(const p of rows){if(!validProduct(p,p?.handle)||ids.has(p.id)||handles.has(p.handle)||!Array.isArray(p.storeCategories)||p.storeCategories.length>5||new Set(p.storeCategories).size!==p.storeCategories.length||p.storeCategories.some(c=>!SEED_CATEGORIES.includes(c)))throw Error('The balanced collection contains an unchecked identity or category.');ids.add(p.id);handles.add(p.handle);p.storeCategories.forEach(c=>counts[c]++);}
    const absent=SEED_CATEGORIES.filter(c=>counts[c]===0),complete=rows.length>=100&&absent.length===0;if(SEED_CATEGORIES.some(c=>seed.categoryCounts?.[c]!==counts[c])||seed.complete!==complete||!Array.isArray(seed.unfilledCategories)||new Set(seed.unfilledCategories).size!==seed.unfilledCategories.length||seed.unfilledCategories.length!==absent.length||seed.unfilledCategories.some(c=>!absent.includes(c)))throw Error('The balanced collection coverage did not match its checked pieces.');
    const checkedAt=typeof data.checkedAt==='number'?data.checkedAt:typeof data.checkedAt==='string'?Date.parse(data.checkedAt):NaN;if(!Number.isFinite(checkedAt)||checkedAt<=0||checkedAt>Date.now()+300000)throw Error('The balanced collection check time is unavailable.');return {schema:1,target:120,minimum:100,loaded:rows.length,complete,categoryCounts:counts,unfilledCategories:absent,sourcePages:seed.sourcePages,partial:seed.partial};
  }
  async function catalogueStart(signal){
    if(document.body.dataset.catalogueSeed==='balanced'){try{const data=await get('/api/growth/catalogue?seed=1',signal);data.verifiedSeed=validateSeed(data);return data;}catch(e){if(signal?.aborted)throw e;const data=await get('/api/growth/catalogue?browse=1',signal);data.seedUnavailable=true;return data;}}
    return get('/api/growth/catalogue?browse=1',signal);
  }
  function applySeed(data){state.seedInfo=data.verifiedSeed||null;state.seedFailure=data.seedUnavailable===true;state.seedBrowseStarted=!state.seedInfo;}
  function renderCoverage(){
    main.querySelector('.catalogue-coverage')?.remove();if(document.body.dataset.catalogueSeed!=='balanced'||!main.querySelector('#demo-products'))return;const panel=node('aside',null,'catalogue-coverage');panel.setAttribute('aria-label','Checked live catalogue coverage');const loaded=state.browse.length;panel.append(node('strong',loaded+' distinct live pieces checked'));
    if(state.seedInfo){panel.append(node('p',state.seedInfo.complete?'All five requested jewellery groups are represented in the checked starting collection.':'This is a partial starting collection; at least 100 distinct pieces and all five groups have not yet been confirmed.'));const counts=node('div',null,'coverage-counts');SEED_CATEGORIES.forEach(c=>counts.append(node('span',c.replace(/-/g,' ')+': '+state.seedInfo.categoryCounts[c])));panel.append(counts);if(state.seedInfo.partial)panel.append(node('p','Some public source checks were unavailable. These counts describe the checked pieces only.'));}
    else panel.append(node('p',state.seedFailure?'The balanced start could not be checked. This is the ordinary live browse collection; broader coverage is still being checked.':'Continue browsing for more published pieces.'));
    const progress=node('div',null,'catalogue-progress');progress.setAttribute('aria-label','Checked distinct pieces toward 100');const fill=node('span');fill.style.width=Math.min(100,loaded)+'%';progress.append(fill);panel.append(progress);const controls=main.querySelector('#collection-controls');if(controls)controls.after(panel);else main.prepend(panel);
  }
  function begin(options={}){
    cancelPending();request=new AbortController();const record={version:navigationVersion,controller:request};
    if(options.signal){if(options.signal.aborted)request.abort();else options.signal.addEventListener('abort',()=>{record.controller.abort();if(record.version===navigationVersion){state.loading=false;publish();status('The previous website action was cancelled.');}},{once:true});}
    state.loading=!record.controller.signal.aborted;publish();return record;
  }
  function current(record){return record.version===navigationVersion&&!record.controller.signal.aborted;}
  function commitPage(kind,handle,push=true,{preserveLoading=false}={}){
    state.pageKind=kind;state.currentHandle=kind==='product'?handle:'';state.focusedHandle='';if(!preserveLoading)state.loading=false;
    if(push){const query=kind==='product'?'?product='+encodeURIComponent(handle):kind==='bag'?'?cart=1':kind==='checkout'?'?checkout=1':'';history.pushState({},'','/concierge-sandbox.html'+query);}
    publish();document.dispatchEvent(new CustomEvent('brites-concierge:page'));
  }
  function validQuantity(value){return Number.isInteger(value)&&value>=1&&value<=20;}
  function cartItem(value){if(!value||!PRODUCT.test(value.productId||'')||!/^[1-9][0-9]{0,19}$/.test(value.variantId||'')||typeof value.title!=='string'||typeof value.variant!=='string'||!Number.isFinite(value.price)||value.price<0||!/^[A-Z]{3}$/.test(value.currency||'')||value.quantity!==undefined&&!validQuantity(value.quantity))return null;return {productId:value.productId,title:value.title.slice(0,300),variantId:value.variantId,variant:value.variant.slice(0,300),price:value.price,currency:value.currency,...(value.quantity!==undefined?{quantity:value.quantity}:{})};}
  function readCart(){try{const saved=JSON.parse(sessionStorage.getItem('brites-sandbox-cart')||'[]');return Array.isArray(saved)?saved.slice(-50).map(cartItem).filter(Boolean):[];}catch{return [];}}
  function cartIdentities(cart){const fingerprint=JSON.stringify(cart);if(cartLines?.fingerprint!==fingerprint)cartLines={fingerprint,ids:cart.map(()=>newLineId())};return cartLines.ids.slice();}
  function newLineId(){return 'test-'+Date.now().toString(36)+'-'+(++lineSequence).toString(36);}
  function writeCart(cart,lineIds){const kept=cart.slice(-50);sessionStorage.setItem('brites-sandbox-cart',JSON.stringify(kept));cartLines={fingerprint:JSON.stringify(kept),ids:lineIds?.length===cart.length?lineIds.slice(-50):kept.map(()=>newLineId())};document.dispatchEvent(new CustomEvent('brites:cart-updated',{detail:{sandbox:true}}));}
  function updateBag(){const count=document.querySelector('#bag-count');if(count)count.textContent=String(readCart().reduce((sum,item)=>sum+(item.quantity||1),0));}
  function link(label,url){const a=node('a',label);a.href=url;a.target='_blank';a.rel='noopener noreferrer';return a;}
  function needsCustomizer(p,v){
    const ordinaryCharm=/^charms?(?:[ -]only)?$/i.test(clean(p.type,100))&&!/\b(?:custom|personalized|personalised|engraving|design fee|chain extender|components?|add[ -]?ons?)\b/i.test(p.title+' '+p.type);
    if(!v||p.cartHold||p.recommendationHold||p.partsOnly&&!ordinaryCharm||/\bcharms?\b/i.test(p.type||'')&&!ordinaryCharm||/\b(?:components?|add[ -]?ons?|custom[ -]?only)\b/i.test(p.type||''))return true;
    const opts=(v.options||[]).filter(o=>/engrav|personali[sz]|custom/i.test(o.name)),detail=v.title+' '+opts.map(o=>o.value).join(' ');
    if(/handwrit|monogram|photo|upload|custom studio/i.test(p.title))return true;
    if(/no engraving|not engraved|without engraving|non[ -]?engraved|unengraved/i.test(detail)||opts.some(o=>/^(?:no|none|without|not included)$/i.test(o.value)))return false;
    return /engrav|personali[sz]|custom/i.test(detail)||opts.some(o=>/^(?:yes|included|with)$/i.test(o.value));
  }
  function controls(){
    const box=main.querySelector('#collection-controls');if(!box)return;
    // Replacing this form discards keyboard focus as soon as a search starts.
    // Keep our live controls; update their values without moving user focus.
    if(collectionControls?.box===box&&box.contains(collectionControls.input)&&box.contains(collectionControls.sort)){
      if(collectionControls.input.value!==state.search)collectionControls.input.value=state.search;
      if(collectionControls.sort.value!==state.sort)collectionControls.sort.value=state.sort;
      return;
    }
    box.replaceChildren();const form=node('form',null,'collection-tools'),wrap=node('div',null,'search-wrap');
    wrap.innerHTML='<svg viewBox="0 0 24 24" width="18" height="18" aria-hidden="true"><circle cx="10.5" cy="10.5" r="6.5" fill="none" stroke="currentColor" stroke-width="1.3"/><path d="m15.5 15.5 5 5" fill="none" stroke="currentColor" stroke-width="1.3"/></svg>';
    const input=document.createElement('input');input.id='store-search';input.type='search';input.maxLength=250;input.placeholder='Search a symbol, piece or material…';input.setAttribute('aria-label','Search the live jewelry collection');input.value=state.search;
    input.addEventListener('input',()=>{const resetScope=state.search!==''||state.filter!=='all'||state.collectionSource!=='browse';cancelPending();state.search=clean(input.value,250);if(!state.search){state.filter='all';if(state.browseVerifiedAt){restoreBrowse();if(resetScope)reviseDiscovery();}}state.limit=PAGE_SIZE;drawGrid();publish();});
    const submit=button('Search');submit.type='submit';wrap.append(input,submit);form.append(wrap);
    const sortLabel=node('label','Sort','tool-select tool-sort'),sort=document.createElement('select');sort.id='store-sort';sort.setAttribute('aria-label','Sort jewelry');
    [['featured','Shop order'],['price-asc','Price: low to high'],['price-desc','Price: high to low'],['title-asc','Name: A–Z'],['title-desc','Name: Z–A']].forEach(([value,label])=>{const o=node('option',label);o.value=value;sort.append(o);});sort.value=state.sort;sort.addEventListener('change',()=>void execute({type:'sort',sort:sort.value}));sortLabel.append(sort);form.append(sortLabel);box.append(form);
    form.addEventListener('submit',e=>{e.preventDefault();void execute({type:'search',query:input.value});});
    const chips=node('div',null,'category-chips');chips.setAttribute('aria-label','Jewelry categories');
    [['all','All pieces'],['regular-necklaces','Regular necklaces'],['beady-necklaces','Beady necklaces'],['stud-earrings','Stud earrings'],['hoop-earrings','Hoop earrings'],['charm-only','Charm-only'],['necklaces','All necklaces'],['earrings','All earrings'],['bracelets','Bracelets'],['rings','Rings'],['charms','Charms'],['available','Available now']].forEach(([value,label])=>{const b=button(label,'',()=>void execute({type:'filter',filter:value}));b.dataset.filter=value;b.setAttribute('aria-pressed',String(state.filter===value));chips.append(b);});box.append(chips);
    collectionControls={box,input,sort};
  }
  function drawGrid(){
    const grid=main.querySelector('#demo-products');if(!grid)return;
    // A replaced card is no longer attended, even when a stationary pointer
    // happens to occupy the new card's position. Wait for real new attention.
    clearTimeout(focusTimer);focusTimer=0;state.focusedHandle='';
    grid.replaceChildren();const pieces=filtered(),shown=pieces.slice(0,state.limit);
    shown.forEach(p=>{
      const card=node('article',null,'piece-card card-enter');card.dataset.productHandle=p.handle;card.dataset.productId=p.id;
      const a=document.createElement('a');a.href='/concierge-sandbox.html?product='+encodeURIComponent(p.handle);a.setAttribute('aria-label','View '+p.title);
      const photo=node('div',null,'piece-photo'),img=picture(p);photo.append(img||node('span','b.','piece-placeholder'));if(p.type)photo.append(node('span',p.type,'piece-type'));
      const copy=node('div',null,'piece-copy');copy.append(node('h3',p.title));const amount=minimum(p),unconfirmed=p.variants.some(v=>v.availabilityKnown===false);copy.append(node('p',amount==null?'Currently unavailable':'From '+money(amount,p.currency),'piece-price'));if(unconfirmed)copy.append(node('p','Availability checked when you open this piece','piece-meta'));const opts=(p.options||[]).map(o=>o.name).filter(Boolean).slice(0,2);if(opts.length)copy.append(node('p',opts.join(' · '),'piece-meta'));a.append(photo,copy);card.append(a);grid.append(card);
    });
    if(!shown.length){const empty=node('div',null,'empty-collection');empty.append(node('h3','A little more room to explore'),node('p',state.loading?'Checking the live shop for your search…':'No loaded pieces match those choices. Search the live shop, try another symbol, or clear the filters.'),button('Show all pieces','secondary',()=>void execute({type:'search',query:'',filter:'all'})));grid.append(empty);}
    const summary=main.querySelector('#result-summary');if(summary)summary.textContent=(state.loading?'Checking the live shop · ':'')+shown.length+' of '+pieces.length+' loaded pieces'+(state.search?' matching “'+state.search+'”':'')+(state.pageInfo.hasNextPage&&!state.search?' · more collection pages available':'')+(state.filter!=='all'?' · '+state.filter:'');
    const more=main.querySelector('#collection-more');if(more){more.replaceChildren();if(pieces.length>state.limit||!state.search&&(state.pageInfo.hasNextPage||state.seedInfo&&!state.seedBrowseStarted&&state.browse.length<MAX_PIECES))more.append(button('Explore more pieces','secondary',()=>void loadMore()),node('p','Continue through the public Brites catalogue.'));}
    main.querySelectorAll('[data-filter]').forEach(e=>e.setAttribute('aria-pressed',String(e.dataset.filter===state.filter)));
    const hero=main.querySelector('#intro-photo');if(hero&&!hero.querySelector('img')&&shown[0]){const img=picture(shown[0]);if(img){img.loading='eager';hero.replaceChildren(img);}}
    renderCoverage();
  }
  function restoreCollection(){
    if(state.pageKind!=='catalogue'||!main.querySelector('#demo-products'))main.replaceChildren(...[...home.childNodes].map(n=>n.cloneNode(true)));
    state.pageKind='catalogue';state.currentHandle='';state.focusedHandle='';state.current=null;state.activeSection='catalogue';controls();drawGrid();if(full)renderServiceStrip();
  }
  function restoreBrowse(){
    state.products=state.browse.slice();state.search='';state.checkedSearch='';state.collectionSource='browse';state.pageInfo={...state.browsePageInfo};state.verifiedAt=state.browseVerifiedAt;
  }
  async function searchCatalogue(query,options={},action={},newDiscovery=true){
    if(typeof query!=='string'||query.length>250)return {ok:false,action:'search',message:'Please use a shorter jewelry search.'};
    if(action.sort&&!SORTS.has(action.sort)||action.filter&&!FILTERS.has(action.filter))return {ok:false,action:'search',message:'Those collection controls are not available.'};
    const initialFocus=focusedElement(),leavingPage=state.pageKind!=='catalogue',record=begin(options),nextSearch=clean(query,250);state.search=nextSearch;state.checkedSearch=null;state.collectionSource=nextSearch?'search':'browse';if(action.sort)state.sort=action.sort;state.filter=action.filter||'all';state.limit=PAGE_SIZE;restoreCollection();if(leavingPage)commitPage('catalogue','',options.push!==false,{preserveLoading:true});else publish();status(state.search?'Finding “'+state.search+'” in the live shop…':'Opening the wider live collection…');
    try{
      const data=nextSearch?await get('/api/growth/catalogue?q='+encodeURIComponent(nextSearch),record.controller.signal):await catalogueStart(record.controller.signal);
      if(!current(record))return {ok:false,action:'search',message:'The earlier search was cancelled.'};
      const pageInfo=checkedPageInfo(data);state.products=checkedProducts(data);state.pageInfo=pageInfo;state.checkedSearch=nextSearch;state.verifiedAt=Date.now();
      if(!nextSearch){state.browse=state.products.slice();state.browsePageInfo={...pageInfo};state.browseVerifiedAt=state.verifiedAt;applySeed(data);}if(newDiscovery)reviseDiscovery();state.loading=false;drawGrid();publish();if(mayRevealCollection(initialFocus))focusSection('catalogue',false);status(filtered().length?'Your checked pieces are ready.':'No checked matches yet. Try another symbol or style.');
      return {ok:true,action:'search',live:data.live!==false,checkedAt:state.verifiedAt,products:filtered().slice(0,state.limit).map(projection),snapshot:snapshot(),message:filtered().length?'Your checked search results are ready on the page.':'I couldn’t find a name or symbol match in these checked results. Try another symbol or show all pieces.'};
    }catch{if(current(record)){state.loading=false;drawGrid();publish();status('The live selection is temporarily unavailable. Your existing view is preserved.');}return {ok:false,action:'search',message:'The live selection could not be checked. Please try again.'};}
  }
  async function loadMore(){
    const available=filtered();if(available.length>state.limit){state.limit=Math.min(MAX_PIECES,state.limit+PAGE_SIZE);drawGrid();publish();return;}
    const startBrowse=!!state.seedInfo&&!state.seedBrowseStarted;if((!startBrowse&&(!state.pageInfo.hasNextPage||!state.pageInfo.endCursor))||state.search||state.browse.length>=MAX_PIECES)return;
    const cursor=startBrowse?null:state.pageInfo.endCursor,record=begin();status('Opening the next collection page…');
    try{const data=await get('/api/growth/catalogue?browse=1'+(cursor?'&cursor='+encodeURIComponent(cursor):''),record.controller.signal);if(!current(record))return;const pageInfo=checkedPageInfo(data,cursor),additional=checkedProducts(data),ids=new Set(state.browse.map(p=>p.id)),handles=new Set(state.browse.map(p=>p.handle)),fresh=additional.filter(p=>!ids.has(p.id)&&!handles.has(p.handle));state.seedBrowseStarted=true;state.browse=state.browse.concat(fresh).slice(0,MAX_PIECES);state.products=state.browse.slice();state.collectionSource='browse';state.checkedSearch='';state.limit=Math.min(MAX_PIECES,state.limit+PAGE_SIZE);state.pageInfo=pageInfo;state.browsePageInfo={...pageInfo};state.loading=false;state.verifiedAt=Date.now();state.browseVerifiedAt=state.verifiedAt;drawGrid();publish();status(fresh.length?'More live pieces are ready.':pageInfo.hasNextPage?'This page has no additional matching pieces. Continue to the next public collection page.':'You have reached the end of the checked public collection.');}catch{if(current(record)){state.loading=false;publish();status('That collection page could not be checked. Try Explore more pieces again.');}}
  }
  async function openProduct(handle,{push=true,signal,section,focusFrom=null,revealFrom=null,revealRequested=false}={}){
    if(!validHandle(handle))return false;
    const initialFocus=focusedElement(),focusRoot=initialFocus?.getRootNode();let focusIntent=!!focusFrom&&focusFrom.isConnected&&document.activeElement===focusFrom,revealIntent=revealRequested===true||!!revealFrom&&revealFrom.isConnected&&main.contains(revealFrom);
    const movedFocus=event=>{if(event.target!==focusFrom)focusIntent=false;if(!sameRevealFocus(initialFocus))revealIntent=false;};if(focusIntent||revealIntent){document.addEventListener('focusin',movedFocus);if(focusRoot!==document){focusRoot?.addEventListener('focusin',movedFocus);focusRoot?.addEventListener('focusout',movedFocus);}}
    const record=begin({signal});status('Opening the checked product details…');
    try{
      const data=await get('/api/growth/product?handle='+encodeURIComponent(handle),record.controller.signal),p=data.product;
      if(!current(record)||data.live===false||!validProduct(p,handle)){if(current(record)){state.loading=false;publish();}return false;}
      const focusHeading=focusIntent&&!document.hidden&&document.activeElement===focusFrom,revealProduct=revealIntent&&!section&&!document.hidden&&(revealRequested===true||revealFrom?.isConnected)&&sameRevealFocus(initialFocus);remember(p);state.current=p;state.selectedImage=0;state.verifiedAt=Date.now();state.activeSection=section||'details';renderProduct(p);commitPage('product',handle,push);if(section&&!document.hidden&&revealIntent&&sameRevealFocus(initialFocus))focusSection(section,true,{immediate:true});
      if(focusHeading){const heading=main.querySelector('.product-copy h1');if(heading){heading.tabIndex=-1;heading.focus({preventScroll:true});try{heading.scrollIntoView?.({behavior:reduced()?'auto':'instant',block:'start'});}catch{}}}
      // A pointer or explicitly requested piece starts at its checked image and title. Reveal
      // the layout without giving it keyboard focus or overriding a section.
      // Complete a requested arrival before returning success. Smooth scrolling
      // can still be mid-transition when the guide reports that details are open.
      if(revealProduct&&revealIntent&&!document.hidden&&current(record)&&state.pageKind==='product'&&state.currentHandle===handle&&state.current?.id===p.id&&sameRevealFocus(initialFocus)){const layout=main.querySelector('.product-layout');try{layout?.scrollIntoView?.({behavior:reduced()?'auto':'instant',block:'start'});}catch{}}
      status('Live options checked. Your guide stays with you.');return true;
    }catch{if(current(record)){state.loading=false;publish();status('This piece could not be checked. Your current page is preserved.');}return false;}
    finally{document.removeEventListener('focusin',movedFocus);if(focusRoot!==document){focusRoot?.removeEventListener('focusin',movedFocus);focusRoot?.removeEventListener('focusout',movedFocus);}}
  }
  function productImages(p){
    const images=(Array.isArray(p.images)?p.images:[]).slice(0,15).flatMap(i=>{const image=typeof i==='string'?i:i?.url,src=safeImage(image);return src?[{image:src,imageAlt:clean(typeof i==='object'?i.altText||p.title:p.title,300)}]:[];});
    const first=safeImage(p.image);if(first&&!images.some(i=>i.image===first))images.unshift({image:first,imageAlt:clean(p.imageAlt||p.title,300)});return images;
  }
  async function returnToCollection(event){
    const origin=event.currentTarget,keyboard=event.detail===0&&origin?.isConnected&&document.activeElement===origin&&!document.hidden;
    let focusIntent=keyboard;const movedFocus=next=>{if(next.target!==origin)focusIntent=false;};if(keyboard)document.addEventListener('focusin',movedFocus);
    try{
      const pending=execute({type:'search',query:''}),version=navigationVersion,result=await pending;
      // Back explicitly opens All pieces. Its completed collection heading
      // is the new focus target; no previous card identity is guessed.
      if(!focusIntent||document.hidden||!result.ok||version!==navigationVersion||state.loading||state.pageKind!=='catalogue'||state.search!==''||state.filter!=='all'||result.snapshot?.discoveryRevision!==state.discoveryRevision||document.activeElement!==document.body)return;
      const heading=main.querySelector('.collection-heading h2');if(heading){heading.tabIndex=-1;heading.focus({preventScroll:true});try{heading.scrollIntoView?.({behavior:reduced()?'auto':'smooth',block:'start'});}catch{}}
    }finally{if(keyboard)document.removeEventListener('focusin',movedFocus);}
  }
  function exactOptionGroups(p){
    const groups=new Map(),append=(name,value)=>{name=clean(name,100);value=clean(value,300);if(!name||!value)return;const key=name.toLowerCase();if(!groups.has(key)&&groups.size<12)groups.set(key,{name,values:[]});const group=groups.get(key);if(group&&group.values.length<250&&!group.values.includes(value))group.values.push(value);};
    (Array.isArray(p.options)?p.options:[]).slice(0,12).forEach(o=>(Array.isArray(o?.values)?o.values:[]).forEach(value=>append(o.name,value)));
    p.variants.forEach(v=>(Array.isArray(v.options)?v.options:[]).slice(0,12).forEach(o=>append(o.name,o.value)));return [...groups.values()];
  }
  function renderProduct(p){
    main.replaceChildren();main.append(button('← Back to the collection','back-link',returnToCollection));
    const layout=node('div',null,'product-layout view-enter'),gallery=node('section',null,'product-gallery'),images=productImages(p);gallery.dataset.storeSection='image';
    const enlarge=button('','product-image-button',()=>zoomImage(p));enlarge.setAttribute('aria-label','Enlarge image of '+p.title);const photo=images[0]?picture(images[0]):null;enlarge.append(photo||node('span','b.','piece-placeholder'));if(images.length)enlarge.append(node('span','Enlarge ↗','image-zoom-label'));else enlarge.disabled=true;gallery.append(enlarge);
    if(images.length>1){const thumbs=node('div',null,'image-thumbs');images.forEach((value,index)=>{const b=button('','',()=>{state.selectedImage=index;const img=picture(value);if(img){img.loading='eager';enlarge.replaceChildren(img,node('span','Enlarge ↗','image-zoom-label'));}thumbs.querySelectorAll('button').forEach((v,j)=>v.setAttribute('aria-pressed',String(index===j)));});b.setAttribute('aria-label','View product image '+(index+1));b.setAttribute('aria-pressed',String(index===0));const img=picture(value);if(img)b.append(img);thumbs.append(b);});gallery.append(thumbs);}
    const copy=node('section',null,'product-copy');copy.dataset.productHandle=p.handle;copy.dataset.productId=p.id;copy.append(node('span',p.type||'A PERSONAL BRITES PIECE','eyebrow'),node('h1',p.title));
    const price=node('p',minimum(p)==null?'Currently unavailable':'From '+money(minimum(p),p.currency),'product-price');price.dataset.storeSection='price';copy.append(price,node('p',p.variants.some(v=>v.available)?'Live options and availability checked for this view':'This piece is currently unavailable','availability'),node('p',p.description.slice(0,12000),'product-description'));
    const options=node('section',null,'option-area');options.dataset.storeSection='options';const label=node('label','Choose your exact piece');label.htmlFor='piece-variant';const select=document.createElement('select');select.id='piece-variant';select.setAttribute('aria-label','Choose an exact product option');select.append(node('option','Select an option…'));select.firstChild.value='';
    p.variants.forEach(v=>{const o=node('option',v.title+' · '+money(v.price,p.currency)+(v.available?'':' · unavailable'));o.value=v.id;o.disabled=!v.available;select.append(o);});
    const selected=node('dl',null,'exact-options'),add=button('Choose an option to add','primary');add.disabled=true;let adding=false,review=null;const choices=new Map(),groups=exactOptionGroups(p),menu=node('div',null,'option-menu');menu.hidden=true;
    const menuTrigger=button('Explore the published option menus','secondary option-menu-trigger',()=>openOptions());menuTrigger.setAttribute('aria-expanded','false');const groupViews=new Map();
    groups.forEach(group=>{const row=node('section',null,'option-group');row.dataset.optionName=group.name;row.append(node('h3',group.name,'option-title'));group.values.forEach(value=>{const choice=button(value,'option-choice',()=>{chooseOption(group.name,value);publish();});choice.dataset.optionValue=value;choice.setAttribute('aria-pressed','false');row.append(choice);});menu.append(row);groupViews.set(group.name,row);});
    const quantityLabel=node('label','Quantity','product-quantity'),quantityInput=document.createElement('input');quantityInput.type='number';quantityInput.min='1';quantityInput.max='20';quantityInput.step='1';quantityInput.value='1';quantityInput.setAttribute('aria-label','Quantity of this exact piece');quantityLabel.append(quantityInput);
    const quantity=()=>Number(quantityInput.value),clearReview=()=>{review?.remove();review=null;if(productUI?.product===p)productUI.review=null;};
    function updateSelection(syncChoices=true){const v=p.variants.find(v=>v.id===select.value);if(syncChoices){choices.clear();(v?.options||[]).forEach(o=>{const group=groups.find(g=>g.name.toLowerCase()===String(o.name).toLowerCase());if(group?.values.includes(o.value))choices.set(group.name,o.value);});}selected.replaceChildren();const shown=v?.options||[...choices].map(([name,value])=>({name,value}));shown.slice(0,12).forEach(o=>{const line=node('div');line.append(node('dt',clean(o.name,100)+':'),node('dd',clean(o.value,200)));selected.append(line);});groupViews.forEach((row,name)=>row.querySelectorAll('.option-choice').forEach(b=>{b.setAttribute('aria-pressed',String(choices.get(name)===b.dataset.optionValue));b.disabled=adding;}));price.textContent=v?money(v.price,p.currency):minimum(p)==null?'Currently unavailable':'From '+money(minimum(p),p.currency);select.disabled=quantityInput.disabled=adding;add.disabled=adding||!validQuantity(quantity())||!v||!v.available||!p.variantsComplete||needsCustomizer(p,v);add.textContent=adding?'Checking this exact option…':v&&needsCustomizer(p,v)?'Customize with the shop':v?'Add this exact option to test bag':'Choose an option to add';}
    function openOptions(name,reveal=true){if(name!==undefined){const matches=groups.filter(g=>g.name.toLowerCase()===String(name).toLowerCase());if(matches.length!==1)return false;name=matches[0].name;}menu.hidden=false;menuTrigger.setAttribute('aria-expanded','true');groupViews.forEach((row,key)=>{row.hidden=!!name&&key!==name;});if(productUI?.product===p)productUI.openedOption=name||null;if(reveal&&!document.hidden)focusSection('options');return true;}
    function chooseOption(name,value){if(adding)return false;const matches=groups.filter(g=>g.name.toLowerCase()===String(name).toLowerCase());if(matches.length!==1)return false;const group=matches[0],values=group.values.filter(v=>v.toLowerCase()===String(value).toLowerCase());if(values.length!==1)return false;clearReview();choices.set(group.name,values[0]);select.value='';if(groups.every(g=>choices.has(g.name))){const candidates=p.variants.filter(v=>groups.every(g=>(v.options||[]).some(o=>String(o.name).toLowerCase()===g.name.toLowerCase()&&o.value===choices.get(g.name))));if(candidates.length===1&&candidates[0].available)select.value=candidates[0].id;}updateSelection(false);status(select.value?'Your exact published option is selected.':'Your published choice is shown. Choose the remaining options to resolve an exact available piece.');return true;}
    function chooseVariant(id){if(adding||typeof id!=='string'||!VARIANT.test(id)||p.variants.filter(v=>v.id===id&&v.available).length!==1)return false;clearReview();select.value=id;updateSelection();return true;}
    function prepareReview(){const v=p.variants.find(v=>v.id===select.value),q=quantity();if(adding||!v?.available||!validQuantity(q)||!p.variantsComplete||needsCustomizer(p,v))return false;clearReview();review=node('section',null,'product-review review');review.append(node('h3','Review your exact test-bag choice'),node('p',p.title),node('p',v.title+' · quantity '+q),node('p',money(v.price*q,p.currency)+' item subtotal'),node('p','We will check this exact option again after you confirm. No real order is placed.'));const confirm=button('Confirm add to test bag','primary',()=>{if(!review?.isConnected||select.value!==v.id||quantity()!==q||!currentProductUI())return;add.click();}),cancel=button('Cancel this review','secondary',()=>{clearReview();publish();});review.append(confirm,cancel);options.append(review);if(productUI?.product===p)productUI.review=review;if(!document.hidden){try{review.scrollIntoView?.({behavior:reduced()?'auto':'smooth',block:'center'});}catch{}}return true;}
    select.addEventListener('change',()=>{clearReview();updateSelection();publish();});quantityInput.addEventListener('change',()=>{clearReview();updateSelection(false);publish();});
    add.addEventListener('click',async()=>{
      const v=p.variants.find(v=>v.id===select.value),version=navigationVersion,q=quantity();
      if(adding||!select.isConnected||state.currentHandle!==p.handle||!v?.available||!validQuantity(q)||needsCustomizer(p,v)||!p.variantsComplete)return;
      const admitted=()=>select.isConnected&&state.pageKind==='product'&&state.currentHandle===p.handle&&state.current?.id===p.id&&select.value===v.id&&quantity()===q&&navigationVersion===version;
      adding=true;updateSelection();
      try{
        const data=await get('/api/growth/product?handle='+encodeURIComponent(p.handle)),live=data.product,exact=live?.variants?.find(x=>x.id===v.id);
        if(!admitted())return;
        if(data.live===false||!validProduct(live,p.handle)||live.id!==p.id||!live.variantsComplete||needsCustomizer(live,exact)||!exact?.available||live.currency!==p.currency||Math.abs(exact.price-v.price)>.005)throw Error('The option, price or availability changed. Check the details again.');
        const cart=readCart(),lineIds=cartIdentities(cart);cart.push({productId:p.id,title:p.title,variantId:v.id.split('/').pop(),variant:v.title,price:v.price,currency:p.currency,...(q!==1?{quantity:q}:{})});lineIds.push(newLineId());writeCart(cart,lineIds);clearReview();status('Added to your test bag. No real shop order is created.');publish();
      }catch(e){if(admitted())status(e.message||'This option could not be checked. Please try again.');}
      finally{adding=false;if(select.isConnected)updateSelection();}
    });
    productUI={product:p,panel:options,select,menu,groups,quantity,quantityInput,menuTrigger,choices:()=>[...choices].map(([name,value])=>({name,value})),openedOption:null,review:null,openOptions,chooseOption,chooseVariant,prepareReview,updateSelection,invalidateReview:clearReview,setQuantity:value=>{if(adding||!validQuantity(value))return false;clearReview();quantityInput.value=String(value);updateSelection(false);return true;}};
    options.append(label,select,menuTrigger,menu,selected,quantityLabel,add,button('Review before adding','secondary',()=>{if(prepareReview())publish();else status('Choose every available exact option first. Held or customized pieces need the shop.');}),node('p',p.variantsComplete?'Exact options above come from the product listing. Engraving or other custom inputs need the shop’s own customizer.':'This preview does not contain every variant. Confirm complete options with the shop before ordering.','option-help'));copy.append(options);
    const links=node('div',null,'product-links');links.append(link('Open the real product page ↗',p.url),button('Make it a gift','text-link',()=>void execute({type:'gift'})));copy.append(links);layout.append(gallery,copy);main.append(layout);
    const details=node('div',null,'product-details'),spec=node('section',null,'detail-panel');spec.dataset.storeSection='details';spec.append(node('h2','The details that matter'),node('p','Materials, dimensions and making details below are the exact listing information. If a detail is not published, your guide will help you ask the shop.'));
    const lines=node('ul');(p.options||[]).slice(0,10).forEach(o=>lines.append(node('li',clean(o.name,100)+': '+(o.values||[]).map(v=>clean(v,200)).slice(0,30).join(' · '))));if(lines.childElementCount)spec.append(lines);spec.append(node('p',p.description.slice(0,12000)));
    const story=node('section',null,'detail-panel story-block');story.dataset.storeSection='story';story.append(node('h2','A story you can make yours'),node('p','Explore reviewed history or symbolism for this exact piece. Meanings are personal interpretations, and can vary across people and cultures.'));const storyButton=button('Explore its reviewed story','secondary',()=>void showStory(p,story,storyButton));story.append(storyButton);details.append(spec,story);main.append(details);if(full){renderServicePanel(main);void loadServices();}
  }
  function neutralSource(source){
    // The knowledge endpoint already binds these citations to approved exact
    // product research. Preserve neutral base-dossier journals and museums as
    // well as supplement sources; enforce public-link safety at display time.
    try{const u=new URL(source?.url),host=u.hostname.replace(/^www\./,'').toLowerCase();
      if(u.protocol!=='https:'||u.username||u.password||u.port||!host.includes('.')||/^(?:localhost|127\.|0\.|10\.|192\.168\.|169\.254\.|172\.(?:1[6-9]|2\d|3[01])\.)/.test(host)||/(?:^|[./_-])(?:internal|private|admin|staging|author|prompt)(?:[./_-]|$)|(?:^|\/)(?:products?|shop|cart|checkout)(?:\/|$)/i.test(host+u.pathname))return null;
      if(/(?:^|\.)(?:etsy|amazon|ebay|aliexpress|walmart|shopify)\.[a-z.]+$/i.test(host))return null;
      const title=clean(source.title,300),safeTitle=storyText(source.title,300),at=source.checkedAt,now=Date.now();
      if(!title||title!==safeTitle||!Number.isFinite(at)||at>now+60000||now-at>30*86400000)return null;
      return {title,url:u.href,checkedAt:at};
    }catch{return null;}
  }
  function storyText(value,max){return clean(value,max).split(/(?<=[.!?])\s+/).filter(s=>!/<[^>]*>|https?:\/\/|(?:system|developer|assistant|author|internal|hidden)\s+(?:prompt|message|instructions?)|(?:tell|ask|instruct|advise|remind)\s+(?:the\s+)?(?:shopper|user|customer)|ignore .*instructions/i.test(s)).join(' ').trim();}
  async function showStory(p,box,b){
    if(p.meaningHold||p.recommendationHold){status('Reviewed meaning for this piece is being checked.');return;}
    const version=++storyVersion;b.disabled=true;
    try{const data=await get('/api/growth/knowledge?ids='+encodeURIComponent(p.id));if(version!==storyVersion||state.currentHandle!==p.handle)return;
      const meanings=(Array.isArray(data.products)?data.products:[]).filter(m=>m.productId===p.id&&m.kind==='interpretation'&&storyText(m.text,1500)&&storyText(m.context,300)&&Array.isArray(m.sources)&&m.sources.length>0&&m.sources.every(s=>neutralSource(s))).slice(0,2);
      box.querySelectorAll('.reviewed-story').forEach(e=>e.remove());if(!meanings.length){b.textContent='No reviewed story is available yet';status('There is no approved cited story for this exact piece yet. The symbol can still have your own personal meaning.');return;}
      meanings.forEach(m=>{const story=node('div',null,'reviewed-story');story.append(node('p',storyText(m.text,1500)),node('p',storyText(m.context,300)+' · a personal interpretation; meanings can vary.','story-qualification'));m.sources.slice(0,4).forEach(s=>{const src=neutralSource(s);story.append(link(src.title,src.url));});box.append(story);});b.hidden=true;focusSection('story');
    }catch{status('The reviewed story could not be checked. Please try again.');}finally{b.disabled=false;}
  }
  function zoomImage(p){
    const image=productImages(p)[state.selectedImage];if(!image)return false;
    document.querySelector('#storefront-image-dialog')?.remove();const dialog=node('dialog',null,'storefront-dialog');dialog.id='storefront-image-dialog';dialog.setAttribute('aria-label','Enlarged image of '+p.title);const content=node('div',null,'zoom-content'),close=button('×','dialog-close',()=>{if(typeof dialog.close==='function')dialog.close();else dialog.remove();});close.setAttribute('aria-label','Close enlarged image');const img=picture(image);img.loading='eager';content.append(close,img,node('p',p.title+' · image from the live product listing'));dialog.append(content);document.body.append(dialog);dialog.addEventListener('close',()=>dialog.remove(),{once:true});dialog.addEventListener('click',e=>{if(e.target===dialog){if(typeof dialog.close==='function')dialog.close();else dialog.remove();}});if(typeof dialog.showModal==='function')dialog.showModal();else dialog.setAttribute('open','');close.focus();state.activeSection='image';publish();return true;
  }
  async function loadServices(){
    if(state.services)return state.services;if(state.servicesPending)return state.servicesPending;
    state.servicesPending=get('/api/growth/storefront-services').then(data=>{state.services=data.services||data;main.querySelectorAll('.service-panel').forEach(updateServices);return state.services;}).catch(()=>{main.querySelectorAll('.published-offers').forEach(list=>{list.replaceChildren(node('p','Current published offers could not be checked. Ask the shop before applying a code.'));});return null;}).finally(()=>{state.servicesPending=null;});return state.servicesPending;
  }
  function renderServiceStrip(){
    main.querySelector('.service-strip')?.remove();const strip=node('section',null,'service-strip');
    [['✧','Made with care','See the merchant’s sourcing and 2–3 day production guidance.','shipping'],['♡','A gift, beautifully personal','Explore gift packages, notes and wrapping with the shop.','gifts'],['⌁','Your idea, made yours','From engraving to a new design, talk through the possibilities.','customize']].forEach(([icon,title,copy,section])=>{const a=node('article');a.append(node('div',icon,'service-icon'),node('h3',title),node('p',copy),button('Explore '+(section==='shipping'?'shop services':section==='gifts'?'gifting':'custom options')+' ↗','text-link',()=>void execute({type:section==='gifts'?'gift':section==='customize'?'customize':'scroll',section})));strip.append(a);});main.append(strip);
  }
  function renderServicePanel(parent){
    parent.querySelector('.service-panel')?.remove();const panel=node('section',null,'service-panel');panel.dataset.storeSection='shipping';panel.append(node('span','THE BRITES MAKING & GIFTING EXPERIENCE','eyebrow'),node('h2','A little extra thought.'));
    const production=node('p','The Brites studio says production normally takes 2–3 days. Production time is separate from shipping time; a custom design or requested deadline needs studio confirmation.');production.dataset.guidancePart='production';
    const sourcing=node('p','The studio says its materials are ethically sourced from the United States. Exact materials and finishes come from the selected listing. This is merchant-provided guidance, not an independent sourcing certification.');sourcing.dataset.guidancePart='sourcing';
    const shipping=node('p','The studio offers a variety of shipping options. Available services, rates and transit estimates depend on your destination and are confirmed by the shop.');shipping.dataset.guidancePart='shipping';panel.append(production,sourcing,shipping,node('div',null,'service-conflicts'));
    const forms=node('div',null,'service-forms'),gift=node('form',null,'service-form');gift.dataset.storeSection='gifts';const giftSummary=node('p','The studio offers gift packages, gift notes and gift wrapping. Confirm package availability and any charge with the shop.');giftSummary.dataset.guidancePart='gifts';gift.append(node('h3','Make it a gift'),giftSummary,node('p','This test view saves your preferences only.'));
    const wrapLabel=node('label'),wrap=document.createElement('input');wrap.type='checkbox';wrap.name='wrapping';wrapLabel.append(wrap,document.createTextNode('Ask for gift wrapping'));
    const packageLabel=node('label'),pack=document.createElement('input');pack.type='checkbox';pack.name='package';packageLabel.append(pack,document.createTextNode('Ask about a gift package'));
    const noteLabel=node('label','Your test gift note'),note=document.createElement('textarea');note.name='note';note.maxLength=350;note.setAttribute('aria-label','Test gift note');note.placeholder='Something they will love to read…';noteLabel.append(note);const saveGift=button('Save test gift preferences','secondary');saveGift.type='submit';gift.append(wrapLabel,packageLabel,noteLabel,saveGift);
    gift.addEventListener('submit',e=>{e.preventDefault();savePreferences({wrapping:wrap.checked,giftPackage:pack.checked,giftNote:note.value.slice(0,350)});status('Gift preferences saved for this test session. Nothing was sent to the shop.');});
    const custom=node('form',null,'service-form');custom.dataset.storeSection='customize';const customSummary=node('p','Explore engraving, adjusted designs or a brand new piece based on your idea. The studio confirms feasibility, materials, pricing and production time before a custom order.');customSummary.dataset.guidancePart='customization';custom.append(node('h3','Make it yours'),customSummary);
    const ideaLabel=node('label','Your test design idea'),idea=document.createElement('textarea');idea.name='idea';idea.maxLength=600;idea.setAttribute('aria-label','Test custom design idea');idea.placeholder='A symbol, a date, a sketch you would like to discuss…';ideaLabel.append(idea);const saveIdea=button('Save test design brief','secondary');saveIdea.type='submit';custom.append(ideaLabel,saveIdea);custom.addEventListener('submit',e=>{e.preventDefault();savePreferences({customIdea:idea.value.slice(0,600)});status('Design brief saved for this test session. It has not been sent to the shop.');});
    const preferences=readPreferences();wrap.checked=preferences.wrapping===true;pack.checked=preferences.giftPackage===true;note.value=preferences.giftNote||'';idea.value=preferences.customIdea||'';forms.append(gift,custom);panel.append(forms,node('p','These notes stay in this browser session. No contact, address, payment or upload information is requested.','local-only-note'));
    const offers=node('div');offers.dataset.storeSection='offers';offers.append(node('h3','Published offers'),node('p','Only an offer currently published by Brites will be shown here. An offer’s eligibility and final discount are confirmed in the real shop.'));
    const list=node('div',null,'published-offers');offers.append(list);panel.append(offers);parent.append(panel);updateServices(panel);return panel;
  }
  function ownSource(source){try{const u=new URL(source?.url);return u.protocol==='https:'&&!u.username&&!u.password&&!u.port&&['britesjewelry.com','www.britesjewelry.com'].includes(u.hostname)?{title:clean(source.title,300)||'Brites published source',url:u.href}:null;}catch{return null;}}
  function updateServices(panel){
    const services=state.services;
    if(services?.guidance){panel.querySelectorAll('[data-guidance-part]').forEach(p=>{const part=services.guidance[p.dataset.guidancePart];if(typeof part?.summary==='string')p.textContent=clean(part.summary,1500)+(p.dataset.guidancePart==='sourcing'?' Merchant-provided guidance; not an independent sourcing certification.':'');});}
    const conflicts=panel.querySelector('.service-conflicts');if(conflicts){conflicts.replaceChildren();(Array.isArray(services?.conflicts)?services.conflicts:[]).slice(0,3).forEach(conflict=>{const src=ownSource(conflict.source);if(!src||typeof conflict.publishedSummary!=='string')return;conflicts.append(node('h3','Confirm current '+clean(conflict.topic,50)+' guidance'),node('p',clean(conflict.merchantSummary,1000)),node('p',clean(conflict.publishedSummary,1500)),link(src.title+' ↗',src.url));});}
    const offers=panel.querySelector('.published-offers');if(offers)renderOffers(offers);
  }
  function renderOffers(list){
    list.replaceChildren();const offers=state.services?.offers?.items||state.services?.offers||state.services?.publishedOffers||[];
    if(!Array.isArray(offers)||!offers.length){list.append(node('p',!state.services?'Checking current published offers…':state.services.offers?.status==='unavailable'?'Current published offers could not be checked.':'No published offer was observed in the checked sources.'));return;}
    if(state.services.offers?.partial===true)list.append(node('p','Some published sources could not be checked. Confirm the current offer and terms with the shop.'));
    offers.slice(0,5).forEach(o=>{const title=clean(o?.summary||o?.title||o?.text||o?.description,1200),src=ownSource(o?.source);if(!title||!src||o.checkoutValidated===true)return;list.append(node('p',title),node('p','Published offer · eligibility, expiry and any combination with other discounts must be confirmed at checkout.'));list.append(link(src.title+' ↗',src.url));});
  }
  function readPreferences(){try{const p=JSON.parse(sessionStorage.getItem('brites-sandbox-gift-preferences')||'{}');return {wrapping:p.wrapping===true,giftPackage:p.giftPackage===true,giftNote:clean(p.giftNote,350),customIdea:clean(p.customIdea,600)};}catch{return {};}}
  function savePreferences(p){sessionStorage.setItem('brites-sandbox-gift-preferences',JSON.stringify({...readPreferences(),...p}));refreshCheckoutPreferences();}
  function refreshCheckoutPreferences(){const ui=checkoutUI;if(!ui?.page?.isConnected)return;const prefs=readPreferences();ui.page.querySelectorAll('[data-checkout-gift]').forEach(line=>{line.textContent=line.dataset.checkoutGift==='wrapping'?prefs.wrapping?'Gift wrapping enquiry saved.':'No gift wrapping enquiry saved.':line.dataset.checkoutGift==='package'?prefs.giftPackage?'Gift package enquiry saved.':'No gift package enquiry saved.':prefs.giftNote?'Test gift note: '+prefs.giftNote:'No test gift note saved.';});}
  async function showService(section){
    if(!main.querySelector('.service-panel'))renderServicePanel(main);
    focusSection(section);void loadServices();
    return {ok:true,action:section==='gifts'?'gift':section==='customize'?'customize':'scroll',message:'The '+section+' section is open. Preferences stay in this test session.',snapshot:snapshot()};
  }
  function renderBag({push=true}={}){
    cancelPending();state.current=null;main.replaceChildren();const page=node('section',null,'bag-page view-enter');page.dataset.storeSection='bag';page.append(node('span','YOUR SESSION-ONLY SELECTION','eyebrow'),node('h1','Your sandbox bag'),node('p','A place to try your choices together. This test bag never places a shop order.','bag-intro'));const cart=readCart(),items=node('div',null,'bag-items');
    if(!cart.length)items.append(node('p','Your sandbox bag is empty.','bag-intro'));
    const lineIds=cartIdentities(cart);cart.forEach((p,index)=>{const row=node('article',null,'bag-item'),copy=node('div');row.dataset.bagLine=lineIds[index];copy.append(node('h3',p.title),node('p',p.variant),button('Remove this test piece','text-link',()=>void execute({type:'bag-remove',lineId:lineIds[index]})));const label=node('label','Quantity','bag-quantity'),quantity=document.createElement('input');quantity.type='number';quantity.min='1';quantity.max='20';quantity.step='1';quantity.value=String(p.quantity||1);quantity.setAttribute('aria-label','Quantity of '+p.title+' · '+p.variant);quantity.addEventListener('change',async()=>{const result=await execute({type:'bag-quantity',lineId:lineIds[index],quantity:Number(quantity.value)});if(!result.ok&&quantity.isConnected){quantity.value=String(p.quantity||1);status(result.message);}});label.append(quantity);copy.append(label);const handle=identities.get(p.productId);if(handle)copy.append(button('View its live details','text-link',()=>void openProduct(handle)));row.append(copy,node('p',money(p.price*(p.quantity||1),p.currency)));items.append(row);});
    page.append(items);const totals=new Map();cart.forEach(p=>totals.set(p.currency,(totals.get(p.currency)||0)+p.price*(p.quantity||1)));totals.forEach((amount,currency)=>{const total=node('div',null,'bag-total');total.append(node('span','Test item subtotal'),node('strong',money(amount,currency)));page.append(total);});if(cart.length)page.append(node('p','Items only. Shipping, taxes, gift services and any discounts are not calculated in this preview.','bag-subtext'));
    const actions=node('div',null,'bag-actions');actions.append(button('Continue exploring','secondary',()=>void execute({type:'search',query:''})));if(cart.length)actions.append(button('Try test checkout →','primary',()=>void execute({type:'checkout'})));page.append(actions);main.append(page);if(full)renderServicePanel(page);commitPage('bag','',push);updateBag();
  }
  async function verifyBag(cart,signal){
    const requests=new Map();for(const item of cart){const handle=identities.get(item.productId);if(!handle)throw Error('One saved piece needs its product page checked before test checkout.');if(!requests.has(handle))requests.set(handle,get('/api/growth/product?handle='+encodeURIComponent(handle),signal));}
    const results=await Promise.all([...requests].map(async([handle,pending])=>({handle,data:await pending}))),products=new Map();for(const {handle,data:d}of results){if(d.live===false||!validProduct(d.product,handle))throw Error('A bag piece could not be checked.');products.set(d.product.id,d.product);}
    for(const item of cart){const p=products.get(item.productId),v=p?.variants.find(v=>v.id==='gid://shopify/ProductVariant/'+item.variantId);if(!p||!p.variantsComplete||!v?.available||needsCustomizer(p,v)||p.currency!==item.currency||Math.abs(v.price-item.price)>.005)throw Error('A saved option, price or availability changed. Reopen that piece before test checkout.');}return products;
  }
  async function editBag(action,options){
    const type=action.type,cart=readCart(),ids=cartIdentities(cart),index=ids.indexOf(action.lineId),visible=main.querySelector('[data-bag-line="'+(typeof action.lineId==='string'&&/^[a-zA-Z0-9][a-zA-Z0-9:_-]{0,199}$/.test(action.lineId)?action.lineId:'')+'"]');
    if(!['bag','checkout'].includes(state.pageKind)||index<0||!visible||type==='bag-quantity'&&!validQuantity(action.quantity))return controlResult(type,false,'Choose one exact current test-bag line and a quantity from 1 to 20.');
    const fingerprint=JSON.stringify(cart),version=navigationVersion;
    if(type==='bag-quantity'){try{await verifyBag([cart[index]],options.signal);}catch(e){return controlResult(type,false,e.message||'That saved option could not be checked.');}}
    if(options.signal?.aborted||version!==navigationVersion||JSON.stringify(readCart())!==fingerprint||!visible.isConnected||cartIdentities(readCart())[index]!==action.lineId)return controlResult(type,false,'Your test bag changed while the choice was checked. Please use its current line.');
    const next=cart.slice(),nextIds=ids.slice();if(type==='bag-remove'){next.splice(index,1);nextIds.splice(index,1);}else next[index]={...next[index],quantity:action.quantity};writeCart(next,nextIds);if(state.pageKind==='checkout')renderBag({push:false});status(type==='bag-remove'?'That exact line was removed from your test bag.':'The exact test-bag line quantity is updated. This does not reserve shop inventory.');publish();return controlResult(type,true,notice.textContent,{cartChanged:true});
  }
  function controlResult(type,ok,message,extra={}){return {ok,action:type,message,...extra,snapshot:snapshot()};}
  async function runProductControl(action,options){
    const type=action.type,crossing=type==='options'&&action.handle&&action.handle!==state.currentHandle,initialFocus=focusedElement(),guard=crossing?trackRevealFocus(initialFocus):null;
    try{if(crossing&&!await openProduct(action.handle,{signal:options.signal}))return controlResult(type,false,'That exact piece could not be checked.');const ui=currentProductUI();if(options.signal?.aborted||!ui||action.handle&&action.handle!==ui.product.handle)return controlResult(type,false,'Open the exact current piece before changing its published options.');
      let ok=false;if(type==='options'){if(action.optionName!==undefined&&typeof action.optionName!=='string')return controlResult(type,false,'Name one literal published option group.');ok=ui.openOptions(action.optionName,!guard||guard.allowed()&&(focusedElement()===initialFocus||!initialFocus?.isConnected&&focusedElement()===document.body));}else if(type==='select-option'){
        const byId=typeof action.variantId==='string',byValue=typeof action.optionName==='string'&&typeof action.optionValue==='string';if(byId===byValue||byId&&(action.optionName!==undefined||action.optionValue!==undefined))return controlResult(type,false,'Choose an exact variant or one literal published option name and value.');ok=byId?ui.chooseVariant(action.variantId):ui.chooseOption(action.optionName,action.optionValue);
      }else if(type==='product-quantity')ok=ui.setQuantity(action.quantity);else if(type==='review-add'){if(action.variantId&&action.variantId!==ui.select.value)return controlResult(type,false,'Select that exact published option before reviewing it.');ok=ui.prepareReview();}
      if(!ok)return controlResult(type,false,type==='review-add'?'Choose a complete available exact option first. Customization or held pieces must be checked with the shop.':'That exact published option or quantity is unavailable. No choice was invented.');publish();const message=type==='options'?'The published option menu is open.':type==='select-option'?ui.select.value?'Your exact published option is selected.':'Your choice is shown; the remaining published options still need your selection.':type==='product-quantity'?'Quantity '+ui.quantity()+' is selected for this exact test piece.':'Your exact test-bag choice is ready for your confirmation click.';status(message);return controlResult(type,true,message,type==='review-add'?{requiredCustomerClick:'Confirm add to test bag',cartChanged:false}:{});
    }finally{guard?.dispose();}
  }
  async function setGiftPreferences(action){
    const patch={};for(const key of ['wrapping','giftPackage'])if(action[key]!==undefined){if(typeof action[key]!=='boolean')return controlResult(action.type,false,'Use a clear yes or no for the test gift preference.');patch[key]=action[key];}
    if(action.giftNote!==undefined){if(typeof action.giftNote!=='string'||action.giftNote.length>300||/[\u0000-\u0008\u000b\u000c\u000e-\u001f]/.test(action.giftNote))return controlResult(action.type,false,'Please keep the exact test gift note within 300 characters.');patch.giftNote=action.giftNote;}
    if(!Object.keys(patch).length)return controlResult(action.type,false,'Please state the test gift preference you want saved.');savePreferences(patch);if(!main.querySelector('.service-panel'))renderServicePanel(main);const prefs=readPreferences();main.querySelectorAll('[data-store-section=gifts]').forEach(form=>{const wrap=form.querySelector('[name=wrapping]'),pack=form.querySelector('[name=package]'),note=form.querySelector('[name=note]');if(wrap)wrap.checked=prefs.wrapping===true;if(pack)pack.checked=prefs.giftPackage===true;if(note)note.value=prefs.giftNote||'';});publish();status('Your gift preferences are saved only in this test session. Nothing was sent to the shop.');return controlResult(action.type,true,notice.textContent);
  }
  async function renderCheckout({push=true,signal}={}){
    const cart=readCart();if(!cart.length){renderBag({push});return {ok:false,action:'checkout',message:'Choose a piece for your test bag first.'};}
    const record=begin({signal});status('Checking the exact bag options before test checkout…');
    try{await verifyBag(cart,record.controller.signal);if(!current(record))return {ok:false,action:'checkout',message:'Test checkout was cancelled.'};if(JSON.stringify(readCart())!==JSON.stringify(cart))throw Error('Your bag changed. Please reopen test checkout.');}catch(e){if(current(record)){state.loading=false;publish();status(e.message);}return {ok:false,action:'checkout',message:e.message||'The exact bag choices could not be checked.'};}
    main.replaceChildren();const page=node('section',null,'checkout-page view-enter');page.dataset.storeSection='checkout';page.append(button('← Return to test bag','back-link',()=>renderBag()),node('span','A COMPLETE SIMULATION','eyebrow'),node('h1','A thoughtful finishing touch.'),node('p','Try the checkout experience without creating an order or providing payment, contact or address details.','bag-intro'));
    const steps=node('nav',null,'checkout-steps');steps.setAttribute('aria-label','Test checkout steps');[['review','Review exact pieces'],['shipping','Demo shipping & gifts'],['confirm','Confirm the simulation']].forEach(([step,label])=>{const b=button(label,'secondary',()=>void execute({type:'checkout-step',step}));b.dataset.checkoutStepButton=step;b.setAttribute('aria-current',step==='review'?'step':'false');steps.append(b);});page.append(steps);
    const grid=node('div',null,'checkout-grid'),summary=node('section',null,'checkout-box checkout-step');summary.dataset.checkoutStep='review';summary.append(node('h2','Your exact choices'));const lineIds=cartIdentities(cart);cart.forEach((p,index)=>{const row=node('div',null,'checkout-line');row.dataset.bagLine=lineIds[index];row.append(node('p',p.title+' · '+p.variant+' · quantity '+(p.quantity||1)),node('p',money(p.price*(p.quantity||1),p.currency)));summary.append(row);});
    const prefs=readPreferences(),services=node('section',null,'checkout-box checkout-step');services.dataset.storeSection='shipping';services.dataset.checkoutStep='shipping';services.append(node('h2','Shipping & gifting'),node('p','Production normally takes 2–3 days according to Brites. Shipping choices, shipping time and final charges are confirmed by the shop.'));
    const demo=node('div',null,'demo-shipping');demo.append(node('p','These two shipping labels demonstrate the interface only. They are not live services, rates or delivery promises.'));[['standard','Demo standard'],['express','Demo express']].forEach(([value,label])=>{const b=button(label,'secondary',()=>void execute({type:'checkout-option',option:'shipping',value}));b.dataset.demoShipping=value;b.setAttribute('aria-pressed',String(value==='standard'));demo.append(b);});services.append(demo);[['wrapping',prefs.wrapping?'Gift wrapping enquiry saved.':'No gift wrapping enquiry saved.'],['package',prefs.giftPackage?'Gift package enquiry saved.':'No gift package enquiry saved.'],['note',prefs.giftNote?'Test gift note: '+prefs.giftNote:'No test gift note saved.']].forEach(([key,text])=>{const line=node('p',text);line.dataset.checkoutGift=key;services.append(line);});services.append(button('Adjust gift preferences','text-link',()=>void showService('gifts')));grid.append(summary,services);page.append(grid);
    const acknowledgement=node('label',null,'checkout-check'),check=document.createElement('input');check.type='checkbox';check.id='confirm-test-checkout';acknowledgement.append(check,document.createTextNode('I understand this completes a test only. No order, payment or message will be sent.'));const complete=button('Complete test checkout','primary');complete.disabled=true;check.addEventListener('change',()=>{complete.disabled=!check.checked;});
    const confirm=node('section',null,'checkout-box checkout-step');confirm.dataset.checkoutStep='confirm';confirm.append(node('h2','Complete the test only'),acknowledgement,complete);page.append(confirm);
    const ui={page,step:'review',shipping:'standard',check,completed:false,completionController:null};checkoutUI=ui;check.addEventListener('change',()=>publish());
    complete.addEventListener('click',async()=>{if(!check.checked||complete.disabled||ui.completed||ui.completionController)return;complete.disabled=true;const version=navigationVersion,controller=new AbortController(),preferences=JSON.stringify(readPreferences()),shipping=ui.shipping;ui.completionController=controller;try{const fresh=readCart();if(JSON.stringify(fresh)!==JSON.stringify(cart))throw Error('Your bag changed. Reopen test checkout before completing.');await verifyBag(fresh,controller.signal);if(controller.signal.aborted||version!==navigationVersion||state.pageKind!=='checkout'||!page.isConnected||checkoutUI!==ui)return;if(JSON.stringify(readCart())!==JSON.stringify(cart))throw Error('Your bag changed during the check. Reopen test checkout.');if(JSON.stringify(readPreferences())!==preferences||ui.shipping!==shipping)throw Error('Your gift or demo shipping choice changed. Review it before completing.');const receipt=node('section',null,'receipt');receipt.append(node('span','TEST COMPLETE','eyebrow'),node('h2','Your selection came together.'),node('p','This was a local checkout simulation. Your test bag is preserved. No order was placed, no payment was taken and no details were sent.'));page.append(receipt);check.disabled=true;ui.completed=true;ui.step='complete';status('Test checkout complete. No real purchase was made.');publish();}catch(e){if(page.isConnected&&checkoutUI===ui){status(e.message||'The test checkout could not be completed.');complete.disabled=!check.checked;}}finally{ui.completionController=null;}});
    main.append(page);if(full){renderServicePanel(page);void loadServices();}state.current=null;commitPage('checkout','',push);status('Test checkout is ready. No payment is connected.');return {ok:true,action:'checkout',message:'The checked test checkout is ready. No real order or payment is possible.',snapshot:snapshot()};
  }
  function runCheckoutControl(action){
    const ui=checkoutUI;if(!ui?.page?.isConnected||state.pageKind!=='checkout'||ui.completed)return controlResult(action.type,false,'Open the current checked test checkout first.');
    if(action.type==='checkout-option'){if(action.option!=='shipping'||!['standard','express'].includes(action.value))return controlResult(action.type,false,'Choose only the labelled demo standard or express interface option.');ui.shipping=action.value;ui.page.querySelectorAll('[data-demo-shipping]').forEach(b=>b.setAttribute('aria-pressed',String(b.dataset.demoShipping===action.value)));status('Demo '+action.value+' is selected. The shop still confirms real shipping availability, rates and timing.');publish();return controlResult(action.type,true,notice.textContent);}
    const step=action.type==='checkout-complete'?'confirm':action.step;if(!['review','shipping','confirm'].includes(step))return controlResult(action.type,false,'Choose a current test-checkout step.');ui.step=step;ui.page.querySelectorAll('[data-checkout-step-button]').forEach(b=>b.setAttribute('aria-current',b.dataset.checkoutStepButton===step?'step':'false'));ui.page.querySelectorAll('.checkout-step.store-highlight').forEach(n=>n.classList.remove('store-highlight'));const target=ui.page.querySelector('[data-checkout-step="'+step+'"]');if(target&&!document.hidden){target.classList.add('store-highlight');try{target.scrollIntoView?.({behavior:reduced()?'auto':'smooth',block:'center'});}catch{}}publish();const message=action.type==='checkout-complete'?'Review the simulation, tick its acknowledgement and click Complete test checkout yourself. No order or payment is possible.':'The '+step+' step of the test checkout is in view.';status(message);return controlResult(action.type,true,message,action.type==='checkout-complete'?{requiredCustomerClick:'Complete test checkout',completed:false,orderPlaced:false}:{});
  }
  async function execute(action,options={}){
    if(!action||typeof action!=='object'||Array.isArray(action)||options.signal?.aborted)return {ok:false,action:'',message:'That action was cancelled or unavailable.'};
    const type=action.type;
    if(['options','select-option','product-quantity','review-add'].includes(type))return runProductControl(action,options);
    if(type==='bag-quantity'||type==='bag-remove')return editBag(action,options);
    if(type==='gift-preferences')return setGiftPreferences(action);
    if(['checkout-step','checkout-option','checkout-complete'].includes(type))return runCheckoutControl(action);
    if(type==='search')return searchCatalogue(action.query,options,action);
    if(type==='sort'||type==='filter'){
      const value=type==='sort'?action.sort:action.filter;if(!(type==='sort'?SORTS:FILTERS).has(value))return {ok:false,action:type,message:'That collection control is unavailable.'};
      const leavingPage=state.pageKind!=='catalogue',resetScope=leavingPage||type==='filter'&&value!=='available'&&(state.search!==''||state.collectionSource!=='browse');
      if(!state.browseVerifiedAt&&(resetScope||state.collectionSource==='browse'&&state.search===''))return searchCatalogue('',options,{sort:type==='sort'?value:state.sort,filter:type==='filter'?value:'all'},type==='filter');
      cancelPending();if(resetScope)restoreBrowse();state[type==='sort'?'sort':'filter']=value;state.limit=PAGE_SIZE;if(type==='filter')reviseDiscovery();if(leavingPage){restoreCollection();commitPage('catalogue','',options.push!==false);}else{restoreCollection();publish();}
      const pieces=filtered().slice(0,state.limit),message=type==='sort'?'The loaded collection is sorted.':pieces.length?'The '+(value==='all'?'full loaded collection':value)+' is ready.':state.pageInfo.hasNextPage?'No '+(value==='all'?'pieces':value)+' on these loaded pages yet. Explore more pieces to continue.':'No checked '+(value==='all'?'pieces':value)+' in this loaded collection. Try a new live search.';
      focusSection('catalogue',false);status(message);return {ok:true,action:type,live:state.verifiedAt>0,checkedAt:state.verifiedAt,products:pieces.map(projection),snapshot:snapshot(),message};
    }
    if(type==='open'){const opened=await openProduct(action.handle,{signal:options.signal,section:SECTIONS.has(action.section)?action.section:undefined,revealRequested:true}),ok=opened&&state.current?.handle===action.handle;return {ok,action:type,...(ok?{live:true,checkedAt:state.verifiedAt,products:[projection(state.current)],snapshot:snapshot()}:{}),message:ok?'The checked product details are open.':'That piece could not be checked.'};}
    if(type==='highlight'||type==='scroll'||type==='zoom'){
      const section=type==='zoom'?'image':action.section;if(!SECTIONS.has(section))return {ok:false,action:type,message:'That page section is unavailable.'};
      const revealFocus=trackRevealFocus(focusedElement());try{
      if(action.handle&&(action.handle!==state.currentHandle||!state.current)){if(!await openProduct(action.handle,{signal:options.signal}))return {ok:false,action:type,message:'That piece could not be checked.'};}
      if(options.signal?.aborted||document.hidden||!revealFocus.allowed())return {ok:false,action:type,message:'The earlier page reveal was cancelled. Your newer view is preserved.'};
      if(section==='shipping'||section==='gifts'||section==='customize'||section==='offers'){await showService(section);return {ok:true,action:type,snapshot:snapshot(),message:'The '+section+' section is open.'};}
      if(type==='zoom'){const ok=state.current?zoomImage(state.current):false;return {ok,action:type,snapshot:snapshot(),message:ok?'The live product image is enlarged.':'Open a piece with a published image first.'};}
      const ok=focusSection(section,type==='highlight',{immediate:true});return {ok,action:type,snapshot:snapshot(),message:ok?'The '+section+' section is highlighted and in view.':'That section is not on the current page.'};
      }finally{revealFocus.dispose();}
    }
    if(type==='bag'){renderBag();return {ok:true,action:type,message:'Your session-only test bag is open.',snapshot:snapshot()};}
    if(type==='checkout')return renderCheckout({signal:options.signal});
    if(type==='gift'||type==='customize')return showService(type==='gift'?'gifts':'customize');
    return {ok:false,action:clean(type,30),message:'That website action is not available in this preview.'};
  }
  function presentProducts(products,options={}){
    if(!Array.isArray(products)||products.length>24||products.some(p=>!validProduct(p,p?.handle)))return {ok:false,action:'present',message:'The checked product selection could not be displayed.'};
    const prior=currentProductUI(),checked=products.map(remember);cancelPending();state.verifiedAt=Date.now();reviseDiscovery();
    if(state.pageKind==='product'&&checked.length===1&&checked[0].handle===state.currentHandle){
      const fresh=checked[0],keys=['id','handle','options','variants','variantsComplete','cartHold','recommendationHold','partsOnly','currency'],signature=(p,fields)=>JSON.stringify(fields.map(key=>p?.[key])),compatible=!!prior&&signature(prior.product,keys)===signature(fresh,keys),renderKeys=keys.concat(['title','type','description','url','image','imageAlt','images']),same=compatible&&signature(prior.product,renderKeys)===signature(fresh,renderKeys),initialFocus=focusedElement();
      if(same){prior.invalidateReview();Object.assign(prior.product,fresh);state.current=prior.product;prior.updateSelection(false);}
      else{const selection=compatible?{variantId:prior.select.value,quantity:prior.quantity(),choices:prior.choices(),opened:!prior.menu.hidden,openedOption:prior.openedOption,focus:initialFocus===prior.select?'variant':initialFocus===prior.quantityInput?'quantity':null}:null;state.current=fresh;renderProduct(fresh);if(selection){if(selection.variantId)productUI.chooseVariant(selection.variantId);else selection.choices.forEach(o=>productUI.chooseOption(o.name,o.value));productUI.setQuantity(selection.quantity);if(selection.opened)productUI.openOptions(selection.openedOption||undefined,false);if(selection.focus&&!document.hidden&&!initialFocus.isConnected&&focusedElement()===document.body)(selection.focus==='variant'?productUI.select:productUI.quantityInput).focus({preventScroll:true});}}
      publish();return {ok:true,action:'present',live:true,checkedAt:state.verifiedAt,products:checked.map(projection),snapshot:snapshot(),message:'The current checked piece is in view.'};
    }
    state.products=checked;state.search='';state.checkedSearch='';state.collectionSource='presented';state.filter='all';state.limit=PAGE_SIZE;state.pageInfo={hasNextPage:false,endCursor:null};restoreCollection();history.replaceState({},'','/concierge-sandbox.html');publish();document.dispatchEvent(new CustomEvent('brites-concierge:page'));
    return {ok:true,action:'present',live:true,checkedAt:state.verifiedAt,products:checked.map(projection),snapshot:snapshot(),message:'The checked selection is displayed in the boutique.'};
  }
  window.BritesSandboxStorefront=Object.freeze({capabilities:CONTROL_CAPABILITIES,snapshot,execute,presentProducts});
  window.BritesSandboxNavigate=handle=>openProduct(handle,{revealRequested:true});
  document.querySelector('#guide-request-form')?.addEventListener('submit',event=>{event.preventDefault();void sendGuideCommand(document.querySelector('#guide-request')?.value||'');});
  renderGuideCommands();
  document.addEventListener('click',event=>{
    const control=event.target.closest?.('[data-store-action]');if(control&&!event.defaultPrevented){const type=control.dataset.storeAction;event.preventDefault();if(type==='bag')void execute({type:'bag'});else if(type==='gifts')void execute({type:'gift'});else if(type==='customize')void execute({type:'customize'});else if(type==='browse')focusSection('catalogue',false);else if(type==='collection')void execute({type:'search',query:''});return;}
    const anchor=event.target.closest?.('a[href]');if(!anchor||event.defaultPrevented||event.button!==0||event.ctrlKey||event.metaKey||event.shiftKey||event.altKey||anchor.target==='_blank')return;
    const target=new URL(anchor.href,location.href);if(target.origin!==location.origin||target.pathname!=='/concierge-sandbox.html')return;const handle=target.searchParams.get('product');if(!handle)return;event.preventDefault();void openProduct(handle,{focusFrom:event.detail===0&&document.activeElement===anchor?anchor:null,revealFrom:event.detail>0&&main.contains(anchor)?anchor:null});
  });
  function attended(target){const card=target?.closest?.('[data-product-handle]'),handle=card?.dataset.productHandle;return main.contains(card)&&validHandle(handle)&&known.has(handle)?handle:'';}
  function focus(handle){if(state.focusedHandle===handle)return;state.focusedHandle=handle;publish();}
  function releaseFocus(){if(focusTimer||!state.focusedHandle)return;focusTimer=setTimeout(()=>{focusTimer=0;focus('');},90);}
  document.addEventListener('pointerover',event=>{const next=attended(event.target);if(next){clearTimeout(focusTimer);focusTimer=0;focus(next);}else releaseFocus();});
  document.addEventListener('pointerout',event=>{if(!event.target.closest?.('[data-product-handle]'))return;const next=attended(event.relatedTarget);if(next){clearTimeout(focusTimer);focusTimer=0;focus(next);}else releaseFocus();});
  document.addEventListener('focusin',event=>{clearTimeout(focusTimer);focusTimer=0;focus(attended(event.target));});
  document.addEventListener('focusout',event=>{if(!main.contains(event.target))return;const next=attended(event.relatedTarget);focus(next);});
  document.addEventListener('brites:cart-updated',event=>{if(event.detail?.sandbox!==true)return;updateBag();if(state.pageKind==='bag')renderBag({push:false});});
  let visibleWindow='';addEventListener('scroll',()=>{if(state.pageKind!=='catalogue')return;const next=snapshot().visiblePieces.map(p=>p.id).join(',');if(next!==visibleWindow){visibleWindow=next;publish();}},{passive:true});
  addEventListener('popstate',()=>{cancelPending();const p=new URLSearchParams(location.search);if(p.get('product'))void openProduct(p.get('product'),{push:false});else if(p.has('cart'))renderBag({push:false});else if(p.has('checkout'))void renderCheckout({push:false});else{state.filter='all';if(!state.browseVerifiedAt){void searchCatalogue('',{push:false},{filter:'all'});return;}restoreBrowse();reviseDiscovery();restoreCollection();commitPage('catalogue','',false);}});
  updateBag();const params=new URLSearchParams(location.search);
  if(params.has('cart')){renderBag({push:false});return;}
  if(params.has('checkout')){await renderCheckout({push:false});return;}
  if(params.has('product')){await openProduct(params.get('product'),{push:false});return;}
  controls();
  const initialVersion=navigationVersion;
  try{
    const data=await catalogueStart();if(state.pageKind!=='catalogue'||navigationVersion!==initialVersion)return;
    const pageInfo=checkedPageInfo(data);state.products=checkedProducts(data);state.browse=state.products.slice();state.pageInfo=pageInfo;state.browsePageInfo={...pageInfo};state.verifiedAt=Date.now();state.browseVerifiedAt=state.verifiedAt;applySeed(data);drawGrid();publish();if(full){renderServiceStrip();void loadServices();}
  }catch{const summary=main.querySelector('#result-summary');if(summary)summary.textContent='The live selection is temporarily unavailable. Try Search again.';else main.append(node('p','The live selection is temporarily unavailable. You can still explore the shop.'));}
})();
