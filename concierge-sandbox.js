(async function(){
  'use strict';
  const main=document.querySelector('#shop-content');
  if(!main)return;
  const full=document.body?.dataset.storefront==='expanded',home=main.cloneNode(true);
  const HANDLE=/^[a-z0-9]+(?:-[a-z0-9]+)*$/,PRODUCT=/^gid:\/\/shopify\/Product\/[1-9][0-9]{0,19}$/,VARIANT=/^gid:\/\/shopify\/ProductVariant\/[1-9][0-9]{0,19}$/;
  const SORTS=new Set(['featured','price-asc','price-desc','title-asc','title-desc']);
  const FILTERS=new Set(['all','necklaces','earrings','bracelets','rings','charms','available']);
  const SECTIONS=new Set(['price','details','options','story','shipping','gifts','customize','catalogue','image','bag','checkout','offers']);
  const PAGE_SIZE=24,MAX_PIECES=1200;
  const state={pageKind:'catalogue',currentHandle:'',focusedHandle:'',search:'',sort:'featured',filter:'all',contextRevision:0,activeSection:'catalogue',loading:false,products:[],browse:[],limit:PAGE_SIZE,pageInfo:{hasNextPage:false,endCursor:null},current:null,services:null,servicesPending:null,selectedImage:0,verifiedAt:0};
  let navigationVersion=0,request=null,noticeTimer=0,highlightTimer=0,focusTimer=0,storyVersion=0;
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
    try{sessionStorage.setItem('brites-sandbox-product-identities',JSON.stringify([...identities].slice(-100)));}catch{}
    return p;
  }
  try{const saved=JSON.parse(sessionStorage.getItem('brites-sandbox-product-identities')||'[]');if(Array.isArray(saved))saved.slice(-100).forEach(v=>{if(Array.isArray(v)&&PRODUCT.test(v[0]||'')&&validHandle(v[1]))identities.set(v[0],v[1]);});}catch{}
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
  function minimum(p){const available=p.variants.filter(v=>v.available);return available.length?Math.min(...available.map(v=>v.price)):null;}
  function projection(p){const available=p.variants.filter(v=>v.available);return {...p,minPrice:minimum(p),suggestedVariantId:available[0]?.id||null};}
  function filtered(){
    let pieces=state.products.filter(p=>{
      const text=(p.title+' '+(p.type||'')+' '+p.description).toLowerCase(),words=state.search.toLowerCase().match(/[\p{L}\p{N}-]+/gu)||[];
      if(words.length&&!words.every(word=>text.includes(word)))return false;
      if(state.filter==='available')return p.variants.some(v=>v.available);
      if(state.filter==='all')return true;
      const names={necklaces:/necklace|pendant/i,earrings:/earrings?|stud|huggie/i,bracelets:/bracelet/i,rings:/\bring\b/i,charms:/\bcharm\b/i};
      return names[state.filter].test((p.type||'')+' '+p.title.toLowerCase());
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
    return {pageKind:state.pageKind==='catalogue'?'collection':state.pageKind,currentHandle,focusedHandle,visiblePieces,search:state.search,sort:state.sort,filter:state.filter,contextRevision:state.contextRevision,activeSection:state.activeSection,loading:state.loading};
  }
  function publish(){state.contextRevision++;document.dispatchEvent(new CustomEvent('brites-storefront:context',{detail:snapshot()}));}
  function status(message){notice.textContent=clean(message,300);notice.dataset.visible=message?'true':'false';clearTimeout(noticeTimer);if(message)noticeTimer=setTimeout(()=>{notice.dataset.visible='false';},3600);}
  function reduced(){return typeof matchMedia==='function'&&matchMedia('(prefers-reduced-motion: reduce)').matches;}
  function focusSection(section,highlight=true){
    if(!SECTIONS.has(section))return false;
    const target=main.querySelector('[data-store-section="'+section+'"]');if(!target)return false;
    clearTimeout(highlightTimer);main.querySelectorAll('.store-highlight').forEach(e=>e.classList.remove('store-highlight'));
    if(highlight){target.classList.add('store-highlight');highlightTimer=setTimeout(()=>target.classList.remove('store-highlight'),2600);}
    try{target.scrollIntoView?.({behavior:reduced()?'auto':'smooth',block:'center'});}catch{}
    state.activeSection=section;publish();return true;
  }
  function cancelPending(){navigationVersion++;request?.abort();request=null;clearTimeout(highlightTimer);clearTimeout(focusTimer);focusTimer=0;main.querySelectorAll('.store-highlight').forEach(e=>e.classList.remove('store-highlight'));state.loading=false;}
  async function get(path,signal){if(typeof fetch!=='function')throw Error('The live selection could not be checked.');const response=await fetch(path,{cache:'no-store',...(signal?{signal}:{})}),data=await response.json();if(!response.ok)throw Error('The live selection could not be checked.');return data;}
  function begin(options={}){
    cancelPending();request=new AbortController();const record={version:navigationVersion,controller:request};
    if(options.signal){if(options.signal.aborted)request.abort();else options.signal.addEventListener('abort',()=>{record.controller.abort();if(record.version===navigationVersion){state.loading=false;publish();status('The previous website action was cancelled.');}},{once:true});}
    state.loading=!record.controller.signal.aborted;publish();return record;
  }
  function current(record){return record.version===navigationVersion&&!record.controller.signal.aborted;}
  function commitPage(kind,handle,push=true){
    state.pageKind=kind;state.currentHandle=kind==='product'?handle:'';state.focusedHandle='';state.loading=false;
    if(push){const query=kind==='product'?'?product='+encodeURIComponent(handle):kind==='bag'?'?cart=1':kind==='checkout'?'?checkout=1':'';history.pushState({},'','/concierge-sandbox.html'+query);}
    publish();document.dispatchEvent(new CustomEvent('brites-concierge:page'));
  }
  function cartItem(value){if(!value||!PRODUCT.test(value.productId||'')||!/^[1-9][0-9]{0,19}$/.test(value.variantId||'')||typeof value.title!=='string'||typeof value.variant!=='string'||!Number.isFinite(value.price)||value.price<0||!/^[A-Z]{3}$/.test(value.currency||''))return null;return {productId:value.productId,title:value.title.slice(0,300),variantId:value.variantId,variant:value.variant.slice(0,300),price:value.price,currency:value.currency};}
  function readCart(){try{const saved=JSON.parse(sessionStorage.getItem('brites-sandbox-cart')||'[]');return Array.isArray(saved)?saved.slice(-50).map(cartItem).filter(Boolean):[];}catch{return [];}}
  function writeCart(cart){sessionStorage.setItem('brites-sandbox-cart',JSON.stringify(cart.slice(-50)));document.dispatchEvent(new CustomEvent('brites:cart-updated',{detail:{sandbox:true}}));}
  function updateBag(){const count=document.querySelector('#bag-count');if(count)count.textContent=String(readCart().length);}
  function link(label,url){const a=node('a',label);a.href=url;a.target='_blank';a.rel='noopener noreferrer';return a;}
  function needsCustomizer(p,v){
    if(!v||p.cartHold||p.recommendationHold||p.partsOnly||/\bcharms?\b/i.test(p.type||''))return true;
    const opts=(v.options||[]).filter(o=>/engrav|personali[sz]|custom/i.test(o.name)),detail=v.title+' '+opts.map(o=>o.value).join(' ');
    if(/handwrit|monogram|photo|upload|custom studio/i.test(p.title))return true;
    if(/no engraving|not engraved|without engraving|non[ -]?engraved|unengraved/i.test(detail)||opts.some(o=>/^(?:no|none|without|not included)$/i.test(o.value)))return false;
    return /engrav|personali[sz]|custom/i.test(detail)||opts.some(o=>/^(?:yes|included|with)$/i.test(o.value));
  }
  function controls(){
    const box=main.querySelector('#collection-controls');if(!box)return;
    box.replaceChildren();const form=node('form',null,'collection-tools'),wrap=node('div',null,'search-wrap');
    wrap.innerHTML='<svg viewBox="0 0 24 24" width="18" height="18" aria-hidden="true"><circle cx="10.5" cy="10.5" r="6.5" fill="none" stroke="currentColor" stroke-width="1.3"/><path d="m15.5 15.5 5 5" fill="none" stroke="currentColor" stroke-width="1.3"/></svg>';
    const input=document.createElement('input');input.id='store-search';input.type='search';input.maxLength=250;input.placeholder='Search a symbol, piece or material…';input.setAttribute('aria-label','Search the live jewelry collection');input.value=state.search;
    input.addEventListener('input',()=>{cancelPending();state.search=clean(input.value,250);state.limit=PAGE_SIZE;drawGrid();publish();});
    const submit=button('Search');submit.type='submit';wrap.append(input,submit);form.append(wrap);
    const sortLabel=node('label','Sort','tool-select tool-sort'),sort=document.createElement('select');sort.id='store-sort';sort.setAttribute('aria-label','Sort jewelry');
    [['featured','Shop order'],['price-asc','Price: low to high'],['price-desc','Price: high to low'],['title-asc','Name: A–Z'],['title-desc','Name: Z–A']].forEach(([value,label])=>{const o=node('option',label);o.value=value;sort.append(o);});sort.value=state.sort;sort.addEventListener('change',()=>void execute({type:'sort',sort:sort.value}));sortLabel.append(sort);form.append(sortLabel);box.append(form);
    form.addEventListener('submit',e=>{e.preventDefault();void execute({type:'search',query:input.value});});
    const chips=node('div',null,'category-chips');chips.setAttribute('aria-label','Jewelry categories');
    [['all','All pieces'],['necklaces','Necklaces'],['earrings','Earrings'],['bracelets','Bracelets'],['rings','Rings'],['charms','Charms'],['available','Available now']].forEach(([value,label])=>{const b=button(label,'',()=>void execute({type:'filter',filter:value}));b.dataset.filter=value;b.setAttribute('aria-pressed',String(state.filter===value));chips.append(b);});box.append(chips);
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
      const copy=node('div',null,'piece-copy');copy.append(node('h3',p.title));const amount=minimum(p);copy.append(node('p',amount==null?'Currently unavailable':'From '+money(amount,p.currency),'piece-price'));const opts=(p.options||[]).map(o=>o.name).filter(Boolean).slice(0,2);if(opts.length)copy.append(node('p',opts.join(' · '),'piece-meta'));a.append(photo,copy);card.append(a);grid.append(card);
    });
    if(!shown.length){const empty=node('div',null,'empty-collection');empty.append(node('h3','A little more room to explore'),node('p',state.loading?'Checking the live shop for your search…':'No loaded pieces match those choices. Search the live shop, try another symbol, or clear the filters.'),button('Show all pieces','secondary',()=>void execute({type:'search',query:'',filter:'all'})));grid.append(empty);}
    const summary=main.querySelector('#result-summary');if(summary)summary.textContent=(state.loading?'Checking the live shop · ':'')+shown.length+' of '+pieces.length+' loaded pieces'+(state.search?' matching “'+state.search+'”':'')+(state.pageInfo.hasNextPage&&!state.search?' · more collection pages available':'')+(state.filter!=='all'?' · '+state.filter:'');
    const more=main.querySelector('#collection-more');if(more){more.replaceChildren();if(pieces.length>state.limit||state.pageInfo.hasNextPage&&!state.search)more.append(button('Explore more pieces','secondary',()=>void loadMore()),node('p','Continue through the public Brites catalogue.'));}
    main.querySelectorAll('[data-filter]').forEach(e=>e.setAttribute('aria-pressed',String(e.dataset.filter===state.filter)));
    const hero=main.querySelector('#intro-photo');if(hero&&!hero.querySelector('img')&&shown[0]){const img=picture(shown[0]);if(img){img.loading='eager';hero.replaceChildren(img);}}
  }
  function restoreCollection(){
    if(state.pageKind!=='catalogue'||!main.querySelector('#demo-products'))main.replaceChildren(...[...home.childNodes].map(n=>n.cloneNode(true)));
    state.pageKind='catalogue';state.currentHandle='';state.focusedHandle='';state.current=null;state.activeSection='catalogue';controls();drawGrid();if(full)renderServiceStrip();
  }
  async function searchCatalogue(query,options={},action={}){
    if(typeof query!=='string'||query.length>250)return {ok:false,action:'search',message:'Please use a shorter jewelry search.'};
    if(action.sort&&!SORTS.has(action.sort)||action.filter&&!FILTERS.has(action.filter))return {ok:false,action:'search',message:'Those collection controls are not available.'};
    const record=begin(options);state.search=clean(query,250);if(action.sort)state.sort=action.sort;if(action.filter)state.filter=action.filter;state.limit=PAGE_SIZE;restoreCollection();publish();status(state.search?'Finding “'+state.search+'” in the live shop…':'Opening the wider live collection…');
    try{
      const data=await get('/api/growth/catalogue'+(state.search?'?q='+encodeURIComponent(state.search):'?browse=1'),record.controller.signal);
      if(!current(record))return {ok:false,action:'search',message:'The earlier search was cancelled.'};
      const pageInfo=checkedPageInfo(data);state.products=checkedProducts(data);state.pageInfo=pageInfo;
      if(!state.search)state.browse=state.products.slice();state.verifiedAt=Date.now();state.loading=false;drawGrid();publish();focusSection('catalogue',false);status(filtered().length?'Your checked pieces are ready.':'No checked matches yet. Try another symbol or style.');
      return {ok:true,action:'search',live:data.live!==false,checkedAt:state.verifiedAt,products:filtered().slice(0,state.limit).map(projection),snapshot:snapshot(),message:filtered().length?'Your checked matches are ready on the page.':'I couldn’t find a checked match for that search. Try a different symbol or style.'};
    }catch{if(current(record)){state.loading=false;drawGrid();publish();status('The live selection is temporarily unavailable. Your existing view is preserved.');}return {ok:false,action:'search',message:'The live selection could not be checked. Please try again.'};}
  }
  async function loadMore(){
    const available=filtered();if(available.length>state.limit){state.limit=Math.min(MAX_PIECES,state.limit+PAGE_SIZE);drawGrid();publish();return;}
    if(!state.pageInfo.hasNextPage||!state.pageInfo.endCursor||state.search||state.browse.length>=MAX_PIECES)return;
    const cursor=state.pageInfo.endCursor,record=begin();status('Opening the next collection page…');
    try{const data=await get('/api/growth/catalogue?browse=1&cursor='+encodeURIComponent(cursor),record.controller.signal);if(!current(record))return;const pageInfo=checkedPageInfo(data,cursor),additional=checkedProducts(data);const ids=new Set(state.browse.map(p=>p.id));state.browse=state.browse.concat(additional.filter(p=>!ids.has(p.id))).slice(0,MAX_PIECES);state.products=state.browse.slice();state.limit=Math.min(MAX_PIECES,state.limit+PAGE_SIZE);state.pageInfo=pageInfo;state.loading=false;state.verifiedAt=Date.now();drawGrid();publish();status(additional.length?'More live pieces are ready.':pageInfo.hasNextPage?'This page has no additional matching pieces. Continue to the next public collection page.':'You have reached the end of the checked public collection.');}catch{if(current(record)){state.loading=false;publish();status('That collection page could not be checked. Try Explore more pieces again.');}}
  }
  async function openProduct(handle,{push=true,signal,section}={}){
    if(!validHandle(handle))return false;
    const record=begin({signal});status('Opening the checked product details…');
    try{
      const data=await get('/api/growth/product?handle='+encodeURIComponent(handle),record.controller.signal),p=data.product;
      if(!current(record)||data.live===false||!validProduct(p,handle)){if(current(record)){state.loading=false;publish();}return false;}
      remember(p);state.current=p;state.selectedImage=0;state.verifiedAt=Date.now();state.activeSection=section||'details';renderProduct(p);commitPage('product',handle,push);if(section)focusSection(section);status('Live options checked. Your guide stays with you.');return true;
    }catch{if(current(record)){state.loading=false;publish();status('This piece could not be checked. Your current page is preserved.');}return false;}
  }
  function productImages(p){
    const images=(Array.isArray(p.images)?p.images:[]).slice(0,15).flatMap(i=>{const image=typeof i==='string'?i:i?.url,src=safeImage(image);return src?[{image:src,imageAlt:clean(typeof i==='object'?i.altText||p.title:p.title,300)}]:[];});
    const first=safeImage(p.image);if(first&&!images.some(i=>i.image===first))images.unshift({image:first,imageAlt:clean(p.imageAlt||p.title,300)});return images;
  }
  function renderProduct(p){
    main.replaceChildren();main.append(button('← Back to the collection','back-link',()=>void execute({type:'search',query:''})));
    const layout=node('div',null,'product-layout view-enter'),gallery=node('section',null,'product-gallery'),images=productImages(p);gallery.dataset.storeSection='image';
    const enlarge=button('','product-image-button',()=>zoomImage(p));enlarge.setAttribute('aria-label','Enlarge image of '+p.title);const photo=images[0]?picture(images[0]):null;enlarge.append(photo||node('span','b.','piece-placeholder'));if(images.length)enlarge.append(node('span','Enlarge ↗','image-zoom-label'));else enlarge.disabled=true;gallery.append(enlarge);
    if(images.length>1){const thumbs=node('div',null,'image-thumbs');images.forEach((value,index)=>{const b=button('','',()=>{state.selectedImage=index;const img=picture(value);if(img){img.loading='eager';enlarge.replaceChildren(img,node('span','Enlarge ↗','image-zoom-label'));}thumbs.querySelectorAll('button').forEach((v,j)=>v.setAttribute('aria-pressed',String(index===j)));});b.setAttribute('aria-label','View product image '+(index+1));b.setAttribute('aria-pressed',String(index===0));const img=picture(value);if(img)b.append(img);thumbs.append(b);});gallery.append(thumbs);}
    const copy=node('section',null,'product-copy');copy.dataset.productHandle=p.handle;copy.dataset.productId=p.id;copy.append(node('span',p.type||'A PERSONAL BRITES PIECE','eyebrow'),node('h1',p.title));
    const price=node('p',minimum(p)==null?'Currently unavailable':'From '+money(minimum(p),p.currency),'product-price');price.dataset.storeSection='price';copy.append(price,node('p',p.variants.some(v=>v.available)?'Live options and availability checked for this view':'This piece is currently unavailable','availability'),node('p',p.description.slice(0,12000),'product-description'));
    const options=node('section',null,'option-area');options.dataset.storeSection='options';const label=node('label','Choose your exact piece');label.htmlFor='piece-variant';const select=document.createElement('select');select.id='piece-variant';select.setAttribute('aria-label','Choose an exact product option');select.append(node('option','Select an option…'));select.firstChild.value='';
    p.variants.forEach(v=>{const o=node('option',v.title+' · '+money(v.price,p.currency)+(v.available?'':' · unavailable'));o.value=v.id;o.disabled=!v.available;select.append(o);});
    const selected=node('dl',null,'exact-options'),add=button('Choose an option to add','primary');add.disabled=true;let adding=false;
    function updateSelection(){const v=p.variants.find(v=>v.id===select.value);selected.replaceChildren();(v?.options||[]).slice(0,12).forEach(o=>{const line=node('div');line.append(node('dt',clean(o.name,100)+':'),node('dd',clean(o.value,200)));selected.append(line);});price.textContent=v?money(v.price,p.currency):minimum(p)==null?'Currently unavailable':'From '+money(minimum(p),p.currency);select.disabled=adding;add.disabled=adding||!v||!v.available||!p.variantsComplete||needsCustomizer(p,v);add.textContent=adding?'Checking this exact option…':v&&needsCustomizer(p,v)?'Customize with the shop':v?'Add this exact option to test bag':'Choose an option to add';}
    select.addEventListener('change',()=>{updateSelection();publish();});
    add.addEventListener('click',async()=>{
      const v=p.variants.find(v=>v.id===select.value),version=navigationVersion;
      if(adding||!select.isConnected||state.currentHandle!==p.handle||!v?.available||needsCustomizer(p,v)||!p.variantsComplete)return;
      const admitted=()=>select.isConnected&&state.pageKind==='product'&&state.currentHandle===p.handle&&state.current?.id===p.id&&select.value===v.id&&navigationVersion===version;
      adding=true;updateSelection();
      try{
        const data=await get('/api/growth/product?handle='+encodeURIComponent(p.handle)),live=data.product,exact=live?.variants?.find(x=>x.id===v.id);
        if(!admitted())return;
        if(data.live===false||!validProduct(live,p.handle)||live.id!==p.id||!live.variantsComplete||needsCustomizer(live,exact)||!exact?.available||live.currency!==p.currency||Math.abs(exact.price-v.price)>.005)throw Error('The option, price or availability changed. Check the details again.');
        const cart=readCart();cart.push({productId:p.id,title:p.title,variantId:v.id.split('/').pop(),variant:v.title,price:v.price,currency:p.currency});writeCart(cart);status('Added to your test bag. No real shop order is created.');
      }catch(e){if(admitted())status(e.message||'This option could not be checked. Please try again.');}
      finally{adding=false;if(select.isConnected)updateSelection();}
    });
    options.append(label,select,selected,add,node('p',p.variantsComplete?'Exact options above come from the product listing. Engraving or other custom inputs need the shop’s own customizer.':'This preview does not contain every variant. Confirm complete options with the shop before ordering.','option-help'));copy.append(options);
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
  function savePreferences(p){sessionStorage.setItem('brites-sandbox-gift-preferences',JSON.stringify({...readPreferences(),...p}));}
  async function showService(section){
    if(!main.querySelector('.service-panel'))renderServicePanel(main);
    focusSection(section);void loadServices();
    return {ok:true,action:section==='gifts'?'gift':section==='customize'?'customize':'scroll',message:'The '+section+' section is open. Preferences stay in this test session.',snapshot:snapshot()};
  }
  function renderBag({push=true}={}){
    cancelPending();state.current=null;main.replaceChildren();const page=node('section',null,'bag-page view-enter');page.dataset.storeSection='bag';page.append(node('span','YOUR SESSION-ONLY SELECTION','eyebrow'),node('h1','Your sandbox bag'),node('p','A place to try your choices together. This test bag never places a shop order.','bag-intro'));const cart=readCart(),items=node('div',null,'bag-items');
    if(!cart.length)items.append(node('p','Your sandbox bag is empty.','bag-intro'));
    cart.forEach((p,index)=>{const row=node('article',null,'bag-item'),copy=node('div');copy.append(node('h3',p.title),node('p',p.variant),button('Remove this test piece','text-link',()=>{const next=readCart();next.splice(index,1);writeCart(next);renderBag({push:false});}));const handle=identities.get(p.productId);if(handle)copy.append(button('View its live details','text-link',()=>void openProduct(handle)));row.append(copy,node('p',money(p.price,p.currency)));items.append(row);});
    page.append(items);const totals=new Map();cart.forEach(p=>totals.set(p.currency,(totals.get(p.currency)||0)+p.price));totals.forEach((amount,currency)=>{const total=node('div',null,'bag-total');total.append(node('span','Test item subtotal'),node('strong',money(amount,currency)));page.append(total);});if(cart.length)page.append(node('p','Items only. Shipping, taxes, gift services and any discounts are not calculated in this preview.','bag-subtext'));
    const actions=node('div',null,'bag-actions');actions.append(button('Continue exploring','secondary',()=>void execute({type:'search',query:''})));if(cart.length)actions.append(button('Try test checkout →','primary',()=>void execute({type:'checkout'})));page.append(actions);main.append(page);if(full)renderServicePanel(page);commitPage('bag','',push);updateBag();
  }
  async function verifyBag(cart,signal){
    const requests=new Map();for(const item of cart){const handle=identities.get(item.productId);if(!handle)throw Error('One saved piece needs its product page checked before test checkout.');if(!requests.has(handle))requests.set(handle,get('/api/growth/product?handle='+encodeURIComponent(handle),signal));}
    const results=await Promise.all([...requests].map(async([handle,pending])=>({handle,data:await pending}))),products=new Map();for(const {handle,data:d}of results){if(d.live===false||!validProduct(d.product,handle))throw Error('A bag piece could not be checked.');products.set(d.product.id,d.product);}
    for(const item of cart){const p=products.get(item.productId),v=p?.variants.find(v=>v.id==='gid://shopify/ProductVariant/'+item.variantId);if(!p||!p.variantsComplete||!v?.available||needsCustomizer(p,v)||p.currency!==item.currency||Math.abs(v.price-item.price)>.005)throw Error('A saved option, price or availability changed. Reopen that piece before test checkout.');}return products;
  }
  async function renderCheckout({push=true,signal}={}){
    const cart=readCart();if(!cart.length){renderBag({push});return {ok:false,action:'checkout',message:'Choose a piece for your test bag first.'};}
    const record=begin({signal});status('Checking the exact bag options before test checkout…');
    try{await verifyBag(cart,record.controller.signal);if(!current(record))return {ok:false,action:'checkout',message:'Test checkout was cancelled.'};}catch(e){if(current(record)){state.loading=false;publish();status(e.message);}return {ok:false,action:'checkout',message:e.message||'The exact bag choices could not be checked.'};}
    main.replaceChildren();const page=node('section',null,'checkout-page view-enter');page.dataset.storeSection='checkout';page.append(button('← Return to test bag','back-link',()=>renderBag()),node('span','A COMPLETE SIMULATION','eyebrow'),node('h1','A thoughtful finishing touch.'),node('p','Try the checkout experience without creating an order or providing payment, contact or address details.','bag-intro'));const grid=node('div',null,'checkout-grid'),summary=node('section',null,'checkout-box');summary.append(node('h2','Your exact choices'));cart.forEach(p=>summary.append(node('p',p.title+' · '+p.variant),node('p',money(p.price,p.currency))));const prefs=readPreferences(),services=node('section',null,'checkout-box');services.dataset.storeSection='shipping';services.append(node('h2','Shipping & gifting'),node('p','Production normally takes 2–3 days according to Brites. Shipping choices, shipping time and final charges are confirmed by the shop.'),node('p',prefs.wrapping?'Gift wrapping enquiry saved.':'No gift wrapping enquiry saved.'),node('p',prefs.giftPackage?'Gift package enquiry saved.':'No gift package enquiry saved.'),node('p',prefs.giftNote?'Test gift note: '+prefs.giftNote:'No test gift note saved.'),button('Adjust gift preferences','text-link',()=>void showService('gifts')));grid.append(summary,services);page.append(grid);
    const acknowledgement=node('label',null,'checkout-check'),check=document.createElement('input');check.type='checkbox';check.id='confirm-test-checkout';acknowledgement.append(check,document.createTextNode('I understand this completes a test only. No order, payment or message will be sent.'));const complete=button('Complete test checkout','primary');complete.disabled=true;check.addEventListener('change',()=>{complete.disabled=!check.checked;});
    complete.addEventListener('click',async()=>{if(!check.checked||complete.disabled)return;complete.disabled=true;try{const fresh=readCart();if(JSON.stringify(fresh)!==JSON.stringify(cart))throw Error('Your bag changed. Reopen test checkout before completing.');await verifyBag(fresh);if(state.pageKind!=='checkout'||!page.isConnected)return;if(JSON.stringify(readCart())!==JSON.stringify(cart))throw Error('Your bag changed during the check. Reopen test checkout.');const receipt=node('section',null,'receipt');receipt.append(node('span','TEST COMPLETE','eyebrow'),node('h2','Your selection came together.'),node('p','This was a local checkout simulation. Your test bag is preserved. No order was placed, no payment was taken and no details were sent.'));page.append(receipt);check.disabled=true;status('Test checkout complete. No real purchase was made.');publish();}catch(e){status(e.message||'The test checkout could not be completed.');complete.disabled=!check.checked;}});
    page.append(acknowledgement,complete);main.append(page);if(full){renderServicePanel(page);void loadServices();}state.current=null;commitPage('checkout','',push);status('Test checkout is ready. No payment is connected.');return {ok:true,action:'checkout',message:'The checked test checkout is ready. No real order or payment is possible.',snapshot:snapshot()};
  }
  async function execute(action,options={}){
    if(!action||typeof action!=='object'||Array.isArray(action)||options.signal?.aborted)return {ok:false,action:'',message:'That action was cancelled or unavailable.'};
    const type=action.type;
    if(type==='search')return searchCatalogue(action.query,options,action);
    if(type==='sort'||type==='filter'){
      const value=type==='sort'?action.sort:action.filter;if(!(type==='sort'?SORTS:FILTERS).has(value))return {ok:false,action:type,message:'That collection control is unavailable.'};
      if(state.pageKind!=='catalogue'&&!state.browse.length)return searchCatalogue('',options,{sort:type==='sort'?value:state.sort,filter:type==='filter'?value:state.filter});
      cancelPending();state[type==='sort'?'sort':'filter']=value;state.limit=PAGE_SIZE;if(state.pageKind!=='catalogue'){state.products=state.browse.slice();state.search='';restoreCollection();commitPage('catalogue','');}else{controls();drawGrid();publish();}
      focusSection('catalogue',false);status(type==='sort'?'The loaded collection is smoothly sorted.':'Your collection filter is applied.');return {ok:true,action:type,live:state.verifiedAt>0,checkedAt:state.verifiedAt,products:filtered().slice(0,state.limit).map(projection),snapshot:snapshot(),message:'The loaded collection has been '+(type==='sort'?'sorted.':'filtered.')};
    }
    if(type==='open'){const opened=await openProduct(action.handle,{signal:options.signal,section:SECTIONS.has(action.section)?action.section:undefined}),ok=opened&&state.current?.handle===action.handle;return {ok,action:type,...(ok?{live:true,checkedAt:state.verifiedAt,products:[projection(state.current)],snapshot:snapshot()}:{}),message:ok?'The checked product details are open.':'That piece could not be checked.'};}
    if(type==='highlight'||type==='scroll'||type==='zoom'){
      const section=type==='zoom'?'image':action.section;if(!SECTIONS.has(section))return {ok:false,action:type,message:'That page section is unavailable.'};
      if(action.handle&&(action.handle!==state.currentHandle||!state.current)){if(!await openProduct(action.handle,{signal:options.signal}))return {ok:false,action:type,message:'That piece could not be checked.'};}
      if(section==='shipping'||section==='gifts'||section==='customize'||section==='offers'){await showService(section);return {ok:true,action:type,snapshot:snapshot(),message:'The '+section+' section is open.'};}
      if(type==='zoom'){const ok=state.current?zoomImage(state.current):false;return {ok,action:type,snapshot:snapshot(),message:ok?'The live product image is enlarged.':'Open a piece with a published image first.'};}
      const ok=focusSection(section,type==='highlight');return {ok,action:type,snapshot:snapshot(),message:ok?'The '+section+' section is highlighted and in view.':'That section is not on the current page.'};
    }
    if(type==='bag'){renderBag();return {ok:true,action:type,message:'Your session-only test bag is open.',snapshot:snapshot()};}
    if(type==='checkout')return renderCheckout({signal:options.signal});
    if(type==='gift'||type==='customize')return showService(type==='gift'?'gifts':'customize');
    return {ok:false,action:clean(type,30),message:'That website action is not available in this preview.'};
  }
  function presentProducts(products,options={}){
    if(!Array.isArray(products)||products.length>24||products.some(p=>!validProduct(p,p?.handle)))return {ok:false,action:'present',message:'The checked product selection could not be displayed.'};
    const checked=products.map(remember);cancelPending();state.verifiedAt=Date.now();
    if(state.pageKind==='product'&&checked.length===1&&checked[0].handle===state.currentHandle){state.current=checked[0];publish();return {ok:true,action:'present',live:true,checkedAt:state.verifiedAt,products:checked.map(projection),snapshot:snapshot(),message:'The current checked piece is in view.'};}
    state.products=checked;state.search='';state.filter='all';state.limit=PAGE_SIZE;state.pageInfo={hasNextPage:false,endCursor:null};restoreCollection();history.replaceState({},'','/concierge-sandbox.html');publish();document.dispatchEvent(new CustomEvent('brites-concierge:page'));
    return {ok:true,action:'present',live:true,checkedAt:state.verifiedAt,products:checked.map(projection),snapshot:snapshot(),message:'The checked selection is displayed in the boutique.'};
  }
  window.BritesSandboxStorefront=Object.freeze({snapshot,execute,presentProducts});
  window.BritesSandboxNavigate=handle=>openProduct(handle);
  document.addEventListener('click',event=>{
    const control=event.target.closest?.('[data-store-action]');if(control&&!event.defaultPrevented){const type=control.dataset.storeAction;event.preventDefault();if(type==='bag')void execute({type:'bag'});else if(type==='gifts')void execute({type:'gift'});else if(type==='customize')void execute({type:'customize'});else if(type==='browse')focusSection('catalogue',false);else if(type==='collection')void execute({type:'search',query:''});return;}
    const anchor=event.target.closest?.('a[href]');if(!anchor||event.defaultPrevented||event.button!==0||event.ctrlKey||event.metaKey||event.shiftKey||event.altKey||anchor.target==='_blank')return;
    const target=new URL(anchor.href,location.href);if(target.origin!==location.origin||target.pathname!=='/concierge-sandbox.html')return;const handle=target.searchParams.get('product');if(!handle)return;event.preventDefault();void openProduct(handle);
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
  addEventListener('popstate',()=>{cancelPending();const p=new URLSearchParams(location.search);if(p.get('product'))void openProduct(p.get('product'),{push:false});else if(p.has('cart'))renderBag({push:false});else if(p.has('checkout'))void renderCheckout({push:false});else{state.products=state.browse.slice();state.search='';restoreCollection();commitPage('catalogue','',false);}});
  updateBag();const params=new URLSearchParams(location.search);
  if(params.has('cart')){renderBag({push:false});return;}
  if(params.has('checkout')){await renderCheckout({push:false});return;}
  if(params.has('product')){await openProduct(params.get('product'),{push:false});return;}
  controls();
  const initialVersion=navigationVersion;
  try{
    const data=await get('/api/growth/catalogue?browse=1');if(state.pageKind!=='catalogue'||navigationVersion!==initialVersion)return;
    const pageInfo=checkedPageInfo(data);state.products=checkedProducts(data);state.browse=state.products.slice();state.pageInfo=pageInfo;state.verifiedAt=Date.now();drawGrid();publish();if(full){renderServiceStrip();void loadServices();}
  }catch{const summary=main.querySelector('#result-summary');if(summary)summary.textContent='The live selection is temporarily unavailable. Try Search again.';else main.append(node('p','The live selection is temporarily unavailable. You can still explore the shop.'));}
})();
