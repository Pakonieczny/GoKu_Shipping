(async function(){
  'use strict';
  const main=document.querySelector('#shop-content'),params=new URLSearchParams(location.search);
  function text(tag,value,className){const e=document.createElement(tag);e.textContent=value;if(className)e.className=className;return e;}
  function image(p,className){if(!p.image)return null;const img=document.createElement('img');img.src=p.image;img.alt=p.imageAlt||p.title;img.loading='lazy';if(className)img.className=className;return img;}
  function money(value,currency){try{return new Intl.NumberFormat('en',{style:'currency',currency}).format(value)+' '+currency;}catch{return value+' '+currency;}}
  function cartItem(value){if(!value||!/^gid:\/\/shopify\/Product\/\d+$/.test(value.productId||'')||!/^\d+$/.test(value.variantId||'')||typeof value.title!=='string'||typeof value.variant!=='string'||!Number.isFinite(value.price)||value.price<0||!/^[A-Z]{3}$/.test(value.currency||''))return null;return {productId:value.productId,title:value.title.slice(0,300),variantId:value.variantId,variant:value.variant.slice(0,300),price:value.price,currency:value.currency};}
  async function get(path){const response=await fetch(path,{cache:'no-store'}),data=await response.json();if(!response.ok)throw Error('The live selection could not be checked.');return data;}
  if(params.has('cart')){
    main.replaceChildren(text('h1','Your sandbox bag'));
    let cart=[];try{const saved=JSON.parse(sessionStorage.getItem('brites-sandbox-cart')||'[]');if(Array.isArray(saved))cart=saved.slice(-50).map(cartItem).filter(Boolean);}catch{}
    if(!cart.length)main.appendChild(text('p','Your sandbox bag is empty.'));
    cart.forEach(p=>main.appendChild(text('p',p.title+' · '+p.variant+' · '+money(p.price,p.currency))));
    main.appendChild(text('p','This test bag never places a shop order.'));
    return;
  }
  try{
    if(params.has('product')){
      const handle=params.get('product');if(!/^[a-z0-9_-]{1,180}$/.test(handle))throw Error('Choose a product from the live selection.');
      const data=await get('/api/growth/product?handle='+encodeURIComponent(handle)),p=data.product;
      main.replaceChildren(text('span','LIVE PRODUCT · SANDBOX VIEW','eyebrow'),text('h1',p.title));
      const photo=image(p,'demo-hero');if(photo)main.appendChild(photo);
      main.appendChild(text('p',p.description.slice(0,1800)));
      main.appendChild(text('h2','Current product options'));
      const options=document.createElement('ul');
      (p.variants||[]).forEach(v=>options.appendChild(text('li',v.title+' · '+money(v.price,p.currency)+(v.available?' · available':' · unavailable'))));
      main.appendChild(options);
      main.appendChild(text('p',p.variantsComplete?'Prices and availability were checked for this view. Shipping and any applicable taxes are confirmed by the real shop.':'This item has more variants than this preview can list. Check the complete selection on the real product page.'));
      if(p.url&&/^https:\/\/(?:www\.)?britesjewelry\.com\/products\/[a-z0-9_-]{1,180}\/?$/.test(p.url)){
        const link=text('a','Open the real product page','pill');link.href=p.url;link.target='_blank';link.rel='noopener noreferrer';main.appendChild(link);
      }
      main.appendChild(text('p','The optional concierge stays with you while browsing. Personalization uses the product page’s own customizer.'));
      return;
    }
    const data=await get('/api/growth/catalogue?q=bunny'),list=document.querySelector('#demo-products');
    data.products.slice(0,8).forEach(p=>{
      const a=document.createElement('a');a.href='/concierge-sandbox.html?product='+encodeURIComponent(p.handle);
      const photo=image(p);if(photo)a.appendChild(photo);
      a.appendChild(text('h3',p.title));const available=(p.variants||[]).filter(v=>v.available);
      if(available.length)a.appendChild(text('p','From '+money(Math.min(...available.map(v=>v.price)),p.currency)));
      list.appendChild(a);
    });
  }catch{
    main.appendChild(text('p','The live selection is temporarily unavailable. You can still explore the shop using the link above.'));
  }
})();
