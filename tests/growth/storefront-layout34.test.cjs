'use strict';
// Actual HTML and CSSOM cascades under declared media conditions. JSDOM does
// not render layout: physical overflow, focus visibility and guide separation
// still require the published browser check.
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs');
const {JSDOM,VirtualConsole}=require('jsdom');
const html=fs.readFileSync(require.resolve('../../concierge-sandbox.html'),'utf8');
const css=fs.readFileSync(require.resolve('../../concierge-sandbox.css'),'utf8');

function matches(query,width,reduced){
  return query.split(',').some(part=>{
    if(/prefers-reduced-motion:\s*reduce/.test(part)&&!reduced)return false;
    if(/prefers-reduced-motion:\s*no-preference/.test(part)&&reduced)return false;
    const minimum=part.match(/min-width:\s*(\d+)px/),maximum=part.match(/max-width:\s*(\d+)px/);
    return (!minimum||width>=Number(minimum[1]))&&(!maximum||width<=Number(maximum[1]));
  });
}
function mediaCascade(rules,width,reduced){
  return [...rules].map(rule=>rule.type===4?(matches(rule.conditionText,width,reduced)?mediaCascade(rule.cssRules,width,reduced):''):rule.cssText).join('\n');
}
function fixture(t,{width=1363,reduced=false,docked=false}={}){
  const errors=[],vc=new VirtualConsole();vc.on('jsdomError',e=>errors.push(e.message));
  const dom=new JSDOM(html,{url:'https://boutique.example/concierge-sandbox.html',virtualConsole:vc}),w=dom.window,d=w.document;
  const original=d.createElement('style');original.textContent=css;d.head.append(original);
  const flattened=mediaCascade(original.sheet.cssRules,width,reduced);original.remove();
  const style=d.createElement('style');style.textContent=flattened;d.head.append(style);
  if(docked){const guide=d.createElement('brites-concierge');guide.dataset.storefrontDocked='true';guide.dataset.open='true';d.body.append(guide);}
  const components=d.createElement('section');components.innerHTML=`
    <div class="product-layout"><div class="product-gallery"></div><div class="product-copy"><div class="option-area">
      <button type="button" class="option-menu-trigger" aria-expanded="true" aria-controls="exact-menu">Explore exact options</button>
      <div class="option-menu" id="exact-menu"><div class="option-group" data-option-name="Metal">
        <h3 class="option-title">Metal</h3>
        <button class="option-choice" id="selected-option" aria-pressed="true">14k Gold Filled</button>
        <button class="option-choice" id="other-option" aria-pressed="false">Sterling Silver</button>
        <button class="option-choice" id="unavailable-option" aria-pressed="false" disabled>Unavailable exact option</button>
      </div></div>
      <div class="product-quantity"><label for="piece-quantity">Quantity</label><input id="piece-quantity" type="number" min="1" value="1"></div>
      <section class="product-review"><h2>Review your exact choice</h2><p>Checked piece and selected variant</p><button type="button" class="primary">Confirm add to test bag</button><button type="button" class="secondary">Cancel</button></section>
    </div></div></div>
    <div class="bag-item"><div><h3>Checked piece</h3></div><div class="bag-quantity"><label for="bag-quantity">Quantity</label><input id="bag-quantity" type="number" min="1" value="1"></div></div>
    <section class="checkout-page"><div class="checkout-steps"><button type="button" aria-current="step">1 · Your pieces</button><button type="button">2 · Shipping & gifting</button><button type="button">3 · Review</button></div>
      <section class="checkout-step checkout-box" data-checkout-step="review"><h2>Review</h2><p>Simulation only</p></section>
      <section class="checkout-step checkout-box" data-checkout-step="shipping" hidden><h2>Shipping & gifting</h2></section>
      <div class="demo-shipping"><button type="button" aria-pressed="true">Demo standard</button><button type="button" aria-pressed="false">Demo express</button></div>
      <label class="checkout-check"><input type="checkbox">I understand this is only a simulation.</label>
    </section>
    <div class="service-form">
      <label id="gift-wrapping-label"><input id="gift-wrapping-choice" type="checkbox">Ask for gift wrapping</label>
      <label id="gift-package-label"><input id="gift-package-choice" type="checkbox">Ask about a gift package</label>
      <label id="gift-note-label">Your test gift note<textarea></textarea></label>
    </div>
    <div class="collection-tools"><div class="search-wrap"><input type="search"><button type="button">Search</button></div><label class="tool-select tool-sort">Sort<select><option>Shop order</option></select></label></div>
    <div class="catalogue-coverage"></div>
    <div class="product-price store-highlight">$54</div>`;
  d.querySelector('main').append(components);
  t.after(()=>w.close());
  return {d,w,errors,style:element=>w.getComputedStyle(typeof element==='string'?d.querySelector(element):element)};
}

test('boutique template retains functional landmarks/actions and one lazy guide entry',t=>{
  const h=fixture(t);
  for(const id of ['shop-content','collection-controls','result-summary','demo-products','collection-more','storefront-status','bag-count'])assert.equal(h.d.querySelectorAll('#'+id).length,1,id+' remains unique');
  assert.equal(h.d.querySelector('.skip-link').getAttribute('href'),'#shop-content');
  assert.equal(h.d.body.dataset.catalogueSeed,'balanced','only this isolated boutique opts in to the validated expanded seed');
  assert.equal(h.d.querySelectorAll('script[src="/brites-concierge.js"]').length,1);
  assert.equal(h.d.querySelectorAll('script[src*="avatar"]').length,0,'avatar stays on demand');
  assert.deepEqual([...h.d.querySelectorAll('.shop-head nav button')].map(b=>b.dataset.storeAction),['collection','gifts','customize']);
  assert.equal(h.d.querySelector('#result-summary').getAttribute('aria-live'),'polite');
  assert.deepEqual(h.errors,[],'actual stylesheet parses cleanly');
});

test('closed option menus and inactive checkout stages remain absent from presentation',t=>{
  const h=fixture(t),menu=h.d.querySelector('.option-menu');menu.hidden=true;
  assert.equal(h.style(menu).display,'none');assert.equal(h.style('[data-checkout-step="shipping"]').display,'none');
  menu.hidden=false;assert.notEqual(h.style(menu).display,'none');
  assert.equal(h.style(menu).overflow,'auto');assert.equal(h.style(menu).getPropertyValue('overscroll-behavior'),'contain');
  assert.notEqual(h.style(menu).maxHeight,'none','long exact option menus remain bounded and scrollable');
});

test('exact selected and unavailable options have distinct visible states without absolute placement',t=>{
  const h=fixture(t),selected=h.style('#selected-option'),plain=h.style('#other-option'),disabled=h.style('#unavailable-option');
  assert.notEqual(selected.backgroundColor,plain.backgroundColor);
  assert.notEqual(selected.borderTopColor,plain.borderTopColor);
  assert.equal(disabled.cursor,'not-allowed');assert.equal(disabled.opacity,'0.65');
  assert.equal(selected.minHeight,'44px');assert.equal(selected.overflowWrap,'anywhere');
  assert.ok(!['fixed','absolute'].includes(h.style('.option-menu').position));
});

test('quantity, review and shipping controls meet the shared touch-target baseline',t=>{
  const h=fixture(t);
  for(const target of ['.option-menu-trigger','.product-quantity input','.bag-quantity input','.product-review .primary','.product-review .secondary','.demo-shipping button','.checkout-check'])assert.ok(parseFloat(h.style(target).minHeight)>=44,target);
  assert.notEqual(h.style('.demo-shipping button[aria-pressed=true]').backgroundColor,h.style('.demo-shipping button[aria-pressed=false]').backgroundColor);
  assert.ok(!['hidden','clip'].includes(h.style('.product-review').overflow),'review does not gain a viewport clip');
});

test('checkout navigation identifies its current stage independently of content sections',t=>{
  const h=fixture(t);
  assert.equal(h.style('.checkout-steps').gridTemplateColumns,'repeat(3,minmax(0,1fr))');
  assert.notEqual(h.style('.checkout-steps>button[aria-current]').backgroundColor,h.style('.checkout-steps>button:not([aria-current])').backgroundColor);
  assert.ok(parseFloat(h.style('.checkout-steps>button').minHeight)>=44);
  assert.notEqual(h.style('[data-checkout-step="review"]').counterIncrement,'checkout','content sections do not invent numbered steps');
});

test('gift checkbox labels provide a 44-pixel target in flow without changing note labels or native semantics',t=>{
  for(const width of [390,760,1363]){
    const h=fixture(t,{width,docked:width===1363});
    for(const id of ['gift-wrapping-label','gift-package-label']){
      const label=h.d.querySelector('#'+id),input=label.querySelector('input'),style=h.style(label);
      assert.ok(parseFloat(style.minHeight)>=44,width+' '+id+' retains a full clickable label target');
      assert.equal(style.display,'flex');assert.equal(style.alignItems,'center');
      assert.ok(!['fixed','absolute'].includes(style.position),'gift choices remain in normal service flow');
      assert.equal(input.type,'checkbox');assert.equal(input.labels.length,1);assert.equal(input.labels[0],label);
      assert.equal(h.style(input).margin,'0px','native checkbox does not consume the enlarged label gap');
      assert.equal(h.style(input).flex,'0 0 auto','native checkbox remains visible when copy wraps');
    }
    assert.equal(h.style('#gift-note-label').display,'block','text-note label keeps its existing field layout');
    assert.equal(h.d.querySelectorAll('#gift-wrapping-choice,#gift-package-choice').length,2,'both literal preferences remain separately selectable');
  }
});

for(const width of [390,760])test('mobile '+width+' keeps all shop actions visible and avoids zoom-prone editing text',t=>{
  const h=fixture(t,{width});
  for(const button of h.d.querySelectorAll('.shop-head nav button'))assert.notEqual(h.style(button).display,'none',button.textContent);
  assert.equal(h.style('.shop-head nav').order,'3');
  for(const selector of ['.product-quantity input','.bag-quantity input'])assert.equal(h.style(selector).fontSize,'16px');
  assert.equal(h.style('.bag-item').gridTemplateColumns,'minmax(0,1fr)');
  if(width<470){assert.equal(h.style('.collection-tools').gridTemplateColumns,'minmax(0,1fr)');assert.equal(h.style('.search-wrap input').fontSize,'16px');assert.equal(h.style('.product-review .primary').width,'100%');}
});

test('desktop docked guide preserves its existing reserved lane',t=>{
  const h=fixture(t,{width:1363,docked:true});
  assert.equal(h.style(h.d.body).paddingRight,'392px');
  assert.ok(!['fixed','absolute'].includes(h.style('.product-layout').position),'shared choice styles do not pin the product over the guide');
});

test('coverage never implies completion from empty content and action pulses respect reduced motion',t=>{
  const h=fixture(t,{reduced:true});
  assert.equal(h.style('.catalogue-coverage').display,'none');
  // JSDOM's computed cascade does not reliably prioritize universal !important
  // against class animation declarations. Verify the actual qualified CSSOM
  // priority rather than reporting synthetic computed animation as a render.
  const quiet=[...h.d.styleSheets[0].cssRules].find(r=>r.selectorText?.replace(/\s/g,'')==='*,*::before,*::after'&&r.style.getPropertyValue('animation')==='none');
  assert.ok(quiet,'the matched quiet stylesheet covers elements and decorations');
  assert.equal(quiet.style.getPropertyPriority('animation'),'important');
  assert.equal(quiet.style.getPropertyPriority('transition'),'important');
  assert.equal(quiet.style.getPropertyPriority('scroll-behavior'),'important');
  for(const selector of ['.option-menu','#selected-option','.store-highlight'])assert.doesNotMatch(h.style(selector).animation,/infinite/,'choice/action feedback must not become perpetual');
  assert.equal(h.style('.store-highlight').getPropertyValue('scroll-margin-top'),'24px');
});
