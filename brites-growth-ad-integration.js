(function(){'use strict';
  if(document.querySelector('#v-research'))return;
  const nativeBuild=window.buildNav,main=document.querySelector('#v-command')?.parentElement;
  if(!main)return;
  const section=document.createElement('section');section.className='view hidden growth-embed';section.id='v-research';
  const root=document.createElement('div');root.id='growth-ad-root';const shadow=root.attachShadow({mode:'open'});
  const css=document.createElement('link');css.rel='stylesheet';css.href='/brites-growth.css';shadow.appendChild(css);
  const local=document.createElement('style');local.textContent=':host{display:block;font:14px/1.5 system-ui;color:#213942}.workspace{padding:0;max-width:none}h1{font-size:26px}.layout{grid-template-columns:minmax(260px,350px) minmax(0,1fr)}@media(max-width:850px){.layout{grid-template-columns:1fr}}';shadow.appendChild(local);
  const content=document.createElement('div');content.id='growth-workspace';shadow.appendChild(content);section.appendChild(root);main.appendChild(section);
  let mounted=null;
  function attach(){
    const nav=document.querySelector('#nav');if(!nav||nav.querySelector('[data-v="research"]'))return;
    const button=document.createElement('button');button.type='button';button.dataset.v='research';button.textContent='✧  Product research';
    button.onclick=async function(){
      document.querySelectorAll('.view').forEach(v=>v.classList.add('hidden'));section.classList.remove('hidden');
      document.querySelectorAll('#nav button').forEach(b=>b.classList.toggle('active',b===button));
      document.querySelector('#vtitle').textContent='Product research';document.querySelector('#vsub').textContent=window.__BRITES_GROWTH_AD_SANDBOX?'Sandbox · saved research and live Ads reports':'Shared evidence for ads, buyer searches and the gift concierge';window.navDrawer?.(false);
      const request=async function(op){
        if(op==='status')return window.api('growthResearchStatus');
        if(op.startsWith('research?')){const ids=new URLSearchParams(op.slice(op.indexOf('?')+1)).get('ids');if(!ids)throw Error('Choose an exact product.');return window.api('growthResearchDossiers',{productIds:ids.split(',')});}
        if(op.startsWith('demand?')){const ids=new URLSearchParams(op.slice(op.indexOf('?')+1)).get('ids');if(!ids)throw Error('Choose an exact product.');return window.api('growthProductDemand',{productIds:ids.split(',')});}
        throw Error('This view reads saved product research.');
      };
      const correctionRequest=async(body,{signal}={})=>{const response=await fetch('/api/growth-corrections',{method:'POST',signal,headers:{'Content-Type':'application/json','X-Edit-Passcode':PASS},body:JSON.stringify(body)}),value=await response.json();if(!response.ok)throw Object.assign(Error(value.error||'Correction review is unavailable.'),{status:response.status});return value;};
      if(!mounted)mounted=window.BritesGrowth.mount(content,{request,correctionRequest,receiptReader:()=>window.api('receiptDiagnostics',{limit:15,maxMs:20000}),receiptPreviewReader:window.__BRITES_GROWTH_AD_SANDBOX?()=>window.api('receiptReconciliationPreview',{limit:15,maxMs:20000}):undefined});else(await mounted).refresh();
    };
    nav.appendChild(button);
  }
  if(nativeBuild)window.buildNav=function(){const result=nativeBuild.apply(this,arguments);attach();return result;};attach();
})();
