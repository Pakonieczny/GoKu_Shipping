const fs=require('fs'),vm=require('vm'),assert=require('assert/strict'),{JSDOM}=require('jsdom');
(async()=>{
 const code=fs.readFileSync('netlify/functions/googleAdsAutopilot.js','utf8'),a=code.indexOf('const _adDeliveryReads='),b=code.indexOf('async function _adDesignDeliveryFresh',a),docs=new Map();let now=1000000,calls=0,scope={settings:{productId:'peach',groupRef:'group1'},sourceVersion:1};
 const ctx={require,Map,Date:{now:()=>now},_adDesignPublicationContext:async()=>({w:scope}),_adDesignWorkspaceRef:()=>({collection:()=>({doc:key=>({get:async()=>({exists:docs.has(key),data:()=>docs.get(key)}),set:async v=>docs.set(key,v)})})}),_adDesignDeliveryFresh:async()=>{calls++;return{ok:true,checkedAt:now,rows:[]};}};vm.createContext(ctx);vm.runInContext(code.slice(a,b),ctx);
 const input={workspaceId:'one',start:'2026-09-01'};await Promise.all(Array.from({length:8},()=>ctx.adDesignDelivery(input)));assert.equal(calls,1);
 now+=60000;const cached=await ctx.adDesignDelivery(input);assert(cached.cached);assert.equal(cached.checkedAt,1000000);assert.equal(calls,1);
 now+=300001;await ctx.adDesignDelivery(input);assert.equal(calls,2);scope={...scope,settings:{...scope.settings,productId:'other'}};await ctx.adDesignDelivery(input);assert.equal(calls,3);
 const dom=new JSDOM('<div data-ai-feedback></div>',{runScripts:'outside-only'}),w=dom.window;let editor=fs.readFileSync('brites-ad-editor.js','utf8').replace('root.BritesAdEditor=api;','root.BritesAdEditor=api;root.TestEditor=Editor;');w.eval(editor);const e=Object.create(w.TestEditor.prototype);e.q=s=>w.document.querySelector(s);e.status=()=>{};e.aiFeedback=r=>'<p>'+r.progress.label+'</p>';const run={progress:{label:'Planning'}};e.updateAI(run);const node=e.q('[data-ai-feedback]').firstChild;e.updateAI(run);assert.equal(e.q('[data-ai-feedback]').firstChild,node,'unchanged polls preserve sidebar nodes');run.progress.label='Generating';e.updateAI(run);assert.equal(e.q('[data-ai-feedback]').textContent,'Generating');
 assert(!w.TestEditor.prototype.aiFeedback.toString().includes('<progress'));
 assert(editor.includes('Math.min(30000,10000+unchanged*5000)'));assert(editor.includes('document.hidden?60000'));dom.window.close();
 console.log('PASS deduplicated Google reads, cache expiry and scope isolation, stable sidebar nodes and adaptive polling');
})().catch(e=>{console.error(e);process.exitCode=1;});
