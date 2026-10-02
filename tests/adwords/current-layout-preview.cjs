const assert=require('assert/strict'),fs=require('fs'),{JSDOM}=require('jsdom');
(async()=>{
 const dom=new JSDOM('<body></body>',{runScripts:'outside-only',url:'https://example.test'}),w=dom.window;
 w.HTMLDialogElement.prototype.showModal=function(){this.open=true};w.HTMLDialogElement.prototype.close=function(){this.open=false};
 const observers=[];w.IntersectionObserver=class{constructor(fn){this.fn=fn;this.frames=[];this.stopped=false;observers.push(this);}observe(f){this.frames.push(f);}unobserve(){}disconnect(){this.stopped=true;}show(f){this.fn([{isIntersecting:true,target:f}]);}};
 w.eval(fs.readFileSync('brites-ad-editor.js','utf8').replace('  api.openAllSizes=','  api.TestEditor=Editor;api.openAllSizes='));
 const variants=[{key:'display_468x60',width:468,height:60,device:'desktop'},{key:'display_300x50',width:300,height:50,device:'mobile'}];
 w.BritesAdResponsive={layoutVersion:20,variants,selectImage:()=>({}),document:()=>({objects:[]})};
 let renders=[],resolveRender=null;w.BritesAdEditor.TestEditor.prototype.renderAIProofs=async(candidate,options)=>{assert.equal(options.preview,true);assert.equal(options.keys.length,1);renders.push(options.keys[0]);if(resolveRender)await new Promise(r=>resolveRender=r);return [{key:options.keys[0],width:468,height:60,mimeType:'image/png',dataBase64:'AA=='}];};
 const state=()=>({candidate:{artboard:{key:'square',width:2048,height:2048},device:'mobile',document:{objects:[]},responsive:{layoutVersion:18,plan:{style:{}},images:[],variants}},reviewProofs:[{key:'historical',width:300,height:50,url:'https://example.test/reviewed.jpg'}]});
 let answer;const opened=w.BritesAdEditor.openAllSizes({scope:{},request:()=>new Promise(r=>answer=r)});
 assert(w.document.querySelector('dialog').open,'popup opens before metadata returns');assert.match(w.document.querySelector('[role=status]').textContent,/Loading saved/);answer(state());await opened;
 assert.equal(renders.length,0,'off-screen sizes do no canvas work');assert.equal(w.document.querySelectorAll('figure').length,3);assert.match(w.document.querySelector('[data-proof-description]').textContent,/not a new AI review/);
 let observer=observers.at(-1);observer.show(observer.frames[1]);await new Promise(r=>setTimeout(r,0));assert.deepEqual(renders,['desktop_display_468x60']);
 let img=observer.frames[1].querySelector('img');assert.match(img.src,/^data:image\/png/);assert.equal(img.loading,'lazy');assert.equal(img.crossOrigin,'anonymous');assert.equal(img.parentElement.getAttribute('aria-busy'),'true');img.dispatchEvent(new w.Event('load'));assert(img._loadBadge.hidden);assert.equal(observer.frames[0].querySelector('img')._loadBadge.hidden,false,'another image keeps its own spinner');
 img.dispatchEvent(new w.Event('error'));assert.equal(img._loadBadge.dataset.state,'error');assert(!img._loadBadge.hidden);w.document.querySelector('[data-close]').click();assert(observer.stopped);
 // An exact immutable review loads saved proofs, with no new rendering or charge.
 await w.BritesAdEditor.openAllSizes({scope:{reviewVersion:3},request:async()=>state()});assert.equal(w.document.querySelector('img').src,'https://example.test/reviewed.jpg');assert.equal(renders.length,1);w.document.querySelector('[data-close]').click();
 // Selected packages never borrow a newer job. Render only a requested size.
 const actions=[];w.BritesAdEditor.TestEditor.prototype.renderSavedProofs=async(saved,options)=>{assert.equal(saved.design.id,'selected-package');assert.equal(options.keys.length,1);return [{key:options.keys[0],url:'https://example.test/selected.png',width:2048,height:2048}];};
 const host=w.document.createElement('div');w.document.body.append(host);await w.BritesAdEditor.openAllSizes({host,scope:{savedDesignId:'selected-package'},request:async(action,input)=>{actions.push(action);assert.equal(action,'openAdDesignSavedDesign');assert.equal(input.id,'selected-package');return {design:{id:'selected-package',artboard:{width:2048,height:2048},device:'mobile'}};}});
 assert.deepEqual(actions,['openAdDesignSavedDesign']);assert.equal(w.document.querySelector('dialog'),null);observer=observers.at(-1);observer.show(observer.frames[0]);await new Promise(r=>setTimeout(r,0));assert.equal(host.querySelector('img').src,'https://example.test/selected.png');host.querySelector('[data-close]').click();assert.equal(host.children.length,0);
 // Closing while a canvas is rendering discards late completions and queued work.
 resolveRender=true;await w.BritesAdEditor.openAllSizes({scope:{},request:async()=>state()});observer=observers.at(-1);observer.show(observer.frames[0]);observer.show(observer.frames[1]);const before=renders.length;w.document.querySelector('[data-close]').click();resolveRender();await new Promise(r=>setTimeout(r,0));assert.equal(renders.length,before);assert.equal(w.document.querySelector('dialog'),null);
 dom.window.close();console.log('PASS immediate all-size popup, visible-only single-size rendering, independent spinners, immutable proof replay, exact selected package, and cancelled late rendering');require('./suite-guard.cjs').done();
})().catch(e=>{console.error(e);process.exitCode=1});
