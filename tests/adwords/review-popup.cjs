const {JSDOM}=require('jsdom'),fs=require('node:fs'),assert=require('node:assert/strict');
(async()=>{const dom=new JSDOM('<body></body>',{runScripts:'outside-only',url:'https://example.test'}),w=dom.window;w.HTMLDialogElement.prototype.showModal=function(){this.open=true};w.eval(fs.readFileSync('brites-ad-editor.js','utf8'));
const q={rubric:'complete-ad-v2',pass:true,score:99,scores:{messaging:99,layout:100,relevance:100,visualAppeal:100,productRecognition:100},categoryReviews:Object.fromEntries(['messaging','layout','relevance','visualAppeal','productRecognition'].map(k=>[k,{summary:'Readable '+k,deductions:k==='messaging'?[{kind:'optional',points:1,reason:'Copy length',evidence:'Square headline',correction:'Shorten headline <script>bad</script>'}]:[]}]))};let calls=0;
const panel=await w.BritesAdEditor.openReview({scope:{workspaceId:'w',productId:'p'},staticState:{jobId:'s'},animatedState:{jobId:'a'},request:async a=>{assert.equal(a,'adEvaluationStatus');calls++;return {phase:'complete',quality:q};}});assert.equal(calls,2);assert(panel.textContent.includes('Readable messaging'));assert(panel.textContent.includes('No corrections identified.'));assert(panel.textContent.includes('−1 points'));assert.equal(panel.querySelectorAll('script').length,0);assert(!panel.textContent.includes('Re-evaluate only'));dom.window.close();console.log('PASS unified popup summaries, exact deductions, escaping and no evaluation button');})().catch(e=>{console.error(e);process.exit(1)});
(async()=>{const dom=new JSDOM('<body></body>',{runScripts:'outside-only',url:'https://example.test'}),w=dom.window;w.HTMLDialogElement.prototype.showModal=function(){this.open=true};w.HTMLDialogElement.prototype.close=function(){this.open=false};w.eval(fs.readFileSync('brites-ad-editor.js','utf8'));
const cats=['messaging','layout','relevance','visualAppeal','productRecognition'],quality=score=>({rubric:'complete-ad-v2',pass:false,score,scores:{messaging:90,layout:score-5,relevance:100,visualAppeal:100,productRecognition:100},categoryReviews:Object.fromEntries(cats.map(k=>[k,{summary:'Summary '+k,deductions:k==='layout'?[{kind:'required',points:100-(score-5),reason:'Caption too small',evidence:'mobile_square at 3.5s',correction:'Enlarge the caption',formats:['mobile_square']}]:k==='messaging'?[{kind:'required',points:10,reason:'Generic hook',evidence:'active headline',correction:'Rewrite the hook',formats:['active']}]:[]}]))});
const calls=[];let animatedFixed=false,staticFixed=false;
const request=async(a,input)=>{calls.push({a,input});
 if(a==='adEvaluationStatus')return {phase:'idle'};
 if(a==='adDesignMotionStatus')return {ok:true,workspaceId:'w',jobId:animatedFixed?'a2':'a',phase:'ready',quality:quality(animatedFixed?94:85),repairReviewHash:'h',fixOptions:[{kind:'caption',category:'layout',index:0,formats:['mobile_square'],estimatedUsd:0,label:'Fix · re-compose mobile_square captions (no new video)'}],...(animatedFixed?{fixTarget:{kind:'caption',label:'captions'}}:{})};
 if(a==='startAdDesignMotion'){assert.equal(input.fixOf,'a');assert.equal(input.repairReviewHash,'h');assert.equal(JSON.stringify(input.fix),JSON.stringify({category:'layout',index:0}));animatedFixed=true;return {ok:true,jobId:'a2',queued:true};}
 if(a==='adDesignEditorAIStatus')return {ok:true,workspaceId:'w',jobId:staticFixed?'s2':'s',phase:'ready',quality:quality(staticFixed?93:88),fixOptions:[{kind:'plan',category:'messaging',index:0,formats:['active'],estimatedUsd:0,label:'Fix · revise the copy/layout plan for this defect (no new images)'}]};
 if(a==='fixAdDesignEditorAI'){assert.equal(input.jobId,'s');assert.equal(JSON.stringify(input.fix),JSON.stringify({category:'messaging',index:0}));staticFixed=true;return {ok:true,jobId:'s2',queued:true};}
 if(a==='applyAdDesignEditorScene'){assert.equal(input.jobId,'s2');return {ok:true};}
 throw Error('unexpected '+a);};
const panel=await w.BritesAdEditor.openReview({scope:{workspaceId:'w',productId:'p',groupRef:'g'},request});
const animatedButton=panel.querySelector('[data-animated] [data-fix]'),staticButton=panel.querySelector('[data-static] [data-fix]');
assert(animatedButton&&animatedButton.dataset.fix==='layout:0'&&animatedButton.textContent==='Fix this layout','each classified deduction gets its own fix button');
assert.equal(panel.querySelectorAll('[data-animated] [data-fix]').length,1,'deductions without a fix option have no button');
assert(panel.querySelector('[data-animated]').textContent.includes('(mobile_square)'),'the named format is visible beside the finding');
animatedButton.click();await new Promise(r=>setTimeout(r,20));
assert(calls.some(c=>c.a==='startAdDesignMotion'),'fix button starts exactly the targeted animated fix');
assert(panel.querySelector('[data-animated]').textContent.includes('85 → 94/100'),'the corrected animated set is re-scored in place');
assert(panel.querySelector('[data-animated]').textContent.includes('Targeted fix applied'),'the section identifies the fix that produced the new set');
assert(!calls.some(c=>c.a==='startAdDesignMotion'&&c.input.rerunOf),'no full re-run is requested');
staticButton.click();await new Promise(r=>setTimeout(r,20));
assert(calls.some(c=>c.a==='fixAdDesignEditorAI')&&calls.some(c=>c.a==='applyAdDesignEditorScene'),'a static fix that scores higher is applied automatically');
assert(panel.querySelector('[data-static]').textContent.includes('88 → 93/100'),'the corrected static set is re-scored in place');
dom.window.close();console.log('PASS per-finding fix buttons start bounded fixes and re-score the corrected set');})().catch(e=>{console.error(e);process.exit(1)});
