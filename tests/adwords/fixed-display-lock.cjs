// Fixed Display ships finished artwork with its text baked in, so a card whose messaging no longer matches that text locks it.
// The lock must be visible on the Fixed Display tile itself, one click must undo it, and stray whitespace or extra keys must never cause it.
// Fake api only: no network, no paid calls.
const fs=require('node:fs'),vm=require('node:vm'),assert=require('node:assert/strict'),{JSDOM}=require('jsdom');
const html=fs.readFileSync('brites-adwords.html','utf8'),source=html.slice(html.indexOf('function campaignStyleChoices('),html.indexOf('function adApprovalDesignReviewHtml(')),component=fs.readFileSync('brites-approval-review.js','utf8');
const copy={headlines:['Saved Charm','A Silver Saved Charm','Jewellery Gifts'],longHeadlines:['Discover the saved silver charm'],descriptions:['Shop the charm at Brites Jewelry.','Find a charm for your collection.']};
const hex=n=>String(n).repeat(32).slice(0,32);
function approval(n,slug,title){return {id:'design-review-'+hex(n),reviewHash:'h-'+slug,summary:'Complete ad · '+title,designReview:{workspaceId:'w-'+slug,productId:slug,groupRef:'g-'+slug,productTitle:title,destination:'https://britesjewelry.com/products/'+slug,copy,context:{countries:['2840'],dailyBudget:12}}};}
function lanes(slug){const img=(format,w,h)=>({kind:'responsive',format,label:slug+' '+format,width:w,height:h,url:'https://storage.googleapis.com/brites/'+slug+'-'+format+'.jpg'}),set=[img('landscape',1200,628),img('square',1200,1200),img('portrait',1638,2048)],fixed={kind:'fixed',label:'300 × 250',width:300,height:250,url:'https://storage.googleapis.com/brites/'+slug+'-300x250.jpg'},film={included:true,key:'mobile_portrait',asset:{width:1080,height:1920},url:'https://storage.googleapis.com/brites/'+slug+'.mp4',inclusion:'Attaches after campaign creation'};
 return {ok:true,videoReview:{advisory:true},styles:{pmax:{images:set,videos:[film],savedVideos:[film],copy,videoNote:'Films attach after creation.'},responsive_display:{images:set.slice(0,2),videos:[],savedVideos:[{...film,included:false,inclusion:'Not on YouTube · not included'}],copy,videoNote:'Not on YouTube.'},fixed_display:{images:[fixed],videos:[],copy:null,videoNote:'Finished artwork only.'}}};}
const duck=approval(1,'duck','Duck Silhouette'),gecko=approval(2,'gecko','Gecko'),bunny=approval(3,'bunny','Bunny'),saturn=approval(4,'saturn','Planet Saturn');
const broken=approval(5,'moon','Moon'),withErrorObject=approval(6,'star','Star'),viaReload=approval(7,'sun','Sun'),viaPlan=approval(8,'leaf','Leaf');
const serverRefusal='HTTP 500 · This review belongs to a different product or ad group.';
let calls=[],gate=null,inFlight=0,maxInFlight=0;const failing=new Set([broken.id,withErrorObject.id,viaReload.id,viaPlan.id]),planFailing=new Set([broken.id,viaPlan.id]);
const dom=new JSDOM('<body></body>',{url:'https://brites.example'}),d=dom.window.document;dom.window.HTMLElement.prototype.scrollIntoView=function(){};
const c={document:d,localStorage:dom.window.localStorage,URL,console,setTimeout,clearTimeout,Intl,BritesCampaignStyles:require('../../brites-campaign-styles'),DASH:{pending:[],budgetCurrency:'CAD',pmaxTargets:[]},esc:s=>String(s),adAttr:s=>String(s),elFrom:s=>{const div=d.createElement('div');div.innerHTML=s;return div.firstElementChild;},adApprovalDesignReviewHtml:()=>'',wireApprovalAllSizes:()=>{},campaignStyleIcon:()=>'',apCountries:()=>'United States',apMoney:n=>'CAD '+Number(n).toFixed(2),toast:()=>{},reload:async()=>{},openAdDesign:()=>{},
 btnBusy:b=>{const was=b.innerHTML;b.disabled=true;b.textContent='Reloading…';return()=>{b.disabled=false;b.innerHTML=was;};},
 api:async(action,input)=>{calls.push({action,input});
  if(action==='adDesignSubmissionStatus'){inFlight++;maxInFlight=Math.max(maxInFlight,inFlight);try{if(gate)await gate.promise;const id=input.id;if(failing.has(id)){if(id===withErrorObject.id)return {error:'This review belongs to a different product or ad group.'};throw Error(serverRefusal);}return lanes(id===duck.id?'duck':id===gecko.id?'gecko':id===bunny.id?'bunny':id===saturn.id?'saturn':'other');}finally{inFlight--;}}
  if(action==='publishAdDesignSubmission'&&input.prepareOnly){if(planFailing.has(input.id))throw Error('This review belongs to a different product or ad group.');return {ok:true,planHash:'plan-'+input.id,plan:{currency:'CAD',totalDaily:12,campaigns:[{style:'pmax',name:'Saved Campaign',dailyBudget:12,formats:['square','landscape','portrait']}]}};}
  throw Error('Unexpected '+action+' in preview verification');}};
vm.createContext(c);vm.runInContext(component,c);vm.runInContext(source,c);vm.runInContext(html.split('\n').find(line=>line.startsWith('function openApprovalDesign(')),c);
const tick=()=>new Promise(r=>setImmediate(r)),settle=async()=>{for(let i=0;i<8;i++)await tick();};
function mount(list){const out=list.map(a=>{const card=c.adDesignSubmissionCard(a);d.body.appendChild(card);c.wireAdDesignSubmission(card,a);return card;});return out;}
const statusCalls=id=>calls.filter(x=>x.action==='adDesignSubmissionStatus'&&(!id||x.input.id===id)).length;
const loadingText=/Loading ad preview|Checking saved videos|Loading previews|Loading complete ad previews|Loading saved videos|Loading the included creative set/;
const artwork={headlines:['Bunny Necklace','Gold Bunny Pendant','Dainty Bunny Charm'],longHeadlines:['Handmade bunny pendant necklace in solid gold'],descriptions:['Shop the bunny pendant at Brites Jewelry.','A thoughtful gift, made to order.']};
const SR=require('../../netlify/functions/googleAdsSubmissionReview.js'),hash=v=>require('node:crypto').createHash('sha256').update(JSON.stringify(v)).digest('hex');
function review(n,slug,extra){const a=approval(n,slug,slug[0].toUpperCase()+slug.slice(1));a.designReview={...a.designReview,artworkCopy:artwork,...extra};return a;}
let saves=[];
c.api=async(action,input)=>{calls.push({action,input});
 if(action==='adDesignSubmissionStatus')return lanes('bunny');
 if(action==='publishAdDesignSubmission'&&input.prepareOnly)return {ok:true,planHash:'plan-'+input.id,plan:{currency:'CAD',totalDaily:12,campaigns:[{style:'pmax',name:'Saved Campaign',dailyBudget:12,formats:['square','landscape','portrait']}]}};
 if(action==='updateAdDesignSubmission'){saves.push(input);const copy=Object.fromEntries(['headlines','longHeadlines','descriptions'].map(k=>[k,(input.copy[k]||[]).map(v=>String(v).trim())]));
  const designReview={...bunnyReview.designReview,copy,includeVideos:input.includeVideos,copyEdited:!SR.sameCopy(copy,artwork,hash)};return {ok:true,reviewHash:'h-saved-'+saves.length,designReview};}
 throw Error('Unexpected '+action);};
// The server's own comparison: text lists only.
assert(SR.sameCopy(artwork,{...artwork,callToAction:'Shop now'},hash),'extra keys do not make messaging "edited"');
assert(SR.sameCopy(artwork,{headlines:artwork.headlines.map(h=>'  '+h+' '),longHeadlines:artwork.longHeadlines.concat(['']),descriptions:artwork.descriptions},hash),'stray whitespace and empty rows do not make messaging "edited"');
assert(!SR.sameCopy(artwork,{...artwork,headlines:['Bunny Necklace','Gold Bunny Pendant','Another one']},hash),'a changed headline is an edit');
assert(!SR.sameCopy(artwork,{...artwork,descriptions:artwork.descriptions.slice(0,1)},hash),'a removed description is an edit');
const stale={headlines:['Bunny Pendant','Gold Bunny','Bunny Gift'],longHeadlines:['Bunny pendant necklace'],descriptions:['Shop now.','Made to order.']};
const bunnyReview=review(3,'bunny',{copy:stale,copyEdited:true});
(async()=>{
 // 1. A locked card says so on the Fixed Display tile itself, not only above in the messaging editor.
 let [card]=mount([bunnyReview]);await settle();
 const fixed=card.querySelector('[data-campaign-style="fixed_display"]'),tile=card.querySelector('[data-style-fixed-lock]'),note=card.querySelector('[data-review-fixed-warning]');
 assert(fixed.disabled&&!fixed.checked,'Fixed Display is locked while the messaging differs from the artwork');
 assert(!tile.hidden&&/Locked: the messaging no longer matches/.test(tile.textContent),'the tile says why it is locked');assert(tile.querySelector('[data-style-fixed-match]'),'the tile offers the one-click fix');
 assert(!note.hidden&&note.querySelector('[data-review-match-artwork]'),'the messaging editor offers the same fix');
 assert.match(note.textContent,/Edit artwork/,'the artwork route stays on offer');
 // 2. One click on the tile puts the artwork's messaging back, saves it, and unlocks Fixed Display.
 tile.querySelector('[data-style-fixed-match]').click();await settle();
 assert.equal(saves.length,1,'one save');assert.equal(JSON.stringify(saves[0].copy),JSON.stringify(artwork),'the saved messaging is exactly the artwork’s');assert.equal(saves[0].includeVideos,true,'the video choice is kept');
 assert(!fixed.disabled,'Fixed Display unlocks after matching the artwork');assert(tile.hidden&&note.hidden,'both lock notes disappear');
 assert.equal(card.querySelectorAll('[data-review-copy="headlines"]').length,3,'the editor shows the artwork’s headlines');
 assert.equal(card.querySelector('[data-review-save-state]').textContent,'✓ Ad changes saved');
 // 3. It can be chosen and prepares a plan including it.
 fixed.checked=true;fixed.onchange&&fixed.onchange();fixed.dispatchEvent(new dom.window.Event('change',{bubbles:true}));
 assert(fixed.checked&&!card.querySelector('[data-style-budget="fixed_display"]').disabled,'Fixed Display can now be selected and takes a budget');
 // 4. Editing the messaging again re-locks it, with the reason, and the same button undoes that.
 const first=card.querySelector('[data-review-copy="headlines"]');first.value='A different first headline';first.dispatchEvent(new dom.window.Event('input',{bubbles:true}));
 card.querySelector('[data-review-save]').click();await settle();
 assert(fixed.disabled&&!fixed.checked&&!tile.hidden,'an edit locks Fixed Display again and shows why');
 card.querySelector('[data-review-match-artwork]').click();await settle();assert(!fixed.disabled&&tile.hidden,'the editor button unlocks it just as well');
 // 5. A review whose messaging already matches is never locked.
 d.body.replaceChildren();[card]=mount([review(4,'saturn',{copy:{...artwork,headlines:artwork.headlines.map(h=>h+' ')},copyEdited:false})]);await settle();
 assert(!card.querySelector('[data-campaign-style="fixed_display"]').disabled&&card.querySelector('[data-style-fixed-lock]').hidden,'a matching review leaves Fixed Display selectable');
 // 6. No saved artwork text to go back to: still locked, still explained, and no button that could not work.
 d.body.replaceChildren();[card]=mount([review(5,'gecko',{artworkCopy:null,copyEdited:true})]);await settle();
 assert(card.querySelector('[data-campaign-style="fixed_display"]').disabled&&!card.querySelector('[data-review-fixed-warning]').hidden,'locked and explained');
 assert(!card.querySelector('[data-review-match-artwork]'),'the editor offers no button without artwork text');
 console.log('PASS Fixed Display lock is explained on its tile, one click unlocks it, an edit re-locks it, and whitespace or extra keys never lock it');
 require('./suite-guard.cjs').done();
})();
