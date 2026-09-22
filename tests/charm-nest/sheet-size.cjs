const assert=require('node:assert/strict'),fs=require('node:fs'),{JSDOM}=require('jsdom');
const Ops=require('../../charm-nest-operations'),O=require('../../charm-nest-orders');
const dom=new JSDOM('<div id="gate"></div>',{url:'https://example.test',runScripts:'outside-only'}),w=dom.window;
const target={metal:'gold14k',runId:'r',sheetId:'gold',status:'complete',charms:[{pinned:{x:1},arrivalPin:true}],persistedDone:true};
const other={metal:'silver',runId:'r',status:'nesting',charms:[{pinned:{x:2}}],placements:[{id:'untouched'}]};
const archived={...target,recalled:{stock:{wPt:90,hPt:90}},charms:[{pinned:{x:3}}]};
const pages=[target,other,archived],dirty=[],messages=[],settings={stock:{gold14k:[1,1]}};
w.CharmNestOrders=O;w.CharmNestOperations=Ops.create();w.pages=pages;w.target=target;
w.CN={S:{settings},stockFor:()=>({wIn:settings.stock.gold14k[0],hIn:settings.stock.gold14k[1]})};
w.markDirty=p=>dirty.push(p);w.notify=m=>messages.push(m);
w.eval(`var CN=window.CN,S=CN.S,B=window.B={run:{runId:'r',status:'running',releasePolicy:2,solidIncluded:{}}};
var allSheets=()=>window.pages,pagesOf=m=>window.pages.filter(p=>p.metal===m),stockFor=CN.stockFor;
var Sets=window.Sets={ofRun:()=>[]},RunCtl={optionsChanged(){}},Session={schedule(){}},refreshAllCards=()=>{};
var labelOf=m=>m,esc=s=>s,toast=window.notify,sheetDirty=window.markDirty,saveSettings=()=>{};`);
const source=fs.readFileSync('charm-nest-bridge.js','utf8'),start=source.indexOf('const Gate =');
w.eval(source.slice(start,source.indexOf('/* ═══ 21',start)));
const gate=w.document.getElementById('gate'),render=()=>w.Gate.renderRelease(target,gate),button=()=>gate.querySelector('[data-solid="size"]');
const dimensions=(width,height)=>{for(const [axis,value] of [['w',width],['h',height]]){const input=gate.querySelector('[data-solid="'+axis+'"]');input.value=value;input.dispatchEvent(new w.Event('input'));}};
(async()=>{
 render();assert(!button().disabled,'another material nesting must not block Apply');dimensions(31,32);
 await button().onclick();assert.equal(Math.round(settings.stock.gold14k[0]*25.4),31);assert.equal(Math.round(settings.stock.gold14k[1]*25.4),32);
 assert.deepEqual(dirty,[target],'only editable pages of the resized material are invalidated');assert.equal(target.charms[0].pinned,null);assert(!target.charms[0].arrivalPin);
 assert.deepEqual(other.placements,[{id:'untouched'}]);assert.deepEqual(other.charms[0].pinned,{x:2});assert.deepEqual(archived.charms[0].pinned,{x:3});
 // Accept a click during a conflicting cloud write, then apply automatically.
 let release;const save=w.CharmNestOperations.run({key:'save-other-sheet',resources:['production:r']},()=>new Promise(r=>release=r));
 await Promise.resolve();await Promise.resolve();w.B.run.arrivalBusy=true;render();assert(!button().disabled);dimensions(33,34);
 const applying=button().onclick();assert.equal(button().getAttribute('aria-busy'),'true');assert.equal(Math.round(settings.stock.gold14k[0]*25.4),31);
 // Editing a newer draft while saving cannot be overwritten by the older request.
 dimensions(35,36);release();await Promise.all([save,applying]);assert.equal(Math.round(settings.stock.gold14k[0]*25.4),33);assert.equal(gate.querySelector('[data-solid="w"]').value,'35');assert(!button().disabled);
 w.B.run.arrivalBusy=false;
 // Same-material operations still protect the dimensions consumed by their job.
 for(const status of ['nesting','finishing','queued']){target.status=status;render();assert(button().disabled,status);}
 target.status='complete';target.persisted=Promise.resolve();target.persistedDone=false;render();assert(button().disabled);target.persistedDone=true;
 let unblock;const pending=w.CharmNestOperations.run({key:'record-write',resources:['production:r']},()=>new Promise(r=>unblock=r));
 await Promise.resolve();await Promise.resolve();render();const raced=button().onclick();target.status='nesting';unblock();await Promise.all([pending,raced]);
 assert.equal(Math.round(settings.stock.gold14k[0]*25.4),33,'recheck protects a job started while the size save was queued');assert.match(messages.at(-1),/Size not applied/);assert.equal(gate.querySelector('[data-solid="w"]').value,'35');
 target.status='complete';w.Sets.ofRun=()=>[{committedAt:1}];render();assert(button().disabled,'committed set remains locked');w.Sets.ofRun=()=>[];
 let finishCommit;const commit=w.CharmNestOperations.run({key:'commit:r',resources:['production:r']},()=>new Promise(r=>finishCommit=r));await Promise.resolve();await Promise.resolve();render();assert(button().disabled,'in-flight commit remains locked');finishCommit();await commit;
 target.recalled={};render();assert(button().disabled,'saved sheet remains locked');
 console.log('Sheet size OK: independent materials, queued cloud saves, draft retention, same-material race checks and immutable history');
})().then(()=>w.close(),e=>{console.error(e);w.close();process.exitCode=1;});
