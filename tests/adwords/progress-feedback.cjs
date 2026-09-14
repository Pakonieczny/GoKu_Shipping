const assert=require('node:assert/strict'),fs=require('node:fs'),{JSDOM}=require('jsdom');
const dom=new JSDOM('<body><section id="work"><progress aria-label="Design" max="100" value="30"></progress></section></body>',{runScripts:'outside-only'}),w=dom.window;let now=10000,tick;
w.Date.now=()=>now;w.setInterval=fn=>{tick=fn;};w.eval(fs.readFileSync('brites-progress.js','utf8'));
tick();const meter=w.document.querySelector('.bp-meter');assert.equal(meter.dataset.label,'30%');
const sample=w.document.querySelector('progress');sample.max=200;sample.value=100;tick();assert.equal(meter.dataset.label,'50%');assert.equal(meter.style.getPropertyValue('--bp-pct'),'50%');sample.value=200;tick();assert.equal(meter.dataset.label,'100%');sample.removeAttribute('value');tick();assert.equal(meter.dataset.label,'Working…');sample.max=100;sample.value=30;
const end=w.BritesProgress.begin('Opening saved work');tick();assert.equal(w.document.querySelector('.bp-activity'),null);
now+=2000;tick();assert.match(w.document.querySelector('.bp-activity').textContent,/Opening saved work/);
const dialog=w.document.createElement('dialog');dialog.setAttribute('open','');w.document.body.append(dialog);tick();assert.ok(dialog.querySelector('.bp-activity'),'activity stays visible in modal top layer');
const end2=w.BritesProgress.begin('Loading ratings');now+=2000;tick();assert.match(dialog.textContent,/2 requests/);end();tick();assert.match(dialog.textContent,/Loading ratings/);
now+=65000;tick();assert.match(dialog.textContent,/completion is not confirmed/);assert.equal(w.document.querySelector('progress').value,30,'elapsed time never invents completion');
const old=w.document.querySelector('.bp-note').textContent;w.document.querySelector('#work').innerHTML='<progress aria-label="Design" max="100" value="30"></progress>';tick();assert.equal(w.document.querySelector('.bp-note').textContent,old,'rerender preserves elapsed and no-progress history');
w.document.querySelector('progress').value=45;tick();assert.doesNotMatch(w.document.querySelector('.bp-note').textContent,/No new progress/);w.document.querySelector('progress').hidden=true;tick();assert.ok(w.document.querySelector('.bp-note').hidden);
const reused=w.document.querySelector('progress');w.BritesProgress.reset(reused);reused.hidden=false;tick();now+=3000;tick();assert.match(reused.nextElementSibling.textContent,/^3s elapsed/,'new task resets the reused progress clock');reused.hidden=true;
const local=w.document.createElement('progress');local.getClientRects=()=>[{width:100,height:8}];dialog.append(local);tick();assert.equal(dialog.querySelector('.bp-activity'),null,'local task progress suppresses duplicate request indicator');local.hidden=true;tick();assert(dialog.querySelector('.bp-activity'),'request feedback returns when the local task indicator is hidden');
end2();tick();assert.equal(w.document.querySelector('.bp-activity'),null);dom.window.close();
const html=fs.readFileSync('brites-adwords.html','utf8');for(const m of html.matchAll(/<script(?:\s[^>]*)?>([\s\S]*?)<\/script>/g)){if(m[1].trim())new (require('node:vm').Script)(m[1]);}
assert.match(html,/finally\{finishWait\(\);\}/);assert.match(fs.readFileSync('scripts/build-public.cjs','utf8'),/brites-progress.css/);
console.log('PASS shared waiting feedback: delayed display, modal visibility, concurrency, long waits, honest progress, rerender persistence, cleanup and page syntax');
