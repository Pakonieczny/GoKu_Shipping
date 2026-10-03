'use strict';
const test = require('node:test'), assert = require('node:assert/strict');
const {JSDOM} = require('jsdom');
const guide = require('../../brites-concierge-guide.js');
const rect = (left, top, width, height) => ({left, top, width, height, right:left+width, bottom:top+height});
function fixture(t, {reduced = false} = {}) {
  const dom = new JSDOM('<div id="panel"><div id="stage" style="color: red"><canvas></canvas></div></div><button id="target">Choose this piece</button>', {pretendToBeVisual:true});
  const win = dom.window, doc = win.document, panel = doc.querySelector('#panel'), stage = doc.querySelector('#stage'), target = doc.querySelector('#target');
  let hidden = false; Object.defineProperty(doc, 'hidden', {get:()=>hidden});
  Object.defineProperty(win, 'innerWidth', {value:1200, configurable:true}); Object.defineProperty(win, 'innerHeight', {value:850, configurable:true});
  const mediaListeners = new Set(); const media = {matches:reduced,addEventListener:(kind,fn)=>mediaListeners.add(fn),removeEventListener:(kind,fn)=>mediaListeners.delete(fn)}; win.matchMedia = ()=>media;
  stage.getBoundingClientRect = ()=>rect(600,120,420,350); target.getBoundingClientRect = ()=>rect(500,450,200,60);
  let clicks = 0, scrolls = 0; target.click = ()=>clicks++; target.scrollIntoView = ()=>scrolls++;
  const floatingEvents=[];const api = guide.create({stage,panel,root:doc.body,window:win,onFloating:value=>floatingEvents.push(value)});
  t.after(()=>{api.destroy();win.close();});
  return {win,doc,panel,stage,target,api,mediaListeners,floatingEvents,counts:()=>({clicks,scrolls}),hide(value){hidden=value;doc.dispatchEvent(new win.Event('visibilitychange'));},motion(value){media.matches=value;for(const fn of mediaListeners)fn({matches:value});}};
}
test('guide requires explicit opt-in and preserves the same canvas and DOM parent', t=>{
  const f=fixture(t), canvas=f.stage.querySelector('canvas'), parent=f.stage.parentNode, original=f.stage.getAttribute('style');
  assert.equal(f.api.guide(f.target),false);assert.equal(f.stage.getAttribute('style'),original);assert.equal(f.doc.querySelector('.brites-guide-placeholder'),null);
  f.api.setEnabled(true);assert.equal(f.api.guide(f.target),true);assert.equal(f.stage.querySelector('canvas'),canvas);assert.equal(f.stage.parentNode,parent);assert.equal(f.stage.dataset.floating,'true');assert.equal(f.stage.style.position,'fixed');assert.equal(f.stage.style.pointerEvents,'none');
  assert.equal(f.doc.querySelectorAll('canvas').length,1);assert.equal(f.doc.querySelectorAll('.brites-guide-placeholder').length,1);assert.equal(f.api.guide(f.target),true);assert.equal(f.doc.querySelectorAll('.brites-guide-placeholder').length,1);assert.deepEqual(f.counts(),{clicks:0,scrolls:0});
  f.api.clear();assert.equal(f.stage.getAttribute('style'),original);assert.equal(f.stage.hasAttribute('data-floating'),false);assert.equal(f.doc.querySelector('.brites-guide-placeholder'),null);assert.equal(f.api.snapshot().floating,false);assert.deepEqual(f.floatingEvents,[true,false]);
});
test('placement is clamped to the viewport and never covers the selected target',()=>{
  for(const target of [rect(500,450,200,60),rect(10,10,50,50),rect(1140,700,40,100),rect(50,790,900,30)]){
    const p=guide.placement(target,{width:1200,height:850});assert.ok(p);assert.ok(p.left>=12&&p.top>=12&&p.left+p.width<=1188&&p.top+p.height<=838);
    assert.ok(p.left+p.width<=target.left-8||p.left>=target.right+8||p.top+p.height<=target.top-8||p.top>=target.bottom+8);
  }
  for(const target of [rect(0,0,1200,850),rect(50,900,20,20),rect(1400,50,20,20),rect(10,10,0,0),{...rect(10,10,30,30),top:NaN}])assert.equal(guide.placement(target,{width:1200,height:850}),null);
  assert.equal(guide.placement(rect(50,50,50,50),{width:200,height:220}),null);
});
test('pause, reduced motion, hidden document and Escape restore the original stage',t=>{
  const f=fixture(t);f.api.setEnabled(true);const begin=()=>assert.equal(f.api.guide(f.target),true);
  begin();f.api.setPaused(true);assert.equal(f.api.snapshot().floating,false);assert.equal(f.api.guide(f.target),false);f.api.setPaused(false);
  begin();f.motion(true);assert.equal(f.api.snapshot().floating,false);assert.equal(f.api.guide(f.target),false);f.motion(false);
  begin();f.hide(true);assert.equal(f.api.snapshot().floating,false);assert.equal(f.api.guide(f.target),false);f.hide(false);
  begin();f.doc.dispatchEvent(new f.win.KeyboardEvent('keydown',{key:'Escape'}));assert.equal(f.api.snapshot().floating,false);
  begin();f.api.setEnabled(false);assert.equal(f.api.snapshot().floating,false);assert.equal(f.api.guide(f.target),false);
});
test('initial reduced motion, unavailable target, hidden panel and detached target never travel',t=>{
  const f=fixture(t,{reduced:true});f.api.setEnabled(true);assert.equal(f.api.guide(f.target),false);f.motion(false);
  f.panel.hidden=true;assert.equal(f.api.guide(f.target),false);f.panel.hidden=false;
  assert.equal(f.api.guide(f.stage.querySelector('canvas')),false);assert.equal(f.api.guide(null),false);f.target.remove();assert.equal(f.api.guide(f.target),false);
  assert.equal(f.stage.style.position,'');assert.equal(f.api.snapshot().floating,false);
});
test('resize and scroll recompute only visible targets; destroy restores styles and removes media listener',async t=>{
  const f=fixture(t);f.api.setEnabled(true);f.api.guide(f.target);const original='color: red';f.target.getBoundingClientRect=()=>rect(800,500,100,40);f.win.dispatchEvent(new f.win.Event('resize'));
  await new Promise(resolve=>f.win.setTimeout(resolve,25));const moved=f.api.snapshot().placement;assert.ok(moved.left>=914||moved.left+moved.width<=792||moved.top+moved.height<=492||moved.top>=554);
  f.target.getBoundingClientRect=()=>rect(100,1000,100,40);f.win.dispatchEvent(new f.win.Event('scroll'));await new Promise(resolve=>f.win.setTimeout(resolve,25));assert.equal(f.api.snapshot().floating,false);
  f.target.getBoundingClientRect=()=>rect(500,450,200,60);f.api.guide(f.target);f.api.destroy();assert.equal(f.stage.getAttribute('style'),original);assert.equal(f.mediaListeners.size,0);assert.equal(f.api.snapshot().destroyed,true);assert.equal(f.api.guide(f.target),false);f.api.destroy();assert.deepEqual(f.counts(),{clicks:0,scrolls:0});
});
test('source contains no control, network, navigation, scroll or voice execution',()=>{
  const source=require('node:fs').readFileSync(require('node:path').join(__dirname,'../../brites-concierge-guide.js'),'utf8');
  assert.doesNotMatch(source,/fetch\(|\.click\(|scrollIntoView|location\.(?:assign|replace|href)|getUserMedia|speechSynthesis|response\.create|new.*canvas/i);
});
