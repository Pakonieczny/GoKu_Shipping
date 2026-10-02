'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {JSDOM, VirtualConsole} = require('jsdom');

const widgetSource = fs.readFileSync(path.join(__dirname, '../../brites-concierge.js'), 'utf8');
const tick = () => new Promise(resolve => setImmediate(resolve));
async function settle() {await tick(); await tick();}
const clone = value => JSON.parse(JSON.stringify(value));
const silver = {id:'gid://shopify/ProductVariant/101',numericId:'101',title:'Sterling Silver / 14 Inch / None',price:54,available:true,options:[{name:'Metal',value:'Sterling Silver'}]};
const gold = {id:'gid://shopify/ProductVariant/102',numericId:'102',title:'14k Gold Filled / 14 Inch / None',price:59,available:true,options:[{name:'Metal',value:'14k Gold Filled'}]};
const product = {id:'gid://shopify/Product/1',handle:'bunny-1',url:'https://britesjewelry.com/products/bunny-1',title:'Bunny Necklace',type:'Necklace',currency:'USD',variants:[silver,gold],variantsComplete:true,suggestedVariantId:silver.id,why:'A bunny design for the requested interest.',minPrice:54};
const answer = {reply:'This bunny necklace could be a thoughtful match.',question:'Would you like to compare the metal options?',preferences:{query:'bunny'},products:[product],meanings:[{productId:product.id,text:'A bunny can recall a beloved pet.',context:'A personal association',sources:[{title:'Brites current catalogue',url:product.url}]}]};
const response = (value, status=200) => ({ok:status>=200&&status<300,status,json:async()=>clone(value)});

function harness(t,{saved=null,reply=answer,voice=false}={}) {
  const errors=[],virtualConsole=new VirtualConsole();virtualConsole.on('jsdomError',error=>errors.push(error));
  const dom=new JSDOM('<!doctype html><html lang="en"><body></body></html>',{url:'https://growth-sandbox.example/concierge-sandbox.html',runScripts:'outside-only',pretendToBeVisual:true,virtualConsole});
  const {window}=dom,{document}=window,requests=[],utterances=[];
  const script=document.createElement('script');script.src='https://growth-sandbox.example/brites-concierge.js';script.dataset.sandbox='true';Object.defineProperty(document,'currentScript',{configurable:true,get:()=>script});
  if(saved)window.sessionStorage.setItem('brites-concierge-v1',JSON.stringify(saved));
  window.SpeechSynthesisUtterance=class{constructor(text){this.text=text;}};
  window.speechSynthesis={cancel(){},pause(){},speak(value){utterances.push(value);}};
  window.BritesConciergeAvatar={create(){return {setState(){},setVisible(){},retry(){},setEmotion(){},setPaused(){},triggerGreeting(){},setLevel(){},destroy(){}};}};
  let voiceConfig=null;
  if(voice)window.BritesConciergeVoice={create(config){voiceConfig=config;return {start:async()=>true,stop:async()=>{},interrupt(){},dispose:async()=>{}};}};
  window.fetch=async(raw,init={})=>{const url=new URL(raw,window.location.href),body=init.body?JSON.parse(init.body):null;requests.push({url,body,init});if(url.pathname==='/api/concierge'&&body?.event)return response({});if(url.pathname==='/api/concierge'&&body?.message)return response(reply);if(url.pathname==='/api/growth/product')return response({product});throw Error('Unexpected request '+url.pathname);};
  window.HTMLElement.prototype.scrollIntoView=function(){};
  window.eval(widgetSource);
  const root=document.querySelector('brites-concierge').shadowRoot;
  const button=label=>Array.from(root.querySelectorAll('button')).find(node=>node.textContent.trim()===label||node.getAttribute('aria-label')===label);
  const ask=async text=>{const input=root.querySelector('input[aria-label="Message the gift concierge"]');input.value=text;root.querySelector('form').dispatchEvent(new window.Event('submit',{bubbles:true,cancelable:true}));await settle();};
  t.after(()=>{try{window.BritesConcierge?.close();}catch{}window.close();});
  return {window,root,button,ask,requests,utterances,errors,get voiceConfig(){return voiceConfig;}};
}

test('completed cards, meanings and narration survive a session reload',async t=>{
  const first=harness(t);first.button('Meet your gift guide').click();await first.ask('Something with a bunny');
  const saved=JSON.parse(first.window.sessionStorage.getItem('brites-concierge-v1'));
  assert.equal(saved.products.length,1);assert.equal(saved.meanings.length,1);assert.match(saved.latestReply,/thoughtful match/);
  const restored=harness(t,{saved});
  assert.equal(restored.root.querySelectorAll('.card').length,1);assert.match(restored.root.querySelector('.meaning').textContent,/beloved pet/);
  restored.button('Read aloud').click();assert.equal(restored.utterances.length,1);assert.match(restored.utterances[0].text,/thoughtful match/);
});

test('an interrupted turn is marked, removed from trusted context and not silently resumed',async t=>{
  const pending='I need a meaningful Christmas gift under $80';
  const h=harness(t,{saved:{history:[{role:'user',content:pending}],preferences:{},products:[],meanings:[],policyLinks:[],productHandles:[],uncertainVariants:[],pendingTurn:{message:pending,startedAt:Date.now()},open:true,updatedAt:Date.now()}});
  assert.match(h.root.querySelector('.messages').textContent,/last request didn’t finish/i);
  await h.ask('The necklace charm');
  const request=h.requests.find(entry=>entry.body?.message==='The necklace charm');
  assert.deepEqual(request.body.history,[]);assert.ok(!h.root.querySelector('.messages').textContent.includes(pending));
  const saved=JSON.parse(h.window.sessionStorage.getItem('brites-concierge-v1'));assert.equal(saved.pendingTurn,null);
});

test('changing a variant invalidates the stale review before confirmation',async t=>{
  const h=harness(t);h.button('Meet your gift guide').click();await h.ask('A bunny necklace');
  h.button('Choose options').click();h.button('Review adding to bag').click();
  assert.match(h.root.querySelector('.review').textContent,/Sterling Silver/);
  const select=h.root.querySelector('select');select.value=gold.id;select.dispatchEvent(new h.window.Event('change',{bubbles:true}));
  assert.equal(h.root.querySelector('.review'),null);assert.equal(h.button('Confirm add to bag'),undefined);assert.equal(h.button('Review adding to bag').disabled,false);
  h.button('Review adding to bag').click();assert.match(h.root.querySelector('.review').textContent,/14k Gold Filled/);
  assert.equal(h.window.sessionStorage.getItem('brites-sandbox-cart'),null);
});

test('voice audio transcripts do not duplicate the checked tool-rendered turn',async t=>{
  const h=harness(t,{voice:true});h.button('Meet your gift guide').click();h.button('Talk to me').click();
  const loader=h.root.querySelector('script[src$="brites-concierge-voice.js"]');assert.ok(loader);loader.onload();await settle();assert.ok(h.voiceConfig);
  h.voiceConfig.onTranscript({role:'user',text:'Something with a bunny',final:true});
  await h.voiceConfig.onTool({message:'Something with a bunny'});
  h.voiceConfig.onTranscript({role:'assistant',text:answer.reply,final:true});
  const bubbles=Array.from(h.root.querySelectorAll('.bubble')).map(node=>node.textContent);
  assert.equal(bubbles.filter(text=>text==='Something with a bunny').length,1);
  assert.equal(bubbles.filter(text=>text.startsWith(answer.reply)).length,1);
});
