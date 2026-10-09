'use strict';

// Independent spoken regression checks. The host, bridge, widget, guide and realtime
// client are production code. Only transport, audio hardware, avatar pixels
// and scrolling are synthetic. These tests cannot certify physical speech,
// GPU appearance, mobile touch or live Shopify installation.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { JSDOM, VirtualConsole } = require('jsdom');
const project = process.env.BRITES_ADVERSARIAL46_PROJECT || path.resolve(__dirname, '../..');
const Voice = require(path.join(project, 'brites-concierge-voice.js'));
const Avatar = require(path.join(project, 'brites-concierge-avatar.js'));
const read = name => fs.readFileSync(path.join(project, name), 'utf8');
const clone = value => JSON.parse(JSON.stringify(value));
const settle = async () => { await new Promise(setImmediate); await new Promise(setImmediate); };
function deferred() { let resolve; return { promise: new Promise(done => { resolve = done; }), resolve }; }
async function eventually(predicate, reason) {
  for (let n = 0; n < 60; n++) { if (predicate()) return; await settle(); }
  assert.ok(predicate(), reason);
}

function product(id, title, motif, type = 'Earrings', materials = ['Sterling Silver', '14/20 Gold Filled']) {
  const handle = 'independent-spoken46-' + id;
  const axis = type === 'Necklace' ? 'Chain Length' : 'Hoop Diameter';
  const values = type === 'Necklace' ? ['17 inch', '21 inch'] : ['9mm', '11mm'];
  return {
    id: 'gid://shopify/Product/' + id, handle, title, type,
    url: 'https://britesjewelry.com/products/' + handle,
    description: 'This published ' + motif + ' design has a charm measuring 13 mm wide and 8 mm high.',
    tags: ['motif:' + motif], currency: 'USD', image: 'https://cdn.shopify.com/' + handle + '.jpg',
    images: [{ url: 'https://cdn.shopify.com/' + handle + '.jpg', alt: title }],
    detailState: 'checked', checkedAt: Date.now(), variantsComplete: true,
    options: [{ name: 'Metal Choice', values: materials }, { name: axis, values }],
    variants: materials.flatMap((metal, m) => values.map((value, n) => ({
      id: 'gid://shopify/ProductVariant/' + (id * 10 + m * 2 + n + 1),
      numericId: String(id * 10 + m * 2 + n + 1),
      title: metal + ' / ' + value, price: 43 + m * 27 + n * 6, available: true,
      options: [{ name: 'Metal Choice', value: metal }, { name: axis, value }]
    })))
  };
}
function catalogue() {
  return [
    product(74401, 'Fox Portrait Stud Earrings', 'fox'),
    product(74402, 'Fox Outline Stud Earrings', 'fox'),
    product(74403, 'Rabbit Drop Earrings', 'rabbit'),
    product(74404, 'Fox Pendant Necklace', 'fox', 'Necklace'),
    product(74405, 'Butterfly Pendant Necklace', 'butterfly', 'Necklace'),
    product(74406, 'Leaf Drop Earrings', 'leaf'),
    product(74407, 'Moon Pendant Necklace', 'moon', 'Necklace')
  ];
}

async function fixture(t, { rows = catalogue(), nativeVoice = false, manualClock = false, confirmedStop = true } = {}) {
  const errors = [], console = new VirtualConsole();
  console.on('jsdomError', error => errors.push(error));
  const dom = new JSDOM(read('concierge-sandbox.html'), { url: 'https://preview.example/concierge-sandbox.html', runScripts: 'outside-only', pretendToBeVisual: true, virtualConsole: console });
  const w = dom.window, d = w.document, requests = [], packets = [], hostResults = [], toolResults = [], avatarCalls = [];
  let avatarView = {state:'idle',emotion:'warm',expression:null};
  let client, channel, config, turn = 0, conciergeRead = null;
  let clock = Date.now(), timerId = 0;
  const timers = new Map();
  if (manualClock) {
    w.Date.now = () => clock;
    w.setTimeout = (fn, ms = 0) => { const id = ++timerId; timers.set(id, { at: clock + Number(ms), fn }); return id; };
    w.clearTimeout = id => timers.delete(id);
  }
  delete d.body.dataset.catalogueSeed;
  w.matchMedia = () => ({ matches: false, addEventListener() {}, removeEventListener() {} });
  w.HTMLElement.prototype.scrollIntoView = function () {};
  w.scrollTo = () => {};
  w.fetch = async (raw, init = {}) => {
    const url = new URL(String(raw), w.location.href);
    requests.push({ url, init });
    let body;
    if (url.pathname === '/api/growth/catalogue') body = { live: true, checkedAt: Date.now(), products: rows, pageInfo: { hasNextPage: false, endCursor: null } };
    else if (url.pathname === '/api/growth/inventory') body = { live: true, checkedAt: Date.now(), products: rows, inventory: { schema: 1, total: rows.length, offset: 0, limit: 24, loaded: rows.length, detailsLoaded: rows.length, ready: true, partial: false, expiresAt: Date.now() + 300000 }, pageInfo: { hasNextPage: false, nextOffset: null } };
    else if (url.pathname === '/api/growth/product') body = { live: true, checkedAt: Date.now(), product: rows.find(p => p.handle === url.searchParams.get('handle')) };
    else if (url.pathname === '/api/growth/storefront-services') body = { schema: 1, guidance: {}, conflicts: [], offers: { items: [] } };
    else if (url.pathname === '/api/growth/knowledge') body = { live: true, checkedAt: Date.now(), meanings: [] };
    else if (url.pathname === '/api/growth/events') body = { ok: true };
    else if (url.pathname === '/api/concierge') {
      const input = JSON.parse(init.body || '{}');
      body = conciergeRead && input.message ? await conciergeRead(input) : { live: true, reply: 'INDEPENDENT46_REMOTE_FALLBACK', products: [], meanings: [], preferences: {}, preserveSelection: true };
    } else if (url.pathname === '/api/concierge-voice') {
      const action = JSON.parse(init.body || '{}').action;
      body = action === 'start' ? { sdp: 'v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=candidate:1 1 UDP 2122260223 192.0.2.10 50000 typ host\r\n', stopToken: 'independent46-synthetic', maxDurationMs: 120000 } : { ok: true, enabled: true, nativeAudio: true, publicDemo: true, stopped: action === 'stop' && confirmedStop };
    } else throw Error('Unexpected independent request ' + url.pathname);
    return { ok: true, json: async () => clone(body) };
  };
  for (const name of ['brites-catalogue-intents.js', 'brites-concierge-shopping-guide.js', 'brites-storefront-bridge.js', 'brites-concierge-voice-actions.js', 'concierge-sandbox.js']) w.eval(read(name));
  await settle();
  const store = w.BritesSandboxStorefront;
  await store.preloadInventory(); await settle();
  if (nativeVoice) {
    w.HTMLMediaElement.prototype.play = async function () {};
    w.HTMLMediaElement.prototype.pause = function () {};
    class Peer {
      constructor() { this.iceGatheringState = 'complete'; }
      addTrack() {} close() {}
      createDataChannel() { channel = { readyState: 'connecting', send: value => packets.push(JSON.parse(value)), close() {} }; return channel; }
      async createOffer() { return { type: 'offer', sdp: 'v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=candidate:1 1 UDP 2122260223 192.0.2.10 50000 typ host\r\n' }; }
      async setLocalDescription(value) { this.localDescription = value; }
      async setRemoteDescription() { channel.readyState = 'open'; channel.onopen(); }
    }
    const runtime = {
      document: d, location: w.location,
      navigator: { mediaDevices: { async getUserMedia() { const track = { readyState: 'live', stop() { this.readyState = 'ended'; }, addEventListener() {}, removeEventListener() {} }; return { getTracks: () => [track], getAudioTracks: () => [track] }; } } },
      RTCPeerConnection: Peer, AbortController: w.AbortController, fetch: w.fetch,
      setTimeout: w.setTimeout.bind(w), clearTimeout: w.clearTimeout.bind(w), addEventListener: w.addEventListener.bind(w), removeEventListener: w.removeEventListener.bind(w)
    };
    w.BritesConciergeVoice = { publicContext: Voice.publicContext, create(options) { config = options; client = Voice.create({ ...options, runtime, greeting: false, onFinalizedTurn: async turn => { const result = await options.onFinalizedTurn(turn); hostResults.push({ turn, result }); return result; },onTool:async(args,context)=>{const result=await options.onTool(args,context);toolResults.push({args,context,result});return result;} }); return client; } };
  }
  w.eval(read('brites-concierge-expression.js'));
  w.BritesConciergeAvatar = { create() { return { setState(value) {avatarView.state=value;avatarCalls.push(['state',value]);}, setEmotion(value) {avatarView.emotion=value;avatarCalls.push(['emotion',value]);}, setExpression(value) {avatarView.expression=clone(value);avatarCalls.push(['expression',value]);}, setSpeechSignal() {}, setInputSignal() {}, setVisible() {}, setPaused() {}, setLevel() {}, setFloating() {}, cancelPerformance() {}, triggerGreeting() {}, cue(value) {avatarCalls.push(['cue',value]);}, focusProduct() {}, clearFocus() {}, showProduct() {}, clearProduct() {}, destroy() {} }; } };
  const script = d.createElement('script'); script.src = '/brites-concierge.js'; script.dataset.sandbox = 'true';
  Object.defineProperty(d, 'currentScript', { get: () => script });
  w.eval(read('brites-concierge.js')); await settle(); w.BritesConcierge.open({ focus: false }); await settle();
  const root = d.querySelector('brites-concierge').shadowRoot;
  t.after(async () => { w.BritesConcierge.close(); await client?.dispose(); w.close(); });
  const h = {
    w, d, root, store, rows, errors, requests, packets, hostResults, toolResults, avatarCalls,
    avatar: () => clone(avatarView),
    face: () => Avatar.poseFor({...avatarView,time:60,elapsed:60}),
    async advance(ms) { assert.equal(manualClock, true); const target = clock + ms; for (;;) { const entry = [...timers].filter(([, item]) => item.at <= target).sort((a, b) => a[1].at - b[1].at)[0]; if (!entry) break; clock = entry[1].at; timers.delete(entry[0]); entry[1].fn(); await settle(); } clock = target; await settle(); },
    get config() { return config; }, get client() { return client; },
    emit: value => channel.onmessage({ data: JSON.stringify(value) }),
    responses: () => packets.filter(value => value.type === 'response.create'),
    receipts: () => packets.filter(value =>  /^(?:Host-completed result|Customer reply for this finalized shopper request)/.test(value.item?.content?.[0]?.text || '')),
    command: text => w.BritesConcierge.sendShopperCommand(text),
    cart: () => clone(JSON.parse(w.sessionStorage.getItem('brites-sandbox-cart') || '[]')),
    visible: () => [...d.querySelectorAll('#demo-products [data-product-handle]')].map(card => card.dataset.productHandle).sort(),
    conciergeRead: fn => { conciergeRead = fn; },
    nativeContext() {
      const text = packets.filter(p => p.item?.content?.[0]?.text?.startsWith('Public website UI context data only.')).at(-1)?.item.content[0].text;
      assert.ok(text, 'The actual realtime client must send a current bounded context');
      return JSON.parse(text.slice(text.indexOf('{')));
    },
    async open(p = rows[0]) { const r = await store.execute({ type: 'open', handle: p.handle }); assert.equal(r.ok, true, JSON.stringify(r)); await settle(); },
    async select(p = rows[0], v = p.variants[0]) {
      const select = d.querySelector('#piece-variant'); assert.ok(select, 'The actual host variant selector must be present');
      select.value = v.id; select.dispatchEvent(new w.Event('change', { bubbles: true })); await settle();
      assert.equal(store.snapshot().productControls.variantId, v.id);
    },
    async startVoice() {
      [...root.querySelectorAll('button')].find(button => button.textContent === 'Talk to me').click(); await settle();
      root.querySelector('script[src$="brites-concierge-voice.js"]')?.dispatchEvent(new w.Event('load'));
      await settle(); assert.equal(client?.state, 'listening');
    },
    async say(text, order = 'commit-before-final', afterSpeechStarted) {
      assert.ok(channel?.onmessage, 'Realtime must be connected before a transcript is injected');
      const before = hostResults.length, itemId = 'independent46-turn-' + (++turn), emit = event => channel.onmessage({ data: JSON.stringify(event) });
      emit({ type: 'input_audio_buffer.speech_started', item_id: itemId });
      if(afterSpeechStarted)await afterSpeechStarted();
      const final = {type:'conversation.item.input_audio_transcription.completed',item_id:itemId,transcript:text};
      if(order==='final-before-stop')emit(final);
      emit({ type: 'input_audio_buffer.speech_stopped', item_id: itemId });
      if(order==='final-before-commit')emit(final);
      emit({ type: 'input_audio_buffer.committed', item_id: itemId });
      if(order==='commit-before-final')emit(final);
      await eventually(() => hostResults.length > before, 'The actual widget finalized native ASR callback must finish');
      assert.equal(hostResults.length,before+1,'The same final native shopper turn is processed once');
      const receipt = this.receipts().at(-1)?.item.content[0].text;
      this.spokenResult=receipt?JSON.parse(receipt.slice(receipt.indexOf('{'))):null;
      this.lastInput={itemId,turnVersion:hostResults.at(-1).turn.turnVersion};
      return hostResults.at(-1).result;
    },
    async speakAndDrain(text) {
      const request = this.responses().at(-1), responseId='independent46-answer-'+turn;
      assert.ok(request,'The actual native client must request this response');
      this.emit({type:'response.created',response:{id:responseId,metadata:request.response.metadata}});
      this.emit({type:'response.output_item.added',response_id:responseId,item:{id:responseId+'-message',type:'message',role:'assistant'}});
      this.emit({type:'output_audio_buffer.started',response_id:responseId});
      this.emit({type:'response.output_audio_transcript.done',response_id:responseId,item_id:responseId+'-message',transcript:text});
      this.emit({type:'response.done',response:{id:responseId,status:'completed'}});
      this.emit({type:'output_audio_buffer.stopped',response_id:responseId});
      await settle();return responseId;
    }

  };
  if (nativeVoice) await h.startVoice();
  return h;
}

function necklace({ id = 74501, materials = ['Sterling Silver', '14k Gold Filled', '14k Solid Gold'], engraving = false } = {}) {
  const p = product(id, 'Fox Portrait Necklace', 'fox', 'Necklace', materials);
  p.options[1].values = ['16 inches', '18 inches'];
  if (engraving) p.options.push({ name: 'Engraving', values: ['No Engraving', 'Engraved'] });
  p.variants = materials.flatMap((metal, m) => ['16 inches', '18 inches'].flatMap((length, n) => (engraving ? ['No Engraving', 'Engraved'] : [null]).map((wording, e) => ({
    id: 'gid://shopify/ProductVariant/' + (id * 100 + m * 8 + n * 2 + e + 1),
    numericId: String(id * 100 + m * 8 + n * 2 + e + 1),
    title: [metal, length, wording].filter(Boolean).join(' / '),
    price: 45 + m * 70 + n * 6 + e * 8, available: true,
    options: [{ name: 'Metal Choice', value: metal }, { name: 'Chain Length', value: length }].concat(wording ? [{ name: 'Engraving', value: wording }] : [])
  }))));
  return p;
}

const INTERNAL_COPY = /\b(?:backend|postconditions?|control_storefront|completedActions|stepResults|inputItemId|turnVersion|no further action(?:s)? (?:was|were) completed|completed \d+ (?:of \d+ )?(?:requested )?actions|remaining website actions|motif (?:parser|resolver|metadata))\b/i;
function shopperCopy(result) {
  const reply = result.reply || result.error;
  assert.equal(typeof reply, 'string', 'Every handled website request needs a shopper-readable reply');
  assert.ok(reply.trim(), 'A handled request cannot silently complete');
  assert.doesNotMatch(reply, INTERNAL_COPY, 'Customer copy must not explain internal processing');
  return reply;
}
function currentChoices(h) { return clone(h.store.snapshot().productControls.selectedOptions); }
function expectedChoices(p, v) { return clone(v.options).sort((a, b) => a.name.localeCompare(b.name)); }
function sortedChoices(h) { return currentChoices(h).sort((a, b) => a.name.localeCompare(b.name)); }
async function chosen(h, p, { quantity = 1, engraved = false } = {}) {
  await h.open(p);
  for (const words of ['Select 14k Solid Gold', 'Select 16 inches'].concat(engraved ? ['Select Engraved'] : p.options.some(g => g.name === 'Engraving') ? ['Select No Engraving'] : [])) {
    const result = await h.say(words); assert.equal(result.ok, true, JSON.stringify(result)); shopperCopy(result);
  }
  if (quantity !== 1) { const result = await h.say('Set quantity to ' + quantity); assert.equal(result.ok, true, JSON.stringify(result)); }
  assert.equal(h.store.snapshot().productControls.quantity, quantity);
  const variant = p.variants.find(v => v.options.every(o => o.value === (o.name === 'Metal Choice' ? '14k Solid Gold' : o.name === 'Chain Length' ? '16 inches' : engraved ? 'Engraved' : 'No Engraving')));
  assert.equal(h.store.snapshot().productControls.variantId, variant.id);
  return variant;
}

function initialNecklace({id=74601,description='The lowercase pendant measures 6–9 mm depending on the letter. The necklace comes on a 14–18 inch chain.',materials=['Sterling Silver','14k Gold Filled','14k Solid Gold']}={}) {
  const p=product(id,'Lowercase Initial Necklace','initial','necklace',materials);
  p.handle='lowercase-initial';p.url='https://britesjewelry.com/products/lowercase-initial';p.description=description;
  const lengths=['14 inch','16 inch','18 inch','20 inch'],words=['None','Engraved'];
  p.options=[{name:'Metal Choice',values:materials},{name:'Necklace Length',values:lengths},{name:'Engraving',values:words}];
  p.variants=materials.flatMap((metal,m)=>lengths.flatMap((length,n)=>words.map((wording,e)=>{const num=String(id*100+m*16+n*2+e+1);return {id:'gid://shopify/ProductVariant/'+num,numericId:num,title:[metal,length,wording].join(' / '),price:49+m*90+n*7+e*10,available:true,options:[{name:'Metal Choice',value:metal},{name:'Necklace Length',value:length},{name:'Engraving',value:wording}]};})));
  return p;
}
const wordCount=value=>String(value).trim().split(/\s+/).length;
function shortReply(result,max=35){const reply=shopperCopy(result);assert.ok(wordCount(reply)<=max,'An ordinary spoken answer should be brief; '+wordCount(reply)+' words: '+reply);return reply;}
function selected(h,name){return h.store.snapshot().productControls.selectedOptions.find(o=>o.name===name)?.value;}

for(const order of ['commit-before-final','final-before-commit','final-before-stop'])test('native final ASR selects published lowercase choices once without a provider tool: '+order,async t=>{
  const p=initialNecklace(),h=await fixture(t,{rows:[p],nativeVoice:true});await h.open(p);
  const r=await h.say('Select a 16 inch solid gold necklace for me',order);assert.equal(r.ok,true,JSON.stringify(r));
  assert.equal(selected(h,'Metal Choice'),'14k Solid Gold');assert.equal(selected(h,'Necklace Length'),'16 inch');assert.equal(selected(h,'Engraving'),undefined);
  assert.equal(h.store.snapshot().productControls.variantId,null);assert.deepEqual(h.cart(),[]);assert.equal(r.completedActions.length,2);assert.doesNotMatch(shopperCopy(r),/choose(?: the)? (?:necklace )?length/i);
  assert.equal(h.responses().at(-1).response.tool_choice,'none');assert.equal(h.packets.some(p=>p.item?.type==='function_call_output'),false,'The finalized shopper can select options without waiting for the model to choose a tool');
  const finished=await h.say('Select None then set quantity to 2 then add this to my cart');assert.equal(finished.ok,true,JSON.stringify(finished));
  const exact=p.variants.find(v=>v.title==='14k Solid Gold / 16 inch / None');assert.equal(h.cart()[0].variantId,exact.numericId);assert.equal(h.cart()[0].quantity,2);
  const before=clone(h.cart());h.emit({type:'conversation.item.input_audio_transcription.completed',item_id:h.lastInput.itemId,transcript:'Select None then set quantity to 2 then add this to my cart'});await settle();assert.deepEqual(h.cart(),before,'A late duplicate native final cannot add twice');
});

for(const phrase of ['What are the dimensions?','How big is the pendant?','What is the pendant size?'])test('native ordinary dimensions answer gives a brief variable pendant measurement: '+phrase,async t=>{
  const p=initialNecklace(),h=await fixture(t,{rows:[p],nativeVoice:true});await h.open(p);const before=clone(h.store.snapshot().productControls);
  const r=await h.say(phrase);assert.equal(r.ok,true,JSON.stringify(r));const reply=shortReply(r,30);
  assert.match(reply,/6\s*[–-]\s*9\s*mm/i);assert.match(reply,/depending on (?:the )?letter/i);assert.doesNotMatch(reply,/Metal Choice|Engraving|shipping|tax|prices?|width|height/i,'A size answer must not invent axes or enumerate unrelated metal and engraving controls');
  if(phrase==='What are the dimensions?')assert.doesNotMatch(reply,/14\s*[–-]\s*18\s*inch/i,'A broad size answer must not quote an outdated chain range alongside the actual 20-inch choice');
  else assert.doesNotMatch(reply,/\b(?:14|16|18|20)\s*(?:inch|inches)/i,'An exact pendant question must retain only the pendant measurement');
  assert.deepEqual(clone(h.store.snapshot().productControls),before);assert.deepEqual(h.cart(),[]);assert.equal(h.responses().at(-1).response.tool_choice,'none');
});

for(const phrase of ['What chain lengths can I choose?','What necklace lengths are available?','How long can this necklace be?','Could you tell me about the available necklace lengths?'])test('native chain answer is brief and reads all actual length options rather than stale prose: '+phrase,async t=>{
  const p=initialNecklace(),h=await fixture(t,{rows:[p],nativeVoice:true});await h.open(p);const r=await h.say(phrase);assert.equal(r.ok,true,JSON.stringify(r));const reply=shortReply(r,30);
  for(const value of [14,16,18,20])assert.match(reply,new RegExp('\\b'+value+'\\b'),'Every exact published length remains available in the spoken answer');assert.match(reply,/inch/i);
  assert.doesNotMatch(reply,/6\s*[–-]\s*9|depending on.*letter|Metal Choice|Engraving|matching published options|checked available/i);
  assert.equal(h.store.snapshot().productControls.selectedOptions.length,0);assert.deepEqual(h.cart(),[]);
});

test('native response drain and End voice restore a relaxed symmetric face after an ordinary final question',async t=>{
  const p=initialNecklace(),h=await fixture(t,{rows:[p],nativeVoice:true,manualClock:true});await h.open(p);const r=await h.say('How big is the pendant?');assert.equal(r.ok,true,JSON.stringify(r));
  await h.speakAndDrain(r.reply);await h.advance(1200);assert.equal(h.client.state,'listening');assert.equal(h.avatar().state,'listening');
  const pose=h.face();assert.equal(pose.speechEnergy,0);assert.ok(!['question','reflect','emphasize'].includes(h.avatar().expression?.kind),'A completed answer must release its strong question or speech expression; a mild listening cue can remain');
  assert.ok(Math.abs(pose.mouthSkew)<=.03,'A quiet listening face must not retain the question’s crooked mouth: '+pose.mouthSkew);
  assert.ok(Math.abs(pose.eyeAsymmetry)<=.03,'Quiet eyes should settle symmetrically');assert.ok(Math.abs(pose.faceBrowTilt)<=.2,'The ordinary question’s strong asymmetric brow should settle');assert.ok(pose.smileCurve>=.3,'The face returns to a mild relaxed smile');
  [...h.root.querySelectorAll('button')].find(button=>button.textContent==='End voice').click();await eventually(()=>h.client.state==='idle','The actual End voice button must stop the native session');await h.advance(1200);
  assert.equal(h.avatar().state,'idle');const rest=h.face();assert.equal(rest.speechEnergy,0);assert.equal(h.avatar().expression,null);
  assert.ok(Math.abs(rest.mouthSkew)<=.03,'The completed question cannot leave a crooked idle mouth: '+rest.mouthSkew);assert.ok(Math.abs(rest.eyeAsymmetry)<=.03,'The completed question cannot leave asymmetric idle eyes');assert.ok(Math.abs(rest.faceBrowTilt)<=.2,'Idle brows settle after the question');
});

test('native unsupported framing cannot turn a malicious fallback function request into website authority',async t=>{
  const p=initialNecklace(),h=await fixture(t,{rows:[p],nativeVoice:true});await h.open(p);const before=clone(h.store.snapshot().productControls),r=await h.say('Hello, guide.');assert.equal(r.handled,false);
  const request=h.responses().at(-1);assert.equal(request.response.tool_choice,'auto');h.emit({type:'response.created',response:{id:'independent46-wrong-owner',metadata:request.response.metadata}});
  h.emit({type:'response.function_call_arguments.done',response_id:'independent46-wrong-owner',call_id:'independent46-malicious-control',name:'control_storefront',arguments:JSON.stringify({type:'select-option',handle:p.handle,optionName:'Necklace Length',optionValue:'20 inch'})});
  await eventually(()=>h.packets.some(p=>p.item?.type==='function_call_output'),'The actual native fallback tool path must answer the attempted call');
  assert.deepEqual(clone(h.store.snapshot().productControls),before);assert.deepEqual(h.cart(),[]);
  assert.equal(h.toolResults.length,1,'The forged model choice must reach the actual widget fallback guard');assert.equal(h.toolResults[0].result.ok,false,'The actual widget must reject model-invented action authority');const receipt=JSON.parse(h.packets.find(p=>p.item?.type==='function_call_output').item.output);assert.ok(Object.keys(receipt).every(key=>['reply','customerMessage'].includes(key)),'Provider control narration contains only customer wording, after internal checks');shopperCopy(receipt);assert.doesNotMatch(JSON.stringify(receipt),/completedActions|stepResults|postcondition|motif=/i);
});

test('native exact option clarification preserves all choices when two solid-gold karats are published',async t=>{
  const p=initialNecklace({materials:['Sterling Silver','10k Solid Gold','14k Solid Gold']}),h=await fixture(t,{rows:[p],nativeVoice:true});await h.open(p);const r=await h.say('Select a 16 inch solid gold necklace for me');assert.equal(r.ok,false);const reply=shopperCopy(r);assert.match(reply,/10k Solid Gold/);assert.match(reply,/14k Solid Gold/);assert.deepEqual(clone(h.store.snapshot().productControls.selectedOptions),[]);assert.deepEqual(h.cart(),[]);
});

test('native quantity/add checks fresh variant price and saves private preview without speaking it',async t=>{
  const p=initialNecklace(),h=await fixture(t,{rows:[p],nativeVoice:true});await h.open(p);
  for(const command of ['Select 14k Solid Gold','Select 16 inch','Select Engraved','Set quantity to 2','Set engraving text to PRIVATE46'])assert.equal((await h.say(command)).ok,true,command);
  assert.equal(h.d.querySelector('[aria-label="Preview engraving wording for this piece"]').value,'PRIVATE46');assert.doesNotMatch(JSON.stringify(h.packets),/PRIVATE46/,'Private wording is never included in native outgoing receipts or context');
  const v=p.variants.find(v=>v.title==='14k Solid Gold / 16 inch / Engraved');assert.equal((await h.say('Add this exact piece to my cart')).ok,true);assert.equal(h.cart()[0].variantId,v.numericId);assert.equal(h.cart()[0].quantity,2);assert.equal(h.cart()[0].engravingPreview,'PRIVATE46');assert.doesNotMatch(JSON.stringify(h.packets),/PRIVATE46/);
  const before=clone(h.cart());v.price+=11;const result=await h.say('Add this exact piece to my cart');assert.equal(result.ok,false);shopperCopy(result);assert.deepEqual(h.cart(),before);assert.equal(h.store.snapshot().productControls.quantity,2);
});

test('native current-piece selection accepts ordinary one wording while preserving an undecided third group',async t=>{
  const p=initialNecklace(),h=await fixture(t,{rows:[p],nativeVoice:true});await h.open(p);const r=await h.say('Can you choose the 16 inch one in solid gold?');assert.equal(r.ok,true,JSON.stringify(r));assert.equal(selected(h,'Metal Choice'),'14k Solid Gold');assert.equal(selected(h,'Necklace Length'),'16 inch');assert.equal(selected(h,'Engraving'),undefined);assert.equal(h.store.snapshot().productControls.variantId,null);assert.deepEqual(h.cart(),[]);shopperCopy(r);
});

test('native short fact narration refuses a duplicated raw identity rather than borrowing sanitized certainty',async t=>{
  const p=initialNecklace(),h=await fixture(t,{rows:[p],nativeVoice:true}),actual=h.store;await h.open(p);
  h.w.BritesSandboxStorefront={...actual,snapshot(){const page=actual.snapshot(),piece=page.visiblePieces.find(row=>row.handle===p.handle);assert.ok(piece);return {...page,visiblePieces:[...page.visiblePieces,clone(piece)]};}};
  h.d.dispatchEvent(new h.w.CustomEvent('brites-storefront:context'));await settle();const r=await h.say('How big is the pendant?');assert.equal(r.ok,false);assert.notEqual(r.verified,true);assert.notEqual(r.productFacts?.status,'verified');shopperCopy(r);assert.equal(actual.snapshot().productControls.selectedOptions.length,0);assert.deepEqual(h.cart(),[]);
});

test('native pending ASR cannot overwrite a shopper choice made after microphone speech starts',async t=>{
  const p=initialNecklace(),h=await fixture(t,{rows:[p],nativeVoice:true});await h.open(p);const exact=p.variants.find(v=>v.title==='Sterling Silver / 18 inch / None');
  const r=await h.say('Select a 16 inch solid gold necklace for me','commit-before-final',()=>h.select(p,exact));assert.equal(r.ok,false);shopperCopy(r);assert.equal(h.store.snapshot().productControls.variantId,exact.id);assert.equal(selected(h,'Metal Choice'),'Sterling Silver');assert.equal(selected(h,'Necklace Length'),'18 inch');assert.equal(selected(h,'Engraving'),'None');assert.deepEqual(h.cart(),[]);
});

test('native actual two-karat ambiguity cues once for that input and never chooses or repeats a change',async t=>{
  const p=initialNecklace({materials:['Sterling Silver','10k Solid Gold','14k Solid Gold']}),h=await fixture(t,{rows:[p],nativeVoice:true});await h.open(p);const before=clone(h.store.snapshot().productControls);
  const r=await h.say('Select a 16 inch solid gold necklace for me');assert.equal(r.ok,false);assert.match(shopperCopy(r),/10k Solid Gold/);assert.match(r.reply,/14k Solid Gold/);assert.deepEqual(clone(h.store.snapshot().productControls),before);assert.deepEqual(h.cart(),[]);
  assert.deepEqual(h.avatarCalls.filter(([method,value])=>method==='cue'&&value==='ambiguity'),[['cue','ambiguity']],'Only the real unresolved two-karat choice triggers one bounded ambiguity presentation');
  const completed=h.hostResults.length;h.emit({type:'conversation.item.input_audio_transcription.completed',item_id:h.lastInput.itemId,transcript:'Select a 16 inch solid gold necklace for me'});await settle();assert.equal(h.hostResults.length,completed);assert.equal(h.avatarCalls.filter(([method,value])=>method==='cue'&&value==='ambiguity').length,1);assert.deepEqual(clone(h.store.snapshot().productControls),before);
});

test('native ordinary inquiry, unknown option and cancellation never borrow the true-ambiguity face cue',async t=>{
  const p=initialNecklace(),h=await fixture(t,{rows:[p],nativeVoice:true});await h.open(p);const before=clone(h.store.snapshot().productControls);
  for(const words of ['How big is the pendant?','What chain lengths can I choose?','Select 17 inches','Select Platinum','No']){await h.say(words);assert.equal(h.avatarCalls.filter(([method,value])=>method==='cue'&&value==='ambiguity').length,0,words+' does not contain competing exact published choices');assert.deepEqual(clone(h.store.snapshot().productControls),before);assert.deepEqual(h.cart(),[]);}
});
