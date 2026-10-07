'use strict';

// Production widget and phrase planner, with synthetic transport/audio events.
// These tests establish same-session recovery and cancellation. They make no
// provider calls and cannot certify physical audio, word timing or GPU quality.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const {JSDOM, VirtualConsole} = require('jsdom');
const Expression = require('../../brites-concierge-expression.js');
const widget = fs.readFileSync(require.resolve('../../brites-concierge.js'), 'utf8');
const actions = fs.readFileSync(require.resolve('../../brites-concierge-voice-actions.js'), 'utf8');
const settle = async () => {await new Promise(resolve => setImmediate(resolve)); await new Promise(resolve => setImmediate(resolve));};
const reply = 'Thank you for sharing that. This silver pendant may be a personal reminder. Which design do you prefer?';

function fixture(t, {lateModule = false, legacy = false} = {}) {
  const errors = [], console = new VirtualConsole();
  console.on('jsdomError', value => errors.push(value));
  const dom = new JSDOM('<!doctype html><body></body>', {
    url: 'https://preview.example/concierge-sandbox.html',
    pretendToBeVisual: true, runScripts: 'outside-only', virtualConsole: console
  });
  const w = dom.window, d = w.document;
  const script = d.createElement('script');
  script.src = '/brites-concierge.js'; script.dataset.sandbox = 'true';
  Object.defineProperty(d, 'currentScript', {get: () => script});
  let clock = 1791340000000, serial = 0, hidden = false, controller = null, config = null, output = null;
  const timers = new Map(), calls = {starts: 0, stops: 0, resumes: 0, expressions: [], levels: [], requests: []};
  Object.defineProperty(d, 'hidden', {get: () => hidden});
  w.Date.now = () => clock;
  w.setTimeout = (fn, ms) => {const id = ++serial; timers.set(id, {fn, at: clock + Number(ms || 0)}); return id;};
  w.clearTimeout = id => timers.delete(id);
  w.HTMLElement.prototype.scrollIntoView = function() {};
  function installModule() {
    w.BritesConciergeExpression = {...Expression, create(options) {
      controller = Expression.create({...options, now: () => clock}); return controller;
    }};
  }
  if (!lateModule) installModule();
  w.BritesConciergeAvatar = {create: () => ({
    setExpression: value => calls.expressions.push(value ? {...value} : null),
    setLevel: value => calls.levels.push(value), setEmotion() {}, setState() {},
    setPaused() {}, setVisible() {}, setFloating() {}, triggerGreeting() {},
    cancelPerformance() {}, retry() {}, destroy() {}
  })};
  const client = {
    state: 'listening', outputMeterState: 'ready', playbackBlocked: false,
    async start() {calls.starts++; return true;},
    async stop() {calls.stops++; output = null;},
    async resumeAudio() {calls.resumes++; this.playbackBlocked = false; config.onPlaybackResumed(); return true;},
    interrupt() {output = null;}, async dispose() {output = null;}
  };
  if (!legacy) Object.defineProperty(client, 'currentOutput', {get: () => output});
  w.BritesConciergeVoice = {create: value => {config = value; return client;}};
  w.fetch = async (url, init = {}) => {
    calls.requests.push({url: String(url), body: init.body ? JSON.parse(init.body) : {}});
    return {ok: true, json: async () => ({enabled: true})};
  };
  w.eval(actions); w.eval(widget);
  const root = d.querySelector('brites-concierge').shadowRoot;
  const button = label => [...root.querySelectorAll('button')].find(value => value.textContent.trim() === label || value.getAttribute('aria-label') === label);
  async function start() {
    w.BritesConcierge.open(); await settle(); button('Talk to me').click(); await settle();
    root.querySelector('script[src$="brites-concierge-voice.js"]')?.dispatchEvent(new w.Event('load')); await settle();
  }
  function advance(ms) {
    const end = clock + ms;
    while (clock < end) {
      clock = Math.min(end, clock + 50);
      for (const [id, value] of [...timers]) if (value.at <= clock) {timers.delete(id); value.fn();}
    }
  }
  function speech(id = 'input-current', version = 1) {
    output = null; config.onSpeechStarted({itemId: id, turnVersion: version, reason: 'speech'});
    client.state = 'listening'; config.onState('listening');
    return {inputItemId: id, turnVersion: version, currentTurn: true};
  }
  function transcript(turn, text = reply, responseId = 'response-current', itemId = 'output-current') {
    config.onTranscript({role: 'assistant', ...turn, responseId, itemId, contentIndex: 0, outputIndex: 0, text, final: true});
  }
  function playback(turn, {playing = true, cleared = false, responseId = 'response-current', itemId = 'output-current'} = {}) {
    output = playing ? {...turn, responseId, itemId, playing: true} : null;
    config.onPlaybackState({...turn, responseId, itemId, playing, cleared});
    client.state = playing ? 'speaking' : 'listening'; config.onState(client.state);
  }
  function sample(turn, mediaMs, amplitude = .4, responseId = 'response-current') {
    config.onLevel({...turn, responseId, itemId: output?.itemId || '', input: 0, output: amplitude, outputTimeMs: mediaMs});
  }
  function block() {client.playbackBlocked = true; config.onPlaybackBlocked();}
  async function enable() {button('Enable audio').click(); await settle();}
  async function loadModule() {
    installModule(); const loader = root.querySelector('script[src$="brites-concierge-expression.js"]');
    assert.ok(loader, 'one optional expression script is pending');
    loader.dispatchEvent(new w.Event('load')); await settle(); assert.ok(controller);
  }
  function state() {return controller.snapshot();}
  t.after(async () => {
    try {w.BritesConcierge.close();} catch {}
    controller?.destroy(); timers.clear(); w.close(); await settle(); assert.deepEqual(errors, []);
    assert.equal(calls.requests.filter(value => value.body.action === 'start').length, 0, 'synthetic tests never call the native provider start endpoint');
  });
  return {w, d, root, button, client, calls, start, advance, speech, transcript, playback, sample, block, enable, loadModule, state,
    get config() {return config;}, get controller() {return controller;},
    hide(value) {hidden = value; d.dispatchEvent(new w.Event('visibilitychange'));}};
}

async function speaking(t, options) {
  const h = fixture(t, options); await h.start(); const turn = h.speech();
  h.transcript(turn); h.playback(turn); h.sample(turn, 0); h.advance(1000); h.sample(turn, 1000);
  return {h, turn};
}

test('Enable audio retains consumed phrase progress and requires fresh media samples in the same native session', async t => {
  const {h, turn} = await speaking(t); const before = h.state(); assert.equal(before.clockMs, 1000);
  h.block(); assert.equal(h.state().playbackHeld, true); assert.equal(h.state().cue, null);
  const count = h.calls.expressions.filter(Boolean).length;
  h.advance(6000); h.sample(turn, 16000, .9);
  assert.equal(h.state().clockMs, before.clockMs); assert.equal(h.calls.expressions.filter(Boolean).length, count);
  await h.enable(); assert.equal(h.state().active, true); assert.equal(h.state().playbackHeld, false); assert.equal(h.state().cue, null);
  assert.equal(h.state().clockMs, 1000, 'recovery does not consume the blocked interval');
  h.sample(turn, 20000); assert.equal(h.state().clockMs, 1000, 'first fresh sample only anchors');
  h.advance(100); h.sample(turn, 20100); assert.equal(h.state().clockMs, 1100);
  assert.ok(h.state().cue); assert.equal(h.calls.starts, 1); assert.equal(h.calls.stops, 0); assert.equal(h.calls.resumes, 1);
});
for(const lateModule of [false,true])test('recovered speaking content stays neutral across timer ticks and quiet or foreign samples'+(lateModule?' after delayed planner loading':''),async t=>{
  const {h,turn}=await speaking(t,{lateModule});h.block();await h.enable();if(lateModule)await h.loadModule();const count=h.calls.expressions.filter(Boolean).length;
  assert.equal(h.state().awaitingRecoveryOutput,true);h.advance(1200);assert.equal(h.state().cue,null);assert.equal(h.state().clockMs,1000);
  h.sample(turn,20000,0);h.advance(500);h.sample({...turn,inputItemId:'old-input'},21000,.9);h.advance(500);assert.equal(h.state().cue,null);assert.equal(h.state().awaitingRecoveryOutput,true);assert.equal(h.calls.expressions.filter(Boolean).length,count);
  h.sample(turn,22000,.4);assert.equal(h.state().awaitingRecoveryOutput,false);assert.equal(h.state().clockMs,1000);assert.ok(h.state().cue);h.advance(100);h.sample(turn,22100,.4);assert.equal(h.state().clockMs,1100);assert.equal(h.calls.starts,1);
});
test('recovery before the first native buffer keeps spoken cues neutral until fresh positive output',async t=>{
  const h=fixture(t);await h.start();const turn=h.speech();h.block();h.transcript(turn);await h.enable();assert.equal(h.state().playing,false);assert.equal(h.state().awaitingRecoveryOutput,true);h.playback(turn);h.advance(1000);assert.equal(h.state().cue,null);assert.equal(h.state().clockMs,0);h.sample(turn,30000,.4);assert.equal(h.state().awaitingRecoveryOutput,false);assert.equal(h.state().clockMs,0);assert.equal(h.state().cue.kind,'appreciate');
});
for(const [name,metadata]of [['noncurrent',{currentTurn:false}],['foreign input',{inputItemId:'old-input'}],['foreign turn',{turnVersion:999}]])test(name+' assistant transcript cannot replace the current caption or saved history',async t=>{
  const {h,turn}=await speaking(t);const caption=h.root.querySelector('.caption-text').textContent,history=JSON.parse(h.w.sessionStorage.getItem('brites-concierge-v1')).history;
  h.config.onTranscript({role:'assistant',...turn,...metadata,responseId:'old-response',itemId:'old-output',text:'Obsolete assistant content',final:true});assert.equal(h.root.querySelector('.caption-text').textContent,caption);assert.deepEqual(JSON.parse(h.w.sessionStorage.getItem('brites-concierge-v1')).history,history);assert.equal(h.state().responseId,'response-current');
});

for (const beforeBuffer of [true, false]) test('a playback gate before ' + (beforeBuffer ? 'native buffer start' : 'the first transcript') + ' retains waiting current content without rendering it', async t => {
  const h = fixture(t); await h.start(); const turn = h.speech(); h.block();
  if (!beforeBuffer) h.playback(turn);
  h.transcript(turn); if (beforeBuffer) h.playback(turn);
  h.advance(1500); h.sample(turn, 3000, .9);
  assert.equal(h.state().playbackHeld, true); assert.equal(h.state().clockMs, 0); assert.equal(h.state().cue, null);
  assert.equal(h.state().phraseCount, 3); await h.enable();
  h.sample(turn, 7000); h.advance(200); h.sample(turn, 7200);
  assert.equal(h.state().clockMs, 200); assert.ok(h.state().cue); assert.equal(h.calls.starts, 1); assert.equal(h.calls.stops, 0);
});

test('enabling before any native buffer arrives does not invent playback and allows that current future buffer', async t => {
  const h = fixture(t); await h.start(); const turn = h.speech(); h.block(); h.transcript(turn);
  await h.enable(); assert.equal(h.state().playbackHeld, false); assert.equal(h.state().playing, false); assert.equal(h.state().clockMs, 0);
  h.advance(1000); assert.equal(h.state().cue?.kind, 'attentive'); assert.equal(h.state().clockMs, 0, 'waiting is attention, not invented speech');
  h.playback(turn); h.sample(turn, 5000); h.advance(150); h.sample(turn, 5150);
  assert.equal(h.state().clockMs, 150); assert.ok(h.state().cue); assert.equal(h.calls.starts, 1);
});

for (const cleared of [false, true]) test('native ' + (cleared ? 'clear' : 'stop') + ' while held cannot be revived by Enable audio or late output', async t => {
  const {h, turn} = await speaking(t); h.block(); h.playback(turn, {playing: false, cleared});
  await h.enable(); const count = h.calls.expressions.filter(Boolean).length;
  h.sample(turn, 20000, .9); h.advance(1000); h.sample(turn, 21000, .9);
  assert.equal(h.state().playing, false); assert.equal(h.state().cue, null);
  assert.equal(h.calls.expressions.filter(Boolean).length, count); assert.equal(h.calls.starts, 1);
});

for (const boundary of ['End voice', 'hide', 'Pause animation']) test(boundary + ' remains destructive during a recoverable playback gate', async t => {
  const {h, turn} = await speaking(t); h.block();
  if (boundary === 'hide') h.hide(true); else h.button(boundary).click();
  const count = h.calls.expressions.filter(Boolean).length;
  h.client.playbackBlocked = false; h.config.onPlaybackResumed(); h.sample(turn, 9000, .9); h.advance(1500);
  assert.equal(h.state().active, false); assert.equal(h.state().cue, null); assert.equal(h.state().clockMs, 0);
  assert.equal(h.calls.expressions.filter(Boolean).length, count);
});

test('a new input while blocked replaces all old content and holds only the new current turn', async t => {
  const {h, turn: old} = await speaking(t); h.block();
  const fresh = h.speech('input-new', 2); h.transcript(fresh, 'I will check that carefully. Which option do you mean?', 'response-new', 'output-new');
  h.transcript(old, 'Congratulations!', 'response-current');
  h.playback(fresh, {responseId: 'response-new', itemId: 'output-new'});
  assert.equal(h.state().turnId, 'input-new'); assert.equal(h.state().playbackHeld, true); assert.equal(h.state().clockMs, 0);
  await h.enable(); h.sample(fresh, 25000, .4, 'response-new'); h.advance(200); h.sample(fresh, 25200, .4, 'response-new');
  assert.equal(h.state().clockMs, 200); assert.equal(h.state().responseId, 'response-new'); assert.ok(h.state().cue);
  assert.notEqual(h.state().cue.kind, 'celebrate'); assert.equal(h.calls.starts, 1);
});

for (const loadWhileHeld of [true, false]) test('a late expression asset ' + (loadWhileHeld ? 'loaded while held' : 'loaded after explicit recovery') + ' keeps progress and never replays a missed interval', async t => {
  const {h, turn} = await speaking(t, {lateModule: true}); h.block(); h.advance(4500); h.sample(turn, 18000, .9);
  if (loadWhileHeld) {
    await h.loadModule(); assert.equal(h.state().playbackHeld, true); assert.equal(h.state().clockMs, 1000); assert.equal(h.state().cue, null);
    await h.enable();
  } else {
    await h.enable(); await h.loadModule(); assert.equal(h.state().playbackHeld, false); assert.equal(h.state().clockMs, 1000);
  }
  h.sample(turn, 22000); h.advance(150); h.sample(turn, 22150);
  assert.equal(h.state().clockMs, 1150); assert.ok(h.state().cue); assert.equal(h.calls.starts, 1); assert.equal(h.calls.stops, 0);
});

test('a late asset after End during an autoplay gate cannot hydrate old text or progress', async t => {
  const {h, turn} = await speaking(t, {lateModule: true}); h.block(); h.button('End voice').click(); await h.loadModule();
  h.config.onPlaybackResumed(); h.sample(turn, 16000, .9); h.advance(1000);
  assert.equal(h.state().active, false); assert.equal(h.state().phraseCount, 0); assert.equal(h.state().cue, null);
});

test('repeat holds retain the exact estimated position and never re-emit a previously consumed phrase transition', async t => {
  const {h, turn} = await speaking(t); const initialTransitions = h.state().transitions;
  for (let index = 0; index < 3; index++) {
    h.block(); h.advance(1000); await h.enable();
    h.sample(turn, 20000 + index * 4000); h.advance(100); h.sample(turn, 20100 + index * 4000);
  }
  assert.equal(h.state().clockMs, 1300); assert.equal(h.state().transitions, initialTransitions); assert.equal(h.calls.starts, 1); assert.equal(h.calls.stops, 0);
});

test('recovery continues distinct meaningful clauses and reaches the final question instead of a static face', async t => {
  const {h, turn} = await speaking(t); h.block(); await h.enable(); h.sample(turn, 20000);
  for (let index = 1; index <= 5; index++) {h.advance(1000); h.sample(turn, 20000 + index * 1000);}
  assert.equal(h.state().clockMs, 6000); assert.equal(h.state().cue?.kind, 'inquiry');
  const kinds = new Set(h.calls.expressions.filter(Boolean).map(value => value.kind));
  assert.ok(kinds.has('appreciate')); assert.ok(kinds.has('reflect')); assert.ok(kinds.has('inquiry'));
  assert.equal(h.state().transitions, 3); assert.equal(h.calls.starts, 1); assert.equal(h.calls.stops, 0);
});

test('duplicate same-response native starts while held do not erase the consumed estimate', async t => {
  const {h, turn} = await speaking(t); h.block(); h.playback(turn); h.playback(turn);
  assert.equal(h.state().clockMs, 1000); assert.equal(h.state().playbackHeld, true);
  await h.enable(); h.sample(turn, 30000); h.advance(100); h.sample(turn, 30100);
  assert.equal(h.state().clockMs, 1100); assert.equal(h.calls.starts, 1);
});

test('legacy buffer callbacks with qualified response identity recover without native getter or a new session', async t => {
  const {h, turn} = await speaking(t, {legacy: true}); h.block(); await h.enable();
  h.sample(turn, 4000); h.advance(100); h.sample(turn, 4100);
  assert.equal(h.state().clockMs, 1100); assert.ok(h.state().cue); assert.equal(h.calls.starts, 1);
});

test('unqualified, foreign input, turn, response and content identity cannot hold or recover a phrase planner', () => {
  let clock = 0; const planner = Expression.create({now: () => clock});
  const current = {currentTurn: true, inputItemId: 'input', turnVersion: 2, responseId: 'response', itemId: 'output', playing: true};
  planner.beginTurn({id: 'input', turnVersion: 2}); planner.transcript({role: 'assistant', ...current, text: reply}); planner.playback(current);
  planner.level({...current, output: .4, outputTimeMs: 0}); clock = 1000; planner.level({...current, output: .4, outputTimeMs: 1000});
  const invalid = [{}, {...current, currentTurn: false}, {...current, currentTurn: undefined}, {...current, inputItemId: 'old'}, {...current, turnVersion: 1}, {...current, turnVersion: undefined}, {...current, responseId: 'old'}, {...current, itemId: 'old-output'}];
  for (const value of invalid) {assert.equal(planner.holdPlayback(value), false); assert.equal(planner.snapshot().playbackHeld, false);}
  assert.equal(planner.holdPlayback(current), true);
  for (const value of invalid) {assert.equal(planner.recoverPlayback(value), false); assert.equal(planner.snapshot().playbackHeld, true); assert.equal(planner.snapshot().clockMs, 1000);}
  assert.equal(planner.recoverPlayback(current), true); assert.equal(planner.snapshot().cue, null);
  planner.cancel(); assert.equal(planner.recoverPlayback(current), false); planner.destroy();
});

for (const boundary of ['pause', 'reduced motion', 'destroy']) test('planner ' + boundary + ' cannot be undone by a delayed recovery binding', () => {
  const planner = Expression.create(); const current = {currentTurn: true, inputItemId: 'input', turnVersion: 1, responseId: 'response', playing: true};
  planner.beginTurn({id: 'input', turnVersion: 1}); planner.transcript({role: 'assistant', ...current, text: reply}); planner.playback(current); planner.holdPlayback(current);
  if (boundary === 'pause') planner.setPaused(true); else if (boundary === 'reduced motion') planner.setReducedMotion(true); else planner.destroy();
  assert.equal(planner.recoverPlayback(current), false); assert.equal(planner.snapshot().active, false); assert.equal(planner.snapshot().cue, null);
  planner.destroy();
});
