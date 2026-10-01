// The real Seal module, with a browser-policy-aware AudioContext fixture.
// Real trusted input is covered by the browser check; synthetic DOM events must stay silent here.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { JSDOM } = require('jsdom');
const source = fs.readFileSync(path.join(__dirname, '../../charm-nest-motion.js'), 'utf8');
const wait = ms => new Promise(resolve => setTimeout(resolve, ms));

function fixture(options = {}) {
  const dom = new JSDOM('<div id="host"><button data-seal-btn>Approved</button><span class="sealRow"></span></div><p id="outside">Work area</p>', { runScripts: 'outside-only', pretendToBeVisual: true });
  const w = dom.window, d = w.document;
  const records = { contexts: [], sources: [], gains: [], animations: [], unlockListeners: [] };
  const control = { initialState: options.initialState || 'suspended', resume: 'running', startError: false, bufferError: false, copyError: false };
  w.matchMedia = () => ({ matches: !!options.reduced });
  w.Element.prototype.getAnimations = () => [];
  w.Element.prototype.animate = function (frames, settings) {
    let finish;
    const gated = this.classList.contains('sealTool') || this.classList.contains('sealRing');
    const finished = gated ? new Promise(resolve => { finish = resolve; }) : Promise.resolve();
    const animation = { el: this, frames, settings, finished, finish: () => finish?.(), cancel: () => finish?.(), playState: 'finished' };
    records.animations.push(animation);
    return animation;
  };
  Object.defineProperty(w.HTMLElement.prototype, 'offsetWidth', { get() { return this.classList.contains('sealLens') ? parseFloat(this.style.width) || 200 : 84; } });
  Object.defineProperty(w.HTMLElement.prototype, 'offsetHeight', { get() { return this.classList.contains('sealLens') ? parseFloat(this.style.height) || 200 : 84; } });
  class FakeAudioContext {
    constructor(settings) { this.settings = settings; this.state = control.initialState; this.sampleRate = 48000; this.destination = {}; this.resumes = 0; records.contexts.push(this); }
    createBuffer(channels, length, rate) {
      if (control.bufferError) throw new Error('Temporary audio buffer failure');
      return { channels, length, rate, copyToChannel(samples, channel) { if (control.copyError) throw new Error('Temporary buffer copy failure'); this.samples = new Float32Array(samples); this.channel = channel; } };
    }
    resume() {
      this.resumes++;
      if (control.resume === 'reject') return Promise.reject(new Error('Browser autoplay policy'));
      if (control.resume === 'pending') return new Promise(resolve => { this.finishResume = () => { this.state = 'running'; resolve(); }; });
      this.state = 'running'; return Promise.resolve();
    }
    createBufferSource() {
      const node = { buffer: null, starts: 0, disconnects: 0, connect(to) { this.to = to; }, disconnect() { this.disconnects++; }, start() { if (control.startError) throw new Error('Audio device unavailable'); this.starts++; }, onended: null };
      records.sources.push(node); return node;
    }
    createGain() { const node = { gain: { value: 0 }, disconnects: 0, connect(to) { this.to = to; }, disconnect() { this.disconnects++; } }; records.gains.push(node); return node; }
  }
  if (!options.noAudio) w.AudioContext = FakeAudioContext;
  const add = d.addEventListener.bind(d);
  d.addEventListener = function (type, listener, settings) {
    if (['pointerdown', 'click', 'keydown', 'touchend'].includes(type) && settings?.capture === true && settings?.passive === true) records.unlockListeners.push({ type, listener });
    return add(type, listener, settings);
  };
  w.eval(source);
  assert(w.Seal.sound, 'Seal.sound exposes the shared sound engine');
  const rect = { left: 310, right: 394, top: 420, bottom: 504, width: 84, height: 84 };
  const visible = node => { node.getBoundingClientRect = () => rect; node.getClientRects = () => [rect]; return node; };
  visible(d.querySelector('#host')); visible(d.querySelector('button'));
  const seal = () => {
    const row = d.querySelector('.sealRow');
    row.innerHTML = w.Seal.html({ id: 'engraving-test', how: 'engraveApproved', at: Date.UTC(2026, 9, 1, 1, 44), by: 'Paul' }, 84, 'pending');
    return visible(row.firstElementChild);
  };
  return { dom, w, d, control, records, seal, close: () => w.close() };
}

async function settle() { for (let i = 0; i < 12; i++) await Promise.resolve(); await wait(1); }
const descent = f => f.records.animations.find(a => a.el.classList.contains('sealTool') && a.settings.duration === 560);
async function finishStamp(f, result) {
  await settle();
  for (const a of f.records.animations) if (a.el.classList.contains('sealTool') || a.el.classList.contains('sealRing')) a.finish();
  await result;
}

(async () => {
  const f = fixture();
  try {
    const sound = f.w.Seal.sound;
    assert.equal(f.records.contexts.length, 0, 'loading the app creates no audio context or sound');
    assert.equal(sound.play(), false, 'stamps stay silent until browser audio has been unlocked');
    f.seal().classList.remove('pending');
    for (const type of ['pointerdown', 'click', 'keydown', 'touchend']) f.d.querySelector('#outside').dispatchEvent(new f.w.Event(type, { bubbles: true }));
    await settle();
    assert.equal(f.records.contexts.length, 0, 'synthetic events cannot bypass browser gesture policy');
    assert.deepEqual(f.records.unlockListeners.map(l => l.type), ['pointerdown', 'click', 'keydown', 'touchend'], 'mouse, keyboard and touch unlock before the asynchronous approval');
    // Invoke only the policy fixture's trusted input handlers; no product event or isTrusted property is changed.
    for (const { listener } of f.records.unlockListeners) listener({ isTrusted: true });
    await settle();
    assert.equal(f.records.contexts.length, 1, 'trusted gestures reuse one context');
    assert.equal(f.records.sources.length, 0, 'unlocking itself is inaudible');
    assert.equal(sound.state().state, 'running');

    for (const rate of [8000, 44100, 48000, 96000]) {
      const samples = sound.samples(rate);
      const peak = Math.max(...samples.map(Math.abs));
      assert.equal(samples.length, Math.ceil(rate * .32), 'the tap is a short 320 ms sound');
      assert(samples.every(Number.isFinite));
      assert(peak > .06 && peak <= .08001, 'generated samples are normalised softly without clipping');
      assert(peak * sound.volume <= .05, 'effective playback peak stays at or below five percent');
      assert.equal(Math.abs(samples[0]), 0, 'the tap starts at zero to avoid a hard click');
      assert(Math.abs(samples[samples.length - 1]) < .00001, 'the paper tail returns smoothly to silence');
      const tail = samples.slice(Math.floor(rate * .27));
      assert(Math.max(...tail.map(Math.abs)) < .001, 'there is no sharp or loud ending');
    }
    const original = f.seal();
    const result = f.w.Seal.press(original);
    const duplicate = f.w.Seal.press(original);
    await settle();
    assert(descent(f), 'the wooden descent starts');
    assert.equal(sound.state().contacts, 0, 'no sound before physical stamp contact');
    assert(original.classList.contains('pending'), 'ink has not landed before contact');
    descent(f).finish(); await settle();
    assert.equal(sound.state().contacts, 1, 'one tap plays when ink lands after the full descent');
    assert(!original.classList.contains('pending'));
    assert(f.w.Seal.busy(), 'the next order waits for the stamp to lift and its ripple to finish');
    assert.equal(f.records.sources.length, 1, 'a double click cannot add another contact sound');
    assert.equal(f.records.sources[0].starts, 1);
    assert.equal(f.records.gains[0].gain.value, sound.volume);
    const playing = f.records.sources[0]; playing.onended();
    assert.equal(playing.disconnects, 1, 'finished audio sources disconnect');
    assert.equal(f.records.gains[0].disconnects, 1, 'finished gain nodes disconnect');
    await finishStamp(f, result); await duplicate;
    assert(!f.w.Seal.busy());
    assert.equal(f.d.querySelector('.sealTool'), null);
    assert.equal(sound.state().contacts, 1, 'lifting the stamp does not add a second tap');

    Object.defineProperty(f.d, 'visibilityState', { configurable: true, value: 'hidden' });
    assert.equal(sound.play(), false, 'hidden tabs remain silent');
    Object.defineProperty(f.d, 'visibilityState', { configurable: true, value: 'visible' });
    const firstContext = f.records.contexts[0]; firstContext.state = 'closed';
    assert.equal(sound.play(), false);
    assert(await sound.unlock(), 'a closed audio context is replaced on the next authorised interaction');
    assert.equal(f.records.contexts.length, 2);
    assert(sound.play());
    f.records.sources.at(-1).onended();
  } finally { f.close(); }

  const blocked = fixture();
  try {
    const sound = blocked.w.Seal.sound;
    blocked.control.resume = 'reject';
    assert.equal(await sound.unlock(), false, 'autoplay refusal is handled without an unhandled rejection');
    const result = blocked.w.Seal.press(blocked.seal()); await settle();
    descent(blocked).finish(); await settle();
    assert.equal(sound.state().contacts, 0, 'blocked audio does not produce a sound');
    await finishStamp(blocked, result);
    assert(!blocked.w.Seal.busy(), 'refused audio cannot block approval or animation completion');
    blocked.control.resume = 'running';
    assert(await sound.unlock(), 'the next valid interaction can retry audio');
    await settle();
    assert.equal(sound.state().contacts, 0, 'unlocking never replays an old blocked contact');
    blocked.control.startError = true;
    assert.equal(sound.play(), false, 'an unavailable audio device fails safely');
    assert.equal(blocked.records.sources.at(-1).disconnects, 1, 'a failed start also releases its source');
    assert.equal(blocked.records.gains.at(-1).disconnects, 1, 'a failed start also releases its gain');
    blocked.control.startError = false;
    assert(sound.play(), 'a later stamp can play after a temporary audio device failure');
    blocked.records.sources.at(-1).onended();
  } finally { blocked.close(); }

  const pending = fixture();
  try {
    pending.control.resume = 'pending';
    const unlocking = pending.w.Seal.sound.unlock();
    const result = pending.w.Seal.press(pending.seal()); await settle();
    descent(pending).finish(); await settle();
    assert.equal(pending.records.sources.length, 0, 'audio still suspended at contact is skipped');
    pending.records.contexts[0].finishResume(); await unlocking; await settle();
    assert.equal(pending.records.sources.length, 0, 'a delayed browser unlock cannot play an out-of-time tap');
    await finishStamp(pending, result);
  } finally { pending.close(); }

  for (const failure of ['bufferError', 'copyError']) {
    const retry = fixture({ initialState: 'running' });
    try {
      retry.control[failure] = true;
      assert.equal(await retry.w.Seal.sound.unlock(), false, 'audio buffer failures are handled safely');
      assert.equal(retry.w.Seal.sound.play(), false, 'an incomplete audio buffer is never played');
      retry.control[failure] = false;
      assert(await retry.w.Seal.sound.unlock(), 'a later interaction retries an incomplete audio buffer');
      assert(retry.w.Seal.sound.play(), 'buffer recovery permits the next real stamp sound');
      retry.records.sources.at(-1).onended();
    } finally { retry.close(); }
  }

  const buttons = fixture();
  try {
    await buttons.w.Seal.sound.unlock();
    const result = buttons.w.Seal.stampOn(buttons.d.querySelector('#host'), { btn: 'button', stamp: { how: 'button', at: Date.now(), by: 'Seth' } });
    await settle();
    assert.equal(buttons.w.Seal.sound.state().contacts, 0);
    descent(buttons).finish(); await settle();
    assert.equal(buttons.w.Seal.sound.state().contacts, 1, 'button stamps use the same one-tap contact timing');
    await finishStamp(buttons, result);
    buttons.records.sources.at(-1).onended();
    await buttons.w.Seal.stampOn(buttons.d.querySelector('#host'), { btn: 'button', stamp: { how: 'button', at: Date.now(), by: 'Seth' }, still: true });
    assert.equal(buttons.w.Seal.sound.state().contacts, 1, 'restoring existing historical stamps never replays audio');
  } finally { buttons.close(); }

  const timeline = fixture();
  try {
    await timeline.w.Seal.sound.unlock();
    const node = timeline.d.createElement('button');
    node.className = 'tlSt pending';
    node.dataset.tlFace = JSON.stringify({ type: 'laserDone', at: Date.now(), by: 'Seth' });
    node.innerHTML = timeline.w.Seal.svg({ how: 'laserDone', at: Date.now(), by: 'Seth' });
    timeline.d.querySelector('#host').append(node);
    const rect = { left: 310, right: 394, top: 420, bottom: 504, width: 84, height: 84 };
    node.getBoundingClientRect = () => rect; node.getClientRects = () => [rect];
    const result = timeline.w.Seal.press(node); await settle();
    assert(descent(timeline), 'a timeline seal uses the shared wooden descent');
    assert.equal(timeline.w.Seal.sound.state().contacts, 0);
    descent(timeline).finish(); await settle();
    assert.equal(timeline.w.Seal.sound.state().contacts, 1, 'timeline seals share the same one-tap contact sound');
    await finishStamp(timeline, result);
    timeline.records.sources.at(-1).onended();
    assert(!timeline.w.Seal.busy());
  } finally { timeline.close(); }

  for (const options of [{ noAudio: true }, { reduced: true }]) {
    const silent = fixture(options);
    try {
      const sound = silent.w.Seal.sound;
      const unlocked = await sound.unlock();
      if (options.noAudio) assert.equal(unlocked, false, 'browsers without Web Audio complete stamps silently');
      const result = silent.w.Seal.press(silent.seal()); await settle();
      if (descent(silent)) descent(silent).finish();
      await finishStamp(silent, result);
      assert(!silent.w.Seal.busy());
      assert.equal(sound.state().contacts, 0, 'unsupported audio or reduced motion cannot emit an unpaired contact');
    } finally { silent.close(); }
  }
  console.log('PASS: quiet locally generated tap; trusted browser unlock; one sound only on full stamp contact; no delayed, historical or hidden-tab replay; node cleanup; audio refusal/failure never blocks approval');
})().catch(error => { console.error(error); process.exitCode = 1; });
