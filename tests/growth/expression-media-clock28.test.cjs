'use strict';

// Behavior regressions for the shipped presentation planner's local output
// media clock. Synthetic timestamps and RMS are not physical-audio, word
// alignment, browser performance, GPU, or human-trust validation.
const test = require('node:test');
const assert = require('node:assert/strict');
const Expression = require('../../brites-concierge-expression.js');

const TEXT = 'Thank you for explaining. Here is one option. Which detail matters most to you?';

function fixture() {
  let wall = 0;
  const events = [];
  const engine = Expression.create({ now: () => wall, onEvent: event => events.push(event) });
  let inputId = 'input-current', responseId = 'response-current', itemId = 'output-current';
  function begin(id = inputId) {
    inputId = id;
    return engine.beginTurn({ id: inputId, context: 'ordinary' });
  }
  function text(value = TEXT, id = responseId) {
    responseId = id;
    itemId = 'output-' + id;
    return engine.transcript({ role: 'assistant', text: value, inputItemId: inputId,
      responseId, itemId, contentIndex: 0, currentTurn: true });
  }
  function playback(playing = true, extra = {}) {
    return engine.playback({ playing, responseId, inputItemId: inputId, currentTurn: true, ...extra });
  }
  begin();
  return { engine, events, begin, text, playback,
    get state() { return engine.snapshot(); },
    sample(wallDelta, value = {}) {
      wall += wallDelta;
      engine.level({ input: 0, output: 0, ...value });
      return engine.snapshot();
    },
    tick(wallDelta) { wall += wallDelta; return engine.tick(); },
    start() { text(); playback(); }
  };
}

test('the first positive media observation anchors without jumping to its nonzero timestamp', () => {
  const h = fixture(); h.start();
  h.sample(1000, { output: .4, outputTimeMs: 25000 });
  assert.equal(h.state.clockMs, 0);
  assert.equal(h.state.currentPhrase, 0);
  assert.equal(h.state.timing, 'estimated-audio-activity');
  assert.equal(h.state.diagnosis, false);
});

for (const interval of [150, 400, 1000]) {
  test(`adjacent positive output samples admit the full ${interval}ms media interval with sparse callbacks`, () => {
    const h = fixture(); h.start();
    h.sample(1000, { output: .4, outputTimeMs: 25000 });
    for (let i = 1; i <= 3; i++) {
      h.sample(interval, { output: .4, outputTimeMs: 25000 + interval * i });
      assert.equal(h.state.clockMs, interval * i, 'rendering callback caps must not stretch the locally observed media clock');
    }
  });
}

test('wall rendering ticks cannot double count an established output media clock', () => {
  const h = fixture(); h.start();
  h.sample(20, { output: .3, outputTimeMs: 20000 });
  h.sample(20, { output: .3, outputTimeMs: 20150 });
  for (let i = 0; i < 5; i++) h.tick(25);
  assert.equal(h.state.clockMs, 150);
  h.sample(1000, { output: .3, outputTimeMs: 20550 });
  assert.equal(h.state.clockMs, 550, 'media and wall time must not be added together');
});

test('the measured media delta determines progress independently of a longer wall gap', () => {
  const h = fixture(); h.start();
  h.sample(1000, { output: .3, outputTimeMs: 40000 });
  h.sample(1000, { output: .3, outputTimeMs: 40150 });
  assert.equal(h.state.clockMs, 150);
});

test('sparse measured playback reaches the final question and settles after the complete response', () => {
  const h = fixture(); h.start();
  const plan = Expression.plan(TEXT);
  const last = plan.at(-1);
  assert.equal(last.kind, 'inquiry');
  h.sample(1000, { output: .35, outputTimeMs: 12000 });
  let elapsed = 0;
  const questionAt = last.startMs + 150;
  while (elapsed < questionAt) {
    const step = Math.min(1000, questionAt - elapsed);
    elapsed += step;
    h.sample(step, { output: .35, outputTimeMs: 12000 + elapsed });
  }
  assert.equal(h.state.currentPhrase, last.index);
  assert.equal(h.state.cue.kind, 'inquiry');
  assert.ok(h.events.some(event => event.kind === 'inquiry'));
  const finishAt = last.startMs + last.durationMs + 1;
  while (elapsed < finishAt) {
    const step = Math.min(1000, finishAt - elapsed);
    elapsed += step;
    h.sample(step, { output: .35, outputTimeMs: 12000 + elapsed });
  }
  assert.equal(h.state.clockMs, finishAt);
  assert.equal(h.state.cue, null);
});

test('text generation and microphone activity cannot move a response before output playback', () => {
  const h = fixture(); h.text();
  for (let i = 0; i < 5; i++) h.sample(1000, { input: .9, output: 0, outputTimeMs: 1000 * i });
  assert.equal(h.state.clockMs, 0);
  assert.equal(h.state.cue, null);
  assert.equal(h.events.length, 0);
  h.playback();
  h.sample(1000, { output: .4, outputTimeMs: 50000 });
  assert.equal(h.state.clockMs, 0, 'generation-era timestamps must not become a playback anchor');
});

test('positive output values without a buffer-start event do not move the speech clock', () => {
  const h = fixture(); h.text();
  h.sample(500, { output: .8, outputTimeMs: 1000 });
  h.sample(1000, { output: .8, outputTimeMs: 2000 });
  assert.equal(h.state.playing, false);
  assert.equal(h.state.clockMs, 0);
  assert.equal(h.state.currentPhrase, -1);
});

test('a playing buffer with silent output and active microphone cannot run through future phrases', () => {
  const h = fixture(); h.start();
  for (let i = 0; i < 8; i++) h.sample(1000, { input: .9, output: 0, outputTimeMs: 25000 + 1000 * i });
  assert.equal(h.state.clockMs, 0);
  assert.equal(h.state.currentPhrase, 0);
  assert.equal(h.events.some(event => event.kind === 'inquiry'), false);
});

test('silence breaks the adjacent-positive interval instead of counting its elapsed media as speech', () => {
  const h = fixture(); h.start();
  h.sample(10, { output: .4, outputTimeMs: 1000 });
  h.sample(100, { output: .4, outputTimeMs: 1100 });
  h.sample(1000, { output: 0, input: .9, outputTimeMs: 2100 });
  h.sample(1000, { output: 0, input: .9, outputTimeMs: 3100 });
  h.sample(1000, { output: .4, outputTimeMs: 4100 });
  assert.equal(h.state.clockMs, 100, 'the first returned positive sample re-anchors rather than counting silence');
  h.sample(200, { output: .4, outputTimeMs: 4300 });
  assert.equal(h.state.clockMs, 300);
});

test('output at or below the activity threshold does not create a positive media interval', () => {
  const h = fixture(); h.start();
  h.sample(100, { output: .015, outputTimeMs: 1000 });
  h.sample(1000, { output: .015, outputTimeMs: 2000 });
  h.sample(1000, { output: .015, outputTimeMs: 3000 });
  assert.equal(h.state.clockMs, 0);
});

for (const invalid of [NaN, Infinity, -1, 86400001, '2000', null]) {
  test(`an explicit invalid timestamp (${String(invalid)}) cannot add a media jump or wall fallback after measured playback`, () => {
    const h = fixture(); h.start();
    h.sample(100, { output: .4, outputTimeMs: 1000 });
    h.sample(150, { output: .4, outputTimeMs: 1150 });
    h.sample(1000, { output: .4, outputTimeMs: invalid });
    assert.equal(h.state.clockMs, 150, 'an invalid measured sample must not be replaced by synthetic wall progress');
  });
}

test('a backward media observation is ignored without reversing or advancing phrase time', () => {
  const h = fixture(); h.start();
  h.sample(100, { output: .4, outputTimeMs: 1000 });
  h.sample(200, { output: .4, outputTimeMs: 1200 });
  h.sample(1000, { output: .4, outputTimeMs: 900 });
  assert.equal(h.state.clockMs, 200);
});

test('nonfinite media cannot create first-sample progress or initialize a reusable output anchor', () => {
  const h = fixture(); h.start();
  h.sample(1000, { output: .4, outputTimeMs: NaN });
  assert.equal(h.state.clockMs, 0);
  h.sample(1000, { output: .4, outputTimeMs: 10000 });
  assert.equal(h.state.clockMs, 0);
  h.sample(150, { output: .4, outputTimeMs: 10150 });
  assert.equal(h.state.clockMs, 150);
});

test('omitting a timestamp in an established measured series holds instead of double counting wall time', () => {
  const h = fixture(); h.start();
  h.sample(100, { output: .4, outputTimeMs: 1000 });
  h.sample(150, { output: .4, outputTimeMs: 1150 });
  h.sample(1000, { output: .4 });
  h.tick(50);
  assert.equal(h.state.clockMs, 150, 'an interrupted media series must not turn recent output into synthetic media progress');
});

test('a media discontinuity larger than 2000ms is ignored and does not replay the skipped interval', () => {
  const h = fixture(); h.start();
  h.sample(100, { output: .4, outputTimeMs: 1000 });
  h.sample(200, { output: .4, outputTimeMs: 1200 });
  h.sample(1000, { output: .4, outputTimeMs: 3201 });
  assert.equal(h.state.clockMs, 200);
  h.sample(150, { output: .4, outputTimeMs: 3351 });
  assert.ok(h.state.clockMs <= 350, 'resume may re-anchor or accept only the new adjacent interval, never the rejected jump');
});

test('zero media deltas hold and the inclusive 2000ms observed interval is accepted', () => {
  const h = fixture(); h.start();
  h.sample(100, { output: .4, outputTimeMs: 1000 });
  h.sample(1000, { output: .4, outputTimeMs: 1000 });
  assert.equal(h.state.clockMs, 0);
  h.sample(2000, { output: .4, outputTimeMs: 3000 });
  assert.equal(h.state.clockMs, 2000);
});

test('an approved late-load speech offset is preserved while the first media sample only anchors', () => {
  const h = fixture(); h.text(); h.playback(true, { startOffsetMs: 1700 });
  h.sample(1000, { output: .4, outputTimeMs: 50000 });
  assert.equal(h.state.clockMs, 1700);
  h.sample(400, { output: .4, outputTimeMs: 50400 });
  assert.equal(h.state.clockMs, 2100);
});

for (const method of ['cancel', 'setPaused', 'setReducedMotion']) {
  test(`${method} clears the media anchor, and a fresh response cannot inherit its interval`, () => {
    const h = fixture(); h.start();
    h.sample(100, { output: .4, outputTimeMs: 10000 });
    h.sample(400, { output: .4, outputTimeMs: 10400 });
    if (method === 'cancel') h.engine.cancel(); else h.engine[method](true);
    h.sample(1000, { input: .9, output: .9, outputTimeMs: 11400 });
    assert.equal(h.state.clockMs, 0);
    assert.equal(h.state.cue, null);
    assert.equal(h.state.active, false);
    if (method !== 'cancel') h.engine[method](false);
    assert.equal(h.begin('input-next'), true);
    h.text(TEXT, 'response-next'); h.playback();
    h.sample(1000, { output: .4, outputTimeMs: 12000 });
    assert.equal(h.state.clockMs, 0);
    h.sample(150, { output: .4, outputTimeMs: 12150 });
    assert.equal(h.state.clockMs, 150);
  });
}

test('buffer stop and response rollover reset the anchor without reviving the retired response', () => {
  const h = fixture(); h.start();
  h.sample(100, { output: .4, outputTimeMs: 10000 });
  h.sample(400, { output: .4, outputTimeMs: 10400 });
  h.playback(false);
  h.sample(1000, { output: .4, outputTimeMs: 11400 });
  assert.equal(h.state.clockMs, 400);
  assert.equal(h.playback(true), false, 'a stopped response is retired');
  h.text(TEXT, 'response-replacement'); h.playback();
  h.sample(1000, { output: .4, outputTimeMs: 12000 });
  assert.equal(h.state.clockMs, 0);
  h.sample(150, { output: .4, outputTimeMs: 12150 });
  assert.equal(h.state.clockMs, 150);
});

test('beginning a new input turn retires the old speech clock and requires its own positive pair', () => {
  const h = fixture(); h.start();
  h.sample(100, { output: .4, outputTimeMs: 10000 });
  h.sample(1000, { output: .4, outputTimeMs: 11000 });
  h.begin('input-next'); h.text(TEXT, 'response-next'); h.playback();
  h.sample(1000, { output: .4, outputTimeMs: 12000 });
  assert.equal(h.state.clockMs, 0);
  h.sample(400, { output: .4, outputTimeMs: 12400 });
  assert.equal(h.state.clockMs, 400);
});

test('switching into listening holds output progression and a new output response re-anchors', () => {
  const h = fixture(); h.start();
  h.sample(100, { output: .4, outputTimeMs: 10000 });
  h.sample(400, { output: .4, outputTimeMs: 10400 });
  h.engine.setListening(true);
  h.sample(1000, { input: .8, output: 0, outputTimeMs: 11400 });
  assert.equal(h.state.clockMs, 400);
  assert.equal(h.state.playing, false);
  assert.equal(h.state.listening, true);
  h.text(TEXT, 'response-next'); h.playback();
  h.sample(1000, { output: .4, outputTimeMs: 12000 });
  assert.equal(h.state.clockMs, 0);
});

test('missing media timestamps retain a bounded output-activity wall fallback before a measured series exists', () => {
  const h = fixture(); h.start();
  h.sample(1000, { output: .4 });
  assert.equal(h.state.clockMs, 100, 'one sparse unmeasured callback retains the existing 100ms safety bound');
  h.tick(50); h.tick(50); h.tick(50); h.tick(1000);
  const settled = h.state.clockMs;
  assert.ok(settled >= 100 && settled <= 320, 'fallback cannot accumulate a long silent interval');
  for (let i = 0; i < 10; i++) h.sample(1000, { input: .9, output: 0 });
  assert.equal(h.state.clockMs, settled);
});

for (const [label, stale] of [
  ['explicitly stale turn', { currentTurn: false }],
  ['another input item', { inputItemId: 'input-old' }],
  ['another response', { responseId: 'response-old' }]
]) {
  test(`levels from ${label} cannot advance, retarget, or poison the current media anchor`, () => {
    const h = fixture(); h.start();
    h.sample(100, { output: .4, outputTimeMs: 1000 });
    h.sample(150, { output: .4, outputTimeMs: 1150 });
    const before = h.state;
    h.sample(1000, { input: .9, output: .9, outputTimeMs: 60000, ...stale });
    assert.deepEqual(h.state, before);
    h.sample(400, { output: .4, outputTimeMs: 1550, currentTurn: true,
      inputItemId: 'input-current', responseId: 'response-current' });
    assert.equal(h.state.clockMs, 550, 'a stale sample must not replace the last qualified media anchor');
  });
}

test('a retired response cannot supply measured levels to a replacement response in the same input turn', () => {
  const h = fixture(); h.start();
  h.sample(100, { output: .4, outputTimeMs: 1000 });
  h.playback(false);
  h.text(TEXT, 'response-replacement'); h.playback();
  h.sample(100, { output: .4, outputTimeMs: 3000,
    responseId: 'response-replacement', inputItemId: 'input-current', currentTurn: true });
  const before = h.state;
  h.sample(1000, { output: .9, outputTimeMs: 60000,
    responseId: 'response-current', inputItemId: 'input-current', currentTurn: true });
  assert.deepEqual(h.state, before);
  h.sample(400, { output: .4, outputTimeMs: 3400,
    responseId: 'response-replacement', inputItemId: 'input-current', currentTurn: true });
  assert.equal(h.state.clockMs, 400);
});
