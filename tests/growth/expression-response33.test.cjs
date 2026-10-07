'use strict';

// Real presentation planner with measured-signal fixtures. These assertions do
// not certify physical audio, provider word timing, human feelings or WebGL.
const test = require('node:test');
const assert = require('node:assert/strict');
const Expression = require('../../brites-concierge-expression.js');

function fixture({ listening = true, context = 'ordinary' } = {}) {
  let time = 0;
  const expressions = [], events = [];
  const engine = Expression.create({ now: () => time,
    onExpression: cue => expressions.push(cue && { ...cue }),
    onEvent: event => events.push({ ...event }) });
  engine.beginTurn({ id: 'input-current', turnVersion: 3, context });
  if (listening) engine.setListening(true);
  engine.tick();
  const identity = { inputItemId: 'input-current', turnVersion: 3, currentTurn: true };
  return { engine, expressions, events,
    get state() { return engine.snapshot(); },
    user(text, extra = {}) {
      return engine.transcript({ ...identity, role: 'user', itemId: 'input-current', text, ...extra });
    },
    assistant(text, extra = {}) {
      return engine.transcript({ ...identity, role: 'assistant', responseId: 'response-current',
        itemId: 'output-current', contentIndex: 0, text, ...extra });
    },
    playback(playing = true, extra = {}) {
      return engine.playback({ ...identity, responseId: 'response-current', playing, ...extra });
    },
    tick(ms = 0) { time += ms; return engine.tick(); },
    sample(ms, input = 0, output = 0, extra = {}) {
      time += ms; engine.level({ ...identity, input, output, ...extra }); return engine.snapshot();
    }
  };
}

test('a concise product result, qualified story and choice question have distinct face acts', () => {
  const plan = Expression.plan('Here are six silver earrings. A bunny may be a personal reminder. Which pair would you like?');
  assert.deepEqual(plan.map(cue => cue.kind), ['resolve', 'reflect', 'inquiry']);
  assert.ok(plan.every(cue => cue.intensity >= .6 && cue.intensity <= .8));
});

test('an unsuccessful suggestion and an actual recovery do not retain a broad smile', () => {
  const plan = Expression.plan("I couldn't find that exact finish. Let's try silver earrings. Which shape do you prefer?");
  assert.deepEqual(plan.map(cue => cue.kind), ['reflect', 'resolve', 'inquiry']);
  for (const text of ['Not those earrings.', 'No, earrings instead.', "I don't like these."]) {
    const h = fixture(); h.user(text);
    assert.equal(h.state.cue.kind, 'reflect', text);
    assert.equal(h.state.diagnosis, false);
  }
});

test('an explicit positive selection gets warmth, while negated approval does not', () => {
  const h = fixture(); h.user('I love these earrings.');
  assert.equal(h.state.cue.kind, 'appreciate');
  h.user("I don't love these earrings.");
  assert.notEqual(h.state.cue.kind, 'appreciate');
  assert.notEqual(h.state.cue.kind, 'celebrate');
});

test('incoming measured speech visibly raises attention before transcription is available', () => {
  const h = fixture(), baseline = h.state.cue.intensity;
  h.sample(60, .6); h.sample(60, .6); h.sample(60, .6);
  assert.equal(h.state.cue.kind, 'attentive');
  assert.ok(h.state.cue.intensity >= baseline + .08);
  assert.equal(h.state.playing, false);
  assert.equal(h.state.clockMs, 0);
  assert.equal(h.state.context, 'ordinary');
  assert.equal(h.state.diagnosis, false);
  assert.equal(h.events.some(event => event.type === 'phrase'), false);
  h.sample(400, 0);
  assert.equal(h.state.cue.intensity, baseline, 'a stopped short input cannot retain a fake pulse');
});

test('a short microphone blip and unobserved elapsed time cannot fabricate listener activity', () => {
  const h = fixture(), baseline = h.state.cue;
  h.sample(10, 1); h.sample(40, 0);
  for (let i = 0; i < 20; i++) {
    h.tick(100);
    assert.deepEqual(h.state.cue, baseline);
  }
});

test('a semantic listener reaction survives one clause instead of disappearing after a second', () => {
  const h = fixture(); h.user('I am not sure whether these are right.');
  const strength = h.state.cue.intensity;
  h.tick(1300);
  assert.equal(h.state.cue.kind, 'reflect');
  assert.equal(h.state.cue.intensity, strength);
  h.tick(800);
  assert.equal(h.state.cue.kind, 'reflect');
  assert.ok(h.state.cue.intensity < strength);
  h.tick(400);
  assert.equal(h.state.cue.kind, 'attentive');
  const settled = h.expressions.length;
  for (let i = 0; i < 20; i++) h.tick(1000);
  assert.equal(h.expressions.length, settled, 'quiet time never creates an autonomous mood cycle');
});

test('current final transcription arriving after speech stopped gets a finite waiting reaction', () => {
  const h = fixture(); h.engine.setListening(false);
  h.user('Not those earrings.');
  assert.equal(h.state.listening, false);
  assert.equal(h.state.playing, false);
  assert.equal(h.state.cue.kind, 'reflect');
  h.tick(1000);
  assert.equal(h.state.cue.kind, 'reflect');
  assert.equal(h.state.clockMs, 0);
  h.tick(1600);
  assert.equal(h.state.cue, null, 'finished listening cannot create ongoing semantic feedback');
});

test('duplicate final callbacks cannot restart the reaction envelope or extend its lifetime', () => {
  const h = fixture(); h.engine.setListening(false);
  const text = 'Not those earrings.';
  h.user(text); h.tick(1800);
  const before = h.state.cue, count = h.expressions.length;
  h.user(text, { final: true });
  assert.deepEqual(h.state.cue, before);
  assert.equal(h.expressions.length, count);
  h.tick(650);
  assert.equal(h.state.cue, null);
});

test('new partial words can sustain the same act without restarting its visible onset', () => {
  const h = fixture(); h.user('I am not sure.');
  const count = h.expressions.length;
  h.tick(1000);
  h.engine.transcript({ role: 'user', itemId: 'input-current', turnVersion: 3,
    currentTurn: true, delta: ' I want a shorter chain.' });
  assert.equal(h.state.cue.kind, 'reflect');
  assert.equal(h.expressions.length, count, 'same-intent text does not replay a fresh target');
  h.tick(1000);
  assert.equal(h.state.cue.kind, 'reflect');
});

test('listener energy preserves restrained support and repair limits', () => {
  for (const [context, text, limit] of [
    ['support', 'A memorial for my sister.', .38],
    ['repair', 'The talk button is broken.', .45]
  ]) {
    const h = fixture({ context }); h.user(text);
    for (let i = 0; i < 30; i++) {
      h.sample(60, 1);
      assert.ok(h.state.cue.intensity <= limit);
      assert.notEqual(h.state.cue.kind, 'celebrate');
    }
  }
});

test('semantic cues yield to actual output and do not leak into an unrelated continuation', () => {
  const h = fixture(); h.user('Not those earrings.');
  h.assistant('Here are six silver earrings.'); h.playback();
  h.sample(50, 1, .3);
  assert.equal(h.state.cue.kind, 'resolve');
  h.playback(false);
  h.tick(500);
  assert.equal(h.state.cue, null);
  h.engine.setListening(true);
  h.tick();
  assert.equal(h.state.cue.kind, 'attentive');
});

test('audible speaking faces retain useful contrast at clause boundaries and during quieter voice', () => {
  const h = fixture({ listening: false });
  h.assistant('Here are six earrings. Which pair do you prefer?'); h.playback();
  h.sample(20, 0, .04, { outputTimeMs: 2000 });
  assert.equal(h.state.cue.kind, 'resolve');
  assert.ok(h.state.cue.intensity >= .25, 'an audible clause onset is visibly coordinated, rather than nearly neutral');
  h.sample(380, 0, .04, { outputTimeMs: 2380 });
  assert.ok(h.state.cue.intensity > .6);
  const question = Expression.plan('Here are six earrings. Which pair do you prefer?')[1];
  let at = 2380;
  while (h.state.clockMs < question.startMs + 380) {
    const step = Math.min(200, question.startMs + 380 - h.state.clockMs);
    at += step; h.sample(step, 0, .04, { outputTimeMs: at });
  }
  assert.equal(h.state.cue.kind, 'inquiry');
  assert.ok(h.state.cue.intensity > .7);
});

test('output buffering and microphone energy alone do not animate future speaking intentions', () => {
  const h = fixture({ listening: false });
  h.assistant('Congratulations! Which pair do you prefer?'); h.playback();
  for (let i = 0; i < 20; i++) h.sample(100, 1, 0, { outputTimeMs: 5000 + 100 * i });
  assert.equal(h.state.clockMs, 0);
  assert.equal(h.state.currentPhrase, 0);
  assert.ok(h.state.cue.intensity <= .12);
  assert.equal(h.events.some(event => event.kind === 'inquiry'), false);
});

test('stale item, version and current-turn flags cannot replace listener meaning or activity', () => {
  const h = fixture(); h.user('I love these earrings.');
  const before = h.state;
  for (const extra of [{ itemId: 'input-old' }, { turnVersion: 2 }, { currentTurn: false }]) {
    assert.equal(h.user('A memorial for my mother.', extra), false);
    assert.deepEqual(h.state, before);
  }
  assert.equal(h.engine.level({ input: 1, output: 0, inputItemId: 'input-old', turnVersion: 3, currentTurn: true }), false);
  assert.deepEqual(h.state, before);
});

for (const boundary of ['cancel', 'setPaused', 'setReducedMotion', 'destroy']) {
  test(`${boundary} clears active listener input and semantic feedback without resurrection`, () => {
    const h = fixture(); h.user('Not those earrings.');
    h.sample(60, .7); h.sample(60, .7); h.sample(60, .7);
    h.engine[boundary](true);
    const count = h.expressions.length;
    h.user('I love these earrings.'); h.sample(1000, 1, 1); h.tick(5000);
    assert.equal(h.state.active, false);
    assert.equal(h.state.cue, null);
    assert.equal(h.expressions.length, count);
  });
}
