'use strict';

// Exercise the shipped presentation module with a synthetic monotonic clock.
// These tests establish behavior/lifecycle contracts, not human emotion
// recognition, a physical microphone, or real WebRTC/GPU performance.
const test = require('node:test');
const assert = require('node:assert/strict');
const Expression = require('../../brites-concierge-expression.js');

function fixture({ context = '', id = 'input-1' } = {}) {
  let time = 0;
  const expressions = [], events = [];
  const engine = Expression.create({
    now: () => time,
    onExpression: cue => expressions.push({ at: time, cue }),
    onEvent: event => events.push({ at: time, event })
  });
  engine.beginTurn({ id, context });
  engine.setListening(true);
  engine.tick();
  return {
    engine, expressions, events,
    get time() { return time; },
    get state() { return engine.snapshot(); },
    advance(ms) { time += ms; return engine.tick(); },
    input(level, ms = 0) { time += ms; engine.level({ input: level, output: 0 }); return engine.snapshot(); },
    user(text, extra = {}) { return engine.transcript({ role: 'user', itemId: id, currentTurn: true, text, ...extra }); },
    speech(ms = 1000) {
      engine.level({ input: .55, output: 0 });
      for (let elapsed = 100; elapsed <= ms; elapsed += 100) {
        time += 100;
        engine.level({ input: .55, output: 0 });
      }
    }
  };
}

test('input activity alone provides attention without inventing an emotional diagnosis', () => {
  const h = fixture();
  for (const level of [.05, .2, .8, 1, .4, .01, 0]) {
    h.input(level, 250);
    h.advance(100);
  }
  assert.equal(h.state.listening, true);
  assert.equal(h.state.playing, false);
  assert.equal(h.state.context, 'ordinary');
  assert.equal(h.state.diagnosis, false);
  assert.ok(h.expressions.every(({ cue }) => !cue || cue.kind === 'attentive'));
  assert.equal(h.events.some(({ event }) => event.type === 'phrase'), false,
    'microphone activity cannot start an assistant phrase performance');
});

test('remote audio samples do not change listening into speaking', () => {
  const h = fixture();
  for (let i = 0; i < 15; i++) {
    h.engine.level({ input: .4, output: .9 });
    h.advance(100);
  }
  assert.equal(h.state.playing, false);
  assert.equal(h.state.listening, true);
  assert.equal(h.state.clockMs, 0);
  assert.equal(h.state.currentPhrase, -1);
  assert.ok(h.expressions.every(({ cue }) => !cue || cue.kind === 'attentive'));
});

test('the face reacts to actually available grief text and remains restrained', () => {
  const h = fixture();
  h.speech();
  assert.equal(h.state.context, 'ordinary', 'activity is insufficient to infer grief');
  assert.equal(h.user('I would like a necklace in memory of my mother, who died last year.'), true);
  assert.equal(h.state.context, 'support');
  assert.equal(h.state.cue.kind, 'support');
  for (let i = 0; i < 30; i++) h.advance(100);
  assert.ok(h.expressions.every(({ cue }) => !cue || Number.isFinite(cue.intensity) && cue.intensity >= 0 && cue.intensity <= .5));
  assert.equal(h.expressions.some(({ cue }) => cue?.kind === 'celebrate'), false);
});

test('a repair complaint produces composure rather than matching anger or celebrating', () => {
  const h = fixture();
  h.user('The talk button is broken and I am frustrated.');
  assert.equal(h.state.context, 'repair');
  assert.ok(h.state.cue.intensity <= .45, 'repair constrains amplitude while retaining the current speech act');
  h.advance(300);
  assert.equal(h.expressions.some(({ cue }) => ['celebrate', 'resolve', 'angry'].includes(cue?.kind)), false);
});

test('uncertainty is not represented as confirmed fit or an assent nod', () => {
  const h = fixture();
  h.user('I am not sure whether this chain will fit her.');
  assert.equal(h.state.cue.kind, 'reflect');
  h.speech();
  h.input(0, 400);
  h.advance(200);
  assert.ok(h.expressions.every(({ cue }) => !cue || !['resolve', 'nod', 'agree', 'celebrate'].includes(cue.kind)));
  assert.ok(h.events.every(({ event }) => !['nod', 'agree', 'confirm'].includes(event.type)));
});

test('a quiet conversation context remains across ordinary follow-up text', () => {
  const h = fixture();
  h.user('A memorial for someone who passed away.');
  h.user('She liked bright colors and rabbit designs.');
  assert.equal(h.state.context, 'support');
  assert.notEqual(h.state.cue.kind, 'celebrate');
  assert.ok(h.state.cue.intensity <= .38, 'ordinary follow-up remains restrained in the quiet context');
  h.engine.beginTurn({ id: 'input-2', context: 'Continuing the memorial selection.' });
  h.engine.setListening(true);
  h.engine.transcript({ role: 'user', itemId: 'input-2', currentTurn: true, text: 'Would silver work?' });
  assert.equal(h.state.context, 'support');
});

test('a fresh independent conversation does not inherit an earlier quiet context', () => {
  const h = fixture();
  h.user('A gift in memory of my mother.');
  h.engine.cancel();
  h.engine.beginTurn({ id: 'independent-input' });
  h.engine.setListening(true);
  h.engine.transcript({ role: 'user', itemId: 'independent-input', currentTurn: true, text: 'I am excited about her graduation.' });
  assert.equal(h.state.context, 'ordinary');
  assert.equal(h.state.cue.kind, 'celebrate');
});

test('an explicit change away from memorial clears the quiet context for this conversation', () => {
  const h = fixture();
  h.user('I was choosing a memorial gift.');
  h.user('This is no longer a memorial gift; it is for her graduation.');
  assert.equal(h.state.context, 'ordinary', 'explicit correction supersedes the former context');
  assert.equal(h.state.cue.kind, 'celebrate');
});

test('grief suppresses exuberance when the same incoming text mentions a celebration', () => {
  const h = fixture();
  h.user('It is her anniversary, but she is grieving the loss of her mother.');
  assert.equal(h.state.context, 'support');
  assert.equal(h.state.cue.kind, 'support');
  assert.equal(h.expressions.some(({ cue }) => cue?.kind === 'celebrate'), false);
});

test('negated excitement and celebration do not trigger a celebratory listening face', () => {
  const h = fixture();
  h.user('I am not excited. This is not a celebration.');
  assert.equal(h.state.context, 'ordinary');
  assert.notEqual(h.state.cue.kind, 'celebrate');
  assert.equal(h.expressions.some(({ cue }) => cue?.kind === 'celebrate'), false);
});

test('an ordinary lost account detail is not treated as a bereavement disclosure', () => {
  const h = fixture();
  h.user('I lost my order number. Could you help me find it?');
  assert.notEqual(h.state.context, 'support', 'the word lost does not establish a grief context');
  assert.notEqual(h.state.cue.kind, 'support');
});

test('not for a memorial is an explicit correction rather than a grief disclosure', () => {
  const h = fixture();
  h.user('I was considering a memorial gift.');
  h.user('It is not for a memorial; it is for her graduation.');
  assert.equal(h.state.context, 'ordinary');
  assert.equal(h.state.cue.kind, 'celebrate');
});

test('no one died supersedes an erroneous earlier grief interpretation', () => {
  const h = fixture();
  h.user('Someone died.');
  h.user('No one died. I am excited about her graduation.');
  assert.equal(h.state.context, 'ordinary', 'the shopper explicitly corrected the earlier interpretation');
  assert.equal(h.state.cue.kind, 'celebrate');
});

test('repeated listening state callbacks do not replay an acknowledgement or erase a live semantic cue', () => {
  const h = fixture();
  h.user('I am excited about her graduation.');
  const current = h.state.cue, count = h.expressions.length;
  for (let i = 0; i < 50; i++) h.engine.setListening(true);
  assert.deepEqual(h.state.cue, current, 'a repeated state is not a new conversational event');
  assert.equal(h.expressions.length, count, 'repeated state notifications do not schedule fresh visible feedback');
});

test('late or foreign user transcripts cannot replace the current listening interpretation', () => {
  const h = fixture();
  h.user('Which rabbit necklace comes in silver?');
  const before = h.state, count = h.expressions.length;
  assert.equal(h.user('A terrible memorial.', { itemId: 'old-input' }), false);
  assert.equal(h.user('Happy graduation!', { currentTurn: false }), false);
  assert.deepEqual(h.state, before);
  assert.equal(h.expressions.length, count);
});

test('a listening acknowledgement is finite and settles without an autonomous loop', () => {
  const h = fixture();
  const baseline = h.state.cue.intensity;
  h.speech();
  h.input(0, 400);
  h.advance(200);
  assert.ok(h.state.cue.intensity > baseline, 'sustained speech followed by a gap permits a small receipt signal');
  for (let i = 0; i < 15; i++) h.advance(100);
  assert.equal(h.state.cue.intensity, baseline);
  const after = h.expressions.length;
  for (let i = 0; i < 100; i++) h.advance(100);
  assert.equal(h.expressions.length, after, 'quiet time does not start another nod or expression cycle');
});

test('a short microphone blip is not treated as a sustained turn after a long silence', () => {
  const h = fixture();
  const baseline = h.state.cue.intensity;
  h.input(.6);
  h.input(0, 50);
  for (let i = 0; i < 30; i++) {
    h.advance(100);
    assert.equal(h.state.cue.intensity, baseline,
      'elapsed silence cannot inflate a short sound into a long speaking run');
  }
});

test('a gap suppressed by the acknowledgement cooldown is discarded rather than replayed later', () => {
  const h = fixture();
  const baseline = h.state.cue.intensity;
  h.speech();
  h.input(0, 400);
  h.advance(200);
  assert.ok(h.state.cue.intensity > baseline);
  h.advance(1000);
  assert.equal(h.state.cue.intensity, baseline);
  h.speech(900);
  h.input(0, 400);
  assert.equal(h.state.cue.intensity, baseline, 'closely spaced acknowledgements are suppressed');
  for (let i = 0; i < 40; i++) {
    h.advance(100);
    assert.equal(h.state.cue.intensity, baseline,
      'the old gap is not a current opportunity after the cooldown expires');
  }
});

test('cancelling listening prevents stale content and activity from resurrecting a face', () => {
  const h = fixture();
  h.user('I am excited for her graduation.');
  h.engine.cancel();
  const count = h.expressions.length;
  assert.equal(h.user('Thank you!'), false);
  h.engine.setListening(true);
  h.input(1, 1000);
  h.advance(3000);
  assert.equal(h.state.active, false);
  assert.equal(h.state.cue, null);
  assert.equal(h.expressions.length, count);
});

test('pausing cannot be bypassed by a queued turn start or listening callback', () => {
  const h = fixture();
  h.engine.setPaused(true);
  const count = h.expressions.length;
  assert.equal(h.engine.beginTurn({ id: 'queued-input' }), false);
  h.engine.setListening(true);
  assert.equal(h.engine.transcript({ role: 'user', itemId: 'queued-input', text: 'Congratulations!', currentTurn: true }), false);
  h.engine.level({ input: 1, output: 1 });
  h.advance(1000);
  assert.equal(h.state.cue, null);
  assert.equal(h.expressions.length, count);
  h.engine.setPaused(false);
  assert.equal(h.state.active, false, 'unpausing alone does not revive obsolete work');
  assert.equal(h.engine.beginTurn({ id: 'new-input' }), true);
  h.engine.setListening(true);
  assert.equal(h.state.cue.kind, 'attentive');
});

test('reduced motion cannot be bypassed by a queued turn start or listening callback', () => {
  const h = fixture();
  h.engine.setReducedMotion(true);
  const count = h.expressions.length;
  assert.equal(h.engine.beginTurn({ id: 'queued-input' }), false);
  h.engine.setListening(true);
  h.engine.level({ input: 1, output: 1 });
  h.advance(1000);
  assert.equal(h.state.cue, null);
  assert.equal(h.expressions.length, count);
  h.engine.setReducedMotion(false);
  assert.equal(h.state.active, false);
});

test('the listening layer yields at actual assistant playback and does not run its old receipt signal', () => {
  const h = fixture();
  h.speech();
  h.input(0, 400);
  h.advance(200);
  h.engine.transcript({ role: 'assistant', itemId: 'reply-1', responseId: 'response-1', currentTurn: true, text: 'Let me explain the chain lengths.' });
  assert.equal(h.engine.playback({ playing: true, responseId: 'response-1', currentTurn: true }), true);
  assert.equal(h.state.listening, false);
  h.engine.level({ input: .95, output: .2 });
  h.advance(100);
  assert.equal(h.state.playing, true);
  assert.notEqual(h.state.cue?.kind, 'attentive');
  h.engine.playback({ playing: false, responseId: 'response-1', currentTurn: true });
  assert.equal(h.state.cue, null);
  h.engine.setListening(true);
  assert.equal(h.state.listening, true);
  assert.equal(h.state.playing, false);
});

test('destroyed engines reject every later turn and retain no presentation cue', () => {
  const h = fixture();
  h.engine.destroy();
  const count = h.expressions.length;
  assert.equal(h.engine.beginTurn({ id: 'after-destroy' }), false);
  h.engine.setListening(true);
  assert.equal(h.engine.transcript({ role: 'user', text: 'Happy anniversary!', currentTurn: true }), false);
  h.engine.level({ input: 1, output: 1 });
  h.advance(2000);
  assert.equal(h.state.cue, null);
  assert.equal(h.expressions.length, count);
});
