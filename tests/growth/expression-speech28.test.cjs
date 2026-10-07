'use strict';

// Exercise the shipped phrase driver with a synthetic clock and closed native
// event identities. These are behavior regressions, not word-alignment,
// physical-audio, human-trust, or GPU validation.
const test = require('node:test');
const assert = require('node:assert/strict');
const Expression = require('../../brites-concierge-expression.js');

function fixture({ id = 'input-current', context = 'ordinary' } = {}) {
  let time = 0;
  const expressions = [], events = [];
  const engine = Expression.create({
    now: () => time,
    onExpression: cue => expressions.push({ at: time, cue }),
    onEvent: event => events.push({ at: time, event })
  });
  engine.beginTurn({ id, context });
  return {
    engine, expressions, events,
    get state() { return engine.snapshot(); },
    advance(ms, output = 0, input = 0) {
      for (let remaining = ms; remaining > 0;) {
        const step = Math.min(50, remaining);
        time += step;
        remaining -= step;
        engine.level({ output, input });
      }
      return engine.snapshot();
    },
    text(text, extra = {}) {
      return engine.transcript({
        role: 'assistant', responseId: 'response-current', itemId: 'output-current',
        inputItemId: id, contentIndex: 0, currentTurn: true, text, ...extra
      });
    },
    playback(playing, extra = {}) {
      return engine.playback({ responseId: 'response-current', currentTurn: true, playing, ...extra });
    }
  };
}

const kinds = text => Expression.plan(text).map(cue => cue.kind);

test('qualified product meaning changes from reflection to an actual invitation for the shopper', () => {
  // Exact public QA sentence fixture; no assertion of a universal symbolic meaning.
  const text = 'I cannot say it has one universal meaning. You might choose a personal reminder. What would you like it to represent?';
  const expected = ['reflect', 'reflect', 'inquiry'];
  assert.deepEqual(kinds(text), expected);
  const h = fixture();
  h.text(text);
  h.playback(true);
  const duration = Expression.plan(text).reduce((sum, cue) => sum + cue.durationMs, 0);
  h.advance(duration + 200, .3);
  assert.deepEqual(h.events.map(({ event }) => event.kind), expected, 'qualified phrases and the final question must all reach the real driver');
  assert.equal(h.state.cue, null);
});

test('explicit possibility and inability to verify retain reflection instead of confident explanation', () => {
  for (const text of [
    'I cannot say this has one universal meaning.',
    "I can't say this has one universal meaning.",
    'I can’t say this has one universal meaning.',
    'A familiar symbol may feel comforting.',
    'A small symbol may be a personal reminder.'
  ]) assert.deepEqual(kinds(text), ['reflect'], text);
  assert.deepEqual(kinds('The finish changes the look of a piece. A small symbol may be a personal reminder. Which style feels right?'), ['explain', 'reflect', 'inquiry']);
});

test('an explicit information request remains a question when its subject contains possible meaning', () => {
  for (const text of [
    'What could this symbol mean to your friend?',
    'Could it mean a personal reminder for her?',
    'Would you like a symbol that may feel comforting?',
    'What might it represent for you?'
  ]) assert.deepEqual(kinds(text), ['inquiry'], text);
  assert.deepEqual(kinds('It could mean a personal reminder.'), ['reflect'], 'a qualified statement remains reflective rather than becoming a question');
});

test('different communicative clauses produce a bounded speech trajectory rather than one reply-wide emotion', () => {
  const text = "I'm sorry that was frustrating. I cannot confirm the fit from that detail. Here is the shorter option. Which would you prefer? Thank you for explaining.";
  assert.deepEqual(kinds(text), ['support', 'reflect', 'resolve', 'inquiry', 'appreciate']);
  const h = fixture();
  h.text(text);
  h.playback(true);
  const duration = Expression.plan(text).reduce((sum, cue) => sum + cue.durationMs, 0);
  h.advance(duration + 200, .35);
  assert.deepEqual(h.events.map(({ event }) => event.kind), ['support', 'reflect', 'resolve', 'inquiry', 'appreciate']);
  assert.equal(h.state.cue, null, 'a completed trajectory does not begin an autonomous expression loop');
  assert.ok(h.expressions.every(({ cue }) => !cue || Expression.KINDS.includes(cue.kind) && cue.intensity >= 0 && cue.intensity <= 1));
});

test('restrained context preserves uncertainty, questioning, gratitude, and resolution as distinct speech acts', () => {
  const cues = Expression.plan('I am sorry. I cannot confirm that. Here is an option. Which one feels right? Thank you for explaining.', { context: 'support' });
  assert.deepEqual(cues.map(cue => cue.kind), ['support', 'reflect', 'resolve', 'inquiry', 'appreciate']);
  assert.ok(cues.every(cue => cue.intensity <= .38));
  assert.equal(cues.some(cue => cue.kind === 'celebrate'), false);
});

test('decimals remain inside their semantic phrase and do not inflate word counts or timings', () => {
  const text = 'The pendant costs $29.99 and the chain is 0.5 mm thick.';
  const cues = Expression.plan(text);
  assert.equal(cues.length, 1);
  assert.equal(cues[0].wordCount, text.split(/\s+/).length);
  assert.equal(cues[0].kind, 'explain');
  assert.equal(Expression.plan('The price is $29.99. Which color would you like?').length, 2);
});

test('sentence separation does not require capitalized transcripts', () => {
  const cues = Expression.plan('the shorter chain sits higher. the longer chain gives more room. which would you prefer?');
  assert.equal(cues.length, 3);
  assert.deepEqual(cues.map(cue => cue.kind), ['explain', 'explain', 'inquiry']);
});

test('explicit negation prevents celebration for short and long occasion clauses', () => {
  for (const text of [
    "I don't want an anniversary gift.",
    "It isn't a graduation gift.",
    'That is not wonderful news.',
    'We should not celebrate yet.',
    'Please do not choose a necklace for graduation because this is a quiet personal gift and nothing celebratory.',
    'This is not an anniversary gift for my friend and it should remain a quiet personal choice without any celebration.'
  ]) {
    assert.equal(kinds(text).includes('celebrate'), false, text);
  }
});

test('a negated earlier alternative does not suppress an affirmed celebration in a later clause', () => {
  const cues = Expression.plan('Not the gold option, but congratulations on her graduation!');
  assert.equal(cues.at(-1).kind, 'celebrate');
  assert.equal(kinds('Happy birthday!')[0], 'celebrate');
});

test('explicit sarcastic complaint is restrained instead of displaying bright appreciation', () => {
  for (const text of ['Thank you for nothing.', 'Yeah right, that is supposed to help.', 'Are you kidding?']) {
    const cues = Expression.plan(text);
    assert.equal(cues.some(cue => cue.kind === 'appreciate' || cue.kind === 'celebrate'), false, text);
    assert.ok(cues.every(cue => cue.intensity <= .45), text);
  }
  assert.equal(kinds('Thank you for the clear explanation.')[0], 'appreciate', 'ordinary thanks remain appropriate');
});

test('real bereavement survives a different clause denying a memorial product label', () => {
  assert.equal(Expression.contextFor('Not a memorial necklace. My mother died yesterday.', 'ordinary'), 'support');
  assert.equal(Expression.contextFor('It is her graduation, but she is grieving the loss of her mother.', 'ordinary'), 'support');
  assert.equal(Expression.contextFor('No one died. This is not a memorial.', 'ordinary'), 'ordinary');
});

test('commercial loss, explicit topic correction, and repaired failures do not retain a grief face', () => {
  for (const text of ['I lost my cart and it keeps emptying.', 'Does this prevent loss of shine?', 'I lost my order number.']) {
    assert.notEqual(Expression.contextFor(text, 'ordinary'), 'support', text);
  }
  assert.equal(Expression.contextFor('Not a memorial. It is a birthday gift.', 'support'), 'ordinary');
  assert.equal(Expression.contextFor('It is not broken anymore. Thank you, that fixed it.', 'repair'), 'ordinary');
});

test('streaming growth cannot repartition an already active 14-word cue into 9 words', () => {
  const h = fixture();
  const partial = 'It is a wonderful anniversary gift for your friend and I can help choose';
  assert.equal(partial.split(/\s+/).length, 14);
  h.text(partial);
  h.playback(true);
  h.advance(3000, .3);
  const before = h.state;
  assert.equal(before.currentPhrase, 0);
  assert.equal(before.cue.kind, 'celebrate');
  assert.equal(h.text(partial + ' one'), true);
  h.engine.tick();
  assert.equal(h.state.clockMs, before.clockMs);
  assert.equal(h.state.currentPhrase, before.currentPhrase);
  assert.deepEqual(h.state.cue, before.cue, 'a text delta must not switch a currently playing expression');
  assert.equal(h.state.transitions, before.transitions);
});

test('late text cannot change an active neutral explanation before its appended occasion is spoken', () => {
  const h = fixture();
  const partial = 'I can help you choose';
  h.text(partial);
  h.playback(true);
  h.advance(500, .3);
  assert.equal(h.state.cue.kind, 'explain');
  const before = h.state;
  h.text(partial + ' a thoughtful anniversary gift.');
  h.engine.tick();
  assert.equal(h.state.currentPhrase, before.currentPhrase);
  assert.equal(h.state.cue.kind, 'explain');
  assert.equal(h.state.clockMs, before.clockMs);
  h.advance(1300, .3);
  assert.equal(h.state.cue.kind, 'celebrate', 'the appended occasion belongs to its future phrase');
});

test('generated text and microphone RMS cannot run the assistant speech clock before playback', () => {
  const h = fixture();
  h.text('Congratulations! Which necklace would you like?');
  const before = h.expressions.length;
  h.advance(5000, 0, .9);
  assert.equal(h.state.playing, false);
  assert.equal(h.state.clockMs, 0);
  assert.equal(h.state.currentPhrase, -1);
  assert.equal(h.state.cue, null);
  assert.equal(h.events.length, 0);
  assert.equal(h.expressions.length, before);
});

test('a buffer-start event without measured output does not advance phrase time', () => {
  const h = fixture();
  h.text('Let me explain the options. Which one would you like?');
  h.playback(true);
  h.advance(3000, 0, .8);
  assert.equal(h.state.clockMs, 0);
  assert.equal(h.state.currentPhrase, 0);
  assert.ok(h.state.cue.intensity <= .12, 'silent preparation remains restrained');
  assert.equal(h.events.length, 1, 'silence cannot run through future expressions');
  h.advance(500, .3);
  assert.ok(h.state.clockMs > 0);
});

test('after measured output stops the phrase clock settles and freezes despite microphone activity', () => {
  const h = fixture();
  h.text('The first choice is a short chain. The second choice has a longer chain. Which would feel comfortable?');
  h.playback(true);
  h.advance(1000, .4);
  h.advance(400, 0, .9);
  const settled = h.state;
  h.advance(3000, 0, .9);
  assert.equal(h.state.clockMs, settled.clockMs);
  assert.equal(h.state.currentPhrase, settled.currentPhrase);
  assert.equal(h.state.transitions, settled.transitions);
  h.advance(500, .4, 0);
  assert.ok(h.state.clockMs > settled.clockMs);
});

test('large text and repeated deltas retain bounded cue counts and finite durations', () => {
  const h = fixture();
  const long = 'Which necklace would you prefer? '.repeat(1000);
  const cues = Expression.plan(long);
  assert.ok(cues.length > 0 && cues.length <= 24);
  assert.ok(cues.every(cue => Number.isFinite(cue.startMs) && Number.isFinite(cue.durationMs) && cue.durationMs > 0 && cue.durationMs <= 5000));
  h.text(long);
  h.playback(true);
  h.advance(400, .3);
  for (let i = 0; i < 100; i++) {
    h.engine.transcript({ role: 'assistant', responseId: 'response-current', itemId: 'output-current', inputItemId: 'input-current', contentIndex: 0, currentTurn: true, delta: long });
    assert.ok(h.state.phraseCount <= 24);
  }
  assert.ok(h.state.clockMs <= 500);
  assert.equal(h.state.diagnosis, false);
});

test('a stopped response cannot restart or replace the expression of a valid continuation', () => {
  const h = fixture();
  h.text('The first explanation.');
  h.playback(true);
  h.advance(500, .3);
  h.playback(false);
  assert.equal(h.state.cue, null);
  h.text('Which option would you like?', { responseId: 'response-tail', itemId: 'output-tail' });
  h.playback(true, { responseId: 'response-tail' });
  h.advance(500, .3);
  const before = h.state;
  assert.equal(h.text('Congratulations!', { responseId: 'response-current', itemId: 'output-current' }), false);
  assert.equal(h.playback(true), false);
  assert.equal(h.playback(false), false);
  assert.deepEqual(h.state, before);
});

test('new turn identity and native current-turn guards reject old transcripts and playback events', () => {
  const h = fixture();
  h.text('Old answer.');
  h.playback(true);
  h.advance(500, .3);
  h.engine.beginTurn({ id: 'input-next', context: 'ordinary' });
  const before = h.state;
  assert.equal(h.text('Old answer continued.'), false, 'prior input binding is retired');
  assert.equal(h.engine.transcript({ role: 'assistant', text: 'Forged old answer.', responseId: 'unseen-old-response', inputItemId: 'input-current', currentTurn: true }), false);
  assert.equal(h.playback(true), false, 'previous response identity is retired');
  assert.equal(h.engine.playback({ playing: true, responseId: 'unseen-old-response', currentTurn: false }), false);
  assert.deepEqual(h.state, before);
});

test('one output item or content part cannot contaminate another part of the current phrase trajectory', () => {
  const h = fixture();
  h.text('Here is the first choice.');
  h.playback(true);
  h.advance(500, .3);
  const before = h.state;
  assert.equal(h.text('A different output item.', { itemId: 'foreign-output' }), false);
  assert.equal(h.text('A different content part.', { contentIndex: 1 }), false);
  assert.equal(h.text('A stale response.', { currentTurn: false }), false);
  assert.deepEqual(h.state, before);
});

test('pause invalidates the trajectory and unpausing cannot resume obsolete expressions', () => {
  const h = fixture();
  h.text('Congratulations! Which necklace would you like?');
  h.playback(true);
  h.advance(700, .3);
  h.engine.setPaused(true);
  const count = h.expressions.length;
  h.advance(3000, .9, .9);
  assert.equal(h.state.cue, null);
  assert.equal(h.state.active, false);
  assert.equal(h.engine.beginTurn({ id: 'queued-turn' }), false);
  assert.equal(h.text('Queued words.'), false);
  assert.equal(h.playback(true), false);
  h.engine.setPaused(false);
  h.advance(500, .9);
  assert.equal(h.state.active, false);
  assert.equal(h.state.cue, null);
  assert.equal(h.expressions.length, count);
  assert.equal(h.engine.beginTurn({ id: 'fresh-turn' }), true);
});

test('buffer clearing and destruction prevent obsolete speech from resurrecting facial output', () => {
  const h = fixture();
  h.text('A thoughtful choice.');
  h.playback(true);
  h.advance(500, .3);
  h.playback(false, { cleared: true });
  assert.equal(h.state.active, false);
  assert.equal(h.state.cue, null);
  assert.equal(h.text('A late continuation.'), false);
  assert.equal(h.playback(true), false);
  h.engine.destroy();
  assert.equal(h.engine.beginTurn({ id: 'after-destroy' }), false);
  h.advance(1000, .9);
  assert.equal(h.state.cue, null);
});
