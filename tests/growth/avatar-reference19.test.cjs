'use strict';

// CPU/source checks only. Reference recordings informed timing hypotheses; this
// file is not evidence of GPU rendering, visible shadow quality or frame rate.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const avatar = require('../../brites-concierge-avatar.js');

const scene = fs.readFileSync(require.resolve('../../brites-concierge-avatar-scene.mjs'), 'utf8');
const controller = fs.readFileSync(require.resolve('../../brites-concierge-avatar.js'), 'utf8');
const bundledScene = fs.readFileSync(require.resolve('../../assets/brites-concierge-avatar-scene.mjs'), 'utf8');

test('rare irregular blinks close softly and do not become a looping attention device', () => {
  assert.equal(avatar.BLINK_EVENTS.length, 3);
  const starts = avatar.BLINK_EVENTS.map(event => event.at);
  assert.ok(starts[1] - starts[0] > 8);
  assert.ok(starts[2] - starts[1] > 8);
  for (const event of avatar.BLINK_EVENTS) {
    assert.ok(event.duration >= .18 && event.duration <= .22);
    assert.equal(avatar.blinkFor(event.at - .01), 0);
    assert.ok(avatar.blinkFor(event.at + event.duration / 2) > .99);
    assert.equal(avatar.blinkFor(event.at + event.duration + .01), 0);
  }
  assert.equal(avatar.blinkFor(avatar.BLINK_EVENTS[0].at + .1, true), 0);
  assert.equal(avatar.blinkFor(NaN), 0);
});

test('conversation modes retain distinct graphic expression controls and restrained output energy', () => {
  const listening = avatar.poseFor({state: 'listening', time: 5, mannerism: 'acknowledge', mannerismElapsed: .42});
  const thinking = avatar.poseFor({state: 'thinking', time: 5, mannerism: 'focus', mannerismElapsed: .42});
  const speaking = avatar.poseFor({state: 'speaking', time: 5, level: 1, mannerism: 'explain', mannerismElapsed: .42});
  assert.equal(listening.eyeColor, '#49c9ff');
  assert.equal(thinking.eyeColor, '#ab87ff');
  assert.equal(speaking.eyeColor, '#ffcb79');
  assert.equal(listening.faceExpression, 'attentive');
  assert.equal(thinking.faceExpression, 'curious');
  assert.equal(speaking.faceExpression, 'explaining');
  assert.ok(listening.faceBrowLift > thinking.faceBrowLift);
  assert.notEqual(thinking.faceBrowTilt, speaking.faceBrowTilt);
  assert.ok(speaking.eyeSmile > thinking.eyeSmile);
  assert.equal(listening.speechEnergy, 0); assert.equal(thinking.speechEnergy, 0); assert.equal(speaking.speechEnergy, 1);
  assert.ok(Math.abs(listening.gazeX) < .02);
  assert.ok(Math.abs(thinking.gazeX) < .06);
});

test('state transition easing and filled ribbon identity stay original and bounded', () => {
  assert.match(scene, /authorship: 'original authored choreography'/);
  assert.match(scene, /transitionMs: 320/);
  assert.match(scene, /colorEase = colorProgress \* colorProgress \* \(3 - 2 \* colorProgress\)/);
  assert.match(scene, /speechShape = reducedMotion \? 0/);
  assert.match(scene, /identity: 'original pearlfin porcelain guide'/);
  assert.match(scene, /two deformable geometric light ribbons/);
  assert.doesNotMatch(scene, /const halo =|const innerHalo =/);
  assert.match(scene, /no human iris or anatomical mouth/);
  assert.match(bundledScene, /original authored choreography/);
  assert.match(bundledScene, /original pearlfin porcelain guide/);
  assert.match(bundledScene, /rare irregular 0\.19-0\.21 second closure/);
  assert.doesNotMatch((scene + controller).toLowerCase(), /wall[- ]?e|disney/);
});

test('reduced motion preserves state meaning while eliminating timed choreography', () => {
  for (const state of avatar.STATES) {
    const first = avatar.poseFor({state, time: 1, reducedMotion: true, level: .8});
    const later = avatar.poseFor({state, time: 101, reducedMotion: true, level: .8});
    assert.deepEqual(later, first);
    assert.equal(first.blink, 0);
  }
});
