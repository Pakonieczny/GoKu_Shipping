'use strict';
const test = require('node:test'), assert = require('node:assert/strict');
const avatar = require('../../brites-concierge-avatar.js');
test('all supported conversational states produce finite rig poses', () => {
  for (const state of avatar.STATES) for (const time of [0, .03, 1.2, 3.31, 6, 12, 1500]) {
    const pose = avatar.poseFor({state, time, elapsed: time, level: .5, gaze: {x: .4, y: -.6}});
    assert.equal(pose.state, state); for (const [key, value] of Object.entries(pose)) if (typeof value === 'number') assert.ok(Number.isFinite(value), key);
    assert.ok(pose.eyeOpen >= .035 && pose.eyeOpen <= 1.06); assert.ok(pose.mouthOpen >= 0 && pose.mouthOpen <= 1);
  }
});
test('unknown state falls back to a quiet neutral character', () => {assert.equal(avatar.validState('purchase'), 'idle'); assert.equal(avatar.poseFor({state: 'guaranteed-sales'}).state, 'idle');});
test('listening, thinking, speaking, success and error have distinct facial or body rigs', () => {
  const idle = avatar.poseFor({state: 'idle', time: 2}), listening = avatar.poseFor({state: 'listening', time: 2}), thinking = avatar.poseFor({state: 'thinking', time: 2}), speaking = avatar.poseFor({state: 'speaking', time: 2}), success = avatar.poseFor({state: 'success', time: 2, elapsed: .6}), error = avatar.poseFor({state: 'error', time: 2});
  assert.ok(listening.browLift > idle.browLift); assert.notEqual(thinking.gazeY, idle.gazeY); assert.ok(thinking.armLift > idle.armLift); assert.equal(speaking.mouth, 'open'); assert.ok(speaking.mouthOpen > 0); assert.ok(success.bob > idle.bob); assert.equal(error.mouth, 'concern'); assert.notEqual(error.headRoll, idle.headRoll);
});
test('reduced motion removes time-varying float, blink, speech pulse and light flicker while retaining the chosen expression', () => {
  for (const state of avatar.STATES) {const start = avatar.poseFor({state, time: 0, elapsed: 0, reducedMotion: true}), later = avatar.poseFor({state, time: 80, elapsed: 80, reducedMotion: true}); assert.deepEqual(later, start); assert.equal(start.bob, 0); assert.equal(start.bodyRoll, 0); assert.equal(start.gesture, 0);}
});
test('gaze and speech amplitude are bounded, and malformed input does not produce a broken mesh', () => {
  for (const gaze of [{x: 999, y: -999}, {x: NaN, y: Infinity}, null]) {const pose = avatar.poseFor({state: 'speaking', time: NaN, level: Infinity, gaze}); for (const value of Object.values(pose)) if (typeof value === 'number') assert.ok(Number.isFinite(value)); assert.ok(Math.abs(pose.headYaw) <= .1); assert.ok(Math.abs(pose.headPitch) <= .09);}
});
test('desktop rendering uses 2K detail while adaptive mobile reduces staging memory', () => {
  const desktop = avatar.qualityFor({width: 1440, memory: 8, pixelRatio: 3}), mobile = avatar.qualityFor({width: 390, memory: 4, pixelRatio: 3});
  assert.equal(desktop.name, 'high'); assert.equal(desktop.shadowSize, 2048); assert.equal(desktop.textureSize, 2048); assert.equal(desktop.pixelRatio, 2); assert.equal(desktop.fps, 60); assert.equal(desktop.bloom, true); assert.equal(mobile.name, 'adaptive'); assert.equal(mobile.textureSize, 1024); assert.equal(mobile.pixelRatio, 1.5); assert.equal(mobile.shadowSize, 1024); assert.equal(mobile.fps, 30); assert.equal(mobile.bloom, false);
});
test('explicit bloom disable does not silently remove detailed materials or shadows', () => {const quality = avatar.qualityFor({width: 1440, bloom: false}); assert.equal(quality.bloom, false); assert.equal(quality.geometryScale, 1); assert.equal(quality.textureSize, 2048); assert.equal(quality.shadowSize, 2048);});
