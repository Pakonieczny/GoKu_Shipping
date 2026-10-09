'use strict';
const test = require('node:test'), assert = require('node:assert/strict');
const avatar = require('../../brites-concierge-avatar.js');
test('all supported conversational states produce finite rig poses', () => {
  const maximumAperture = {idle: 1, listening: 1.06, thinking: 1, speaking: 1.08, success: 1.02, error: 1};
  for (const state of avatar.STATES) for (const time of [0, .03, 1.2, 3.31, 6, 12, 1500]) for (const level of [0, .5, 1]) {
    const pose = avatar.poseFor({state, time, elapsed: time, level, gaze: {x: .4, y: -.6}});
    assert.equal(pose.state, state); for (const [key, value] of Object.entries(pose)) if (typeof value === 'number') assert.ok(Number.isFinite(value), key);
    assert.ok(pose.eyeOpen >= .035 && pose.eyeOpen <= maximumAperture[state], state + ' exceeds its bounded eye aperture');
    assert.equal(pose.mouthOpen, 0, state + ' keeps a closed curve at every output amplitude'); assert.ok(Math.abs(pose.mouthCurve) < .08);
    if (state === 'speaking') assert.equal(pose.eyeColor, '#70d8f1');
  }
});
test('unknown state falls back to a quiet neutral character', () => {assert.equal(avatar.validState('purchase'), 'idle'); assert.equal(avatar.poseFor({state: 'guaranteed-sales'}).state, 'idle');});
test('listening, thinking, speaking, success and error have distinct facial or body rigs', () => {
  const idle = avatar.poseFor({state: 'idle', time: 2}), listening = avatar.poseFor({state: 'listening', time: 2}), thinking = avatar.poseFor({state: 'thinking', time: 2}), speaking = avatar.poseFor({state: 'speaking', time: 2}), success = avatar.poseFor({state: 'success', time: 2, elapsed: .6}), error = avatar.poseFor({state: 'error', time: 2});
  assert.ok(listening.faceBrowLift > idle.faceBrowLift); assert.ok(thinking.faceBrowLift > idle.faceBrowLift); assert.equal(thinking.faceBrowTilt, 0); assert.equal(thinking.eyeAsymmetry, 0); assert.equal(thinking.gazeY, idle.gazeY, 'thoughtfulness cannot pull the eyes away from the shopper'); assert.ok(thinking.armLift > idle.armLift);
  assert.equal(speaking.mouth, 'smile-curve'); assert.equal(speaking.mouthOpen, 0); assert.equal(speaking.speechEnergy, 0, 'silent output has no emission');
  const audible = avatar.poseFor({state: 'speaking', time: 2, level: .5});
  assert.equal(audible.mouthOpen, 0); assert.equal(audible.mouthCurve, speaking.mouthCurve, 'amplitude does not open or distort the semantic curve'); assert.equal(audible.speechEnergy, .5); assert.equal(audible.speechSignalValid, true); assert.deepEqual(audible.speechBands, [0,0,0,0,0,0], 'amplitude-only fallback cannot invent spectrum');
  assert.ok(speaking.eyeOpen > idle.eyeOpen); assert.ok(success.smileCurve > idle.smileCurve); assert.equal(error.faceExpression, 'reassuring'); assert.equal(error.mouth, 'reflective-curve'); assert.notEqual(error.headRoll, idle.headRoll);
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
