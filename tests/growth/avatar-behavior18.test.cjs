'use strict';

// CPU/source and synthetic-DOM checks only. They do not create a WebGL
// context and are not evidence of GPU rendering, shadow appearance or FPS.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const {JSDOM} = require('jsdom');
const THREE = require('three');
const avatar = require('../../brites-concierge-avatar.js');

const sceneSource = fs.readFileSync(require.resolve('../../brites-concierge-avatar-scene.mjs'), 'utf8');

test('conversation transitions have explicit original-robot behavior cues', () => {
  assert.deepEqual(avatar.BEHAVIOR_CUES, {
    greet: 'anticipation', acknowledge: 'listening', focus: 'thinking',
    explain: 'speaking', confirm: 'celebrate', reassure: 'reassure'
  });
  assert.match(sceneSource, /identity: 'original single-eye pebble robot'/);
  assert.match(sceneSource, /no human iris or mouth/);
});

test('each cue anticipates, expresses and settles on its finite deadline', () => {
  for (const [name, duration] of Object.entries(avatar.MANNERISMS)) {
    const early = avatar.mannerismFor({name, elapsed: .06});
    const middle = avatar.mannerismFor({name, elapsed: duration * .42});
    const late = avatar.mannerismFor({name, elapsed: duration * .9});
    const settled = avatar.mannerismFor({name, elapsed: duration});
    assert.equal(early.cue, avatar.BEHAVIOR_CUES[name]);
    assert.equal(early.phase, 'anticipate');
    assert.ok(early.anticipate > 0);
    assert.equal(early.head, 0);
    assert.equal(early.body, 0);
    assert.equal(middle.phase, 'express');
    assert.equal(late.phase, 'settle');
    assert.equal(settled.active, false);
    assert.equal(settled.cue, null);
    for (const pose of [early, middle, late, settled]) {
      for (const [key, value] of Object.entries(pose)) if (typeof value === 'number') assert.ok(Number.isFinite(value), `${name}.${key}`);
      assert.ok(Math.abs(pose.nod) <= 1);
      assert.ok(pose.offer >= 0 && pose.offer <= 1);
      assert.ok(pose.lift >= 0 && pose.lift <= .12);
    }
  }
});

test('listening, thinking, speaking, reassurance and celebration use distinct restrained signatures', () => {
  const sample = (state, mannerism, emotion = null) => avatar.poseFor({state, emotion, time: .42, elapsed: .42, mannerism, mannerismElapsed: .42, level: .6});
  const listening = sample('listening', 'acknowledge');
  const thinking = sample('thinking', 'focus');
  const speaking = sample('speaking', 'explain');
  const reassure = sample('error', 'reassure', 'reassuring');
  const celebrate = sample('success', 'confirm', 'celebrate');

  assert.equal(listening.mannerismCue, 'listening');
  assert.notEqual(listening.nod, 0);
  assert.ok(listening.eyeScaleY > 1.06);
  assert.equal(thinking.mannerismCue, 'thinking');
  assert.ok(thinking.eyeDeformation < listening.eyeDeformation);
  assert.equal(speaking.mannerismCue, 'speaking');
  assert.ok(speaking.statusWave > listening.statusWave);
  assert.ok(speaking.armLiftRight > speaking.armLiftLeft);
  assert.equal(reassure.mannerismCue, 'reassure');
  assert.equal(reassure.offer, 0);
  assert.ok(reassure.statusWave < speaking.statusWave);
  assert.equal(celebrate.mannerismCue, 'celebrate');
  assert.ok(celebrate.bob > 0);
  assert.ok(celebrate.armLiftRight > celebrate.armLiftLeft);
  assert.ok(celebrate.statusWave > speaking.statusWave);
});

test('quiet contexts replace celebration before deadline validation and never retain celebratory output', () => {
  const active = avatar.mannerismFor({name: 'confirm', elapsed: .5, emotion: 'calm'});
  assert.equal(active.name, 'reassure');
  assert.equal(active.cue, 'reassure');
  assert.equal(active.celebrate, 0);
  assert.equal(active.offer, 0);
  assert.equal(avatar.mannerismFor({name: 'confirm', elapsed: 1.06, emotion: 'calm'}).active, false);
  const pose = avatar.poseFor({state: 'success', emotion: 'reassuring', time: .5, elapsed: .5, mannerism: 'confirm', mannerismElapsed: .5});
  assert.equal(pose.mannerismCue, 'reassure');
  assert.ok(Math.abs(pose.bob) < .02, 'quiet breathing may remain but celebration lift must not');
  assert.equal(pose.offer, 0);
});

test('reduced motion retains readable state poses but removes every transient cue', () => {
  for (const name of Object.keys(avatar.MANNERISMS)) {
    const start = avatar.poseFor({state: 'speaking', mannerism: name, mannerismElapsed: .4, reducedMotion: true});
    const later = avatar.poseFor({state: 'speaking', time: 99, elapsed: 99, mannerism: name, mannerismElapsed: 99, reducedMotion: true});
    assert.deepEqual(start, later);
    assert.equal(start.mannerismCue, null);
    assert.equal(start.statusWave, 0);
    assert.equal(start.gesture, 0);
    assert.equal(start.bob, 0);
  }
});

function domFixture(t) {
  const dom = new JSDOM('<div id="mount"></div>', {url: 'https://sandbox.example/', pretendToBeVisual: true});
  const win = dom.window;
  let clock = 1000, intersect;
  const jobs = new Map(); let serial = 0;
  Object.defineProperty(win.performance, 'now', {value: () => clock});
  Object.defineProperty(win.document, 'hidden', {get: () => false});
  win.setTimeout = (callback, delay) => {jobs.set(++serial, {callback, due: clock + delay}); return serial;};
  win.clearTimeout = id => jobs.delete(id);
  win.matchMedia = () => ({matches: false, addEventListener() {}, removeEventListener() {}});
  win.IntersectionObserver = class {constructor(callback) {intersect = callback;} observe() {} disconnect() {}};
  const api = avatar.create({container: win.document.getElementById('mount'), visible: true, greetingOnOpen: false, loadScene: async () => {throw Error('Synthetic WebGL unavailable');}});
  t.after(() => {api.destroy(); dom.window.close();});
  return {api, jobs, intersect: value => intersect([{isIntersecting: value}]), advance(ms) {clock += ms; for (const [id, job] of [...jobs]) if (job.due <= clock) {jobs.delete(id); job.callback();}}};
}

test('controller exposes the current cue and cancels it when display motion stops', t => {
  const f = domFixture(t);
  const expected = {listening: 'listening', thinking: 'thinking', speaking: 'speaking', success: 'celebrate', error: 'reassure'};
  for (const [state, cue] of Object.entries(expected)) {
    f.api.setState('idle');
    f.api.setState(state);
    assert.equal(f.api.snapshot().mannerism.cue, cue);
    assert.equal(f.api.element.dataset.cue, cue);
    assert.equal(f.jobs.size, 1);
  }
  f.api.setPaused(true);
  assert.equal(f.api.snapshot().mannerism.cue, null);
  assert.equal(f.api.element.dataset.cue, '');
  assert.equal(f.jobs.size, 0);
  f.api.setPaused(false);
  assert.equal(f.api.snapshot().mannerism.cue, null, 'resuming must not replay a transient cue');
  f.api.setState('idle'); f.api.setState('listening');
  f.intersect(false);
  assert.equal(f.api.snapshot().mannerism.cue, null);
  assert.equal(f.jobs.size, 0);
});

function meshFixture() {
  const start = sceneSource.indexOf('  const head = new THREE.Group();');
  const end = sceneSource.indexOf('  const decoration = mesh(', start);
  const poseStart = sceneSource.indexOf('  const whiteColor = new THREE.Color(');
  const poseEnd = sceneSource.indexOf('  function render(pose', poseStart);
  assert.ok(start > 0 && end > start && poseStart > end && poseEnd > poseStart);
  const materialNames = ['ivory','gold','paleGold','face','lidMaterial','eyeMaterial','pupilMaterial','glint','gemMaterial','corneaMaterial','irisMaterial','mouthMaterial'];
  const materials = Object.fromEntries(materialNames.map(name => [name, new THREE.MeshPhysicalMaterial()]));
  const script = '(()=>{const geometries=new Set(),geometry=value=>{geometries.add(value);return value;},segments=(high,minimum=16)=>Math.max(minimum,Math.round(high*quality.geometryScale)),avatar=new THREE.Group();' + sceneSource.slice(start, end) + '\nconst key=new THREE.SpotLight(),eyeLight=new THREE.PointLight();let sampleTime=.42,reducedMotion=false;' + sceneSource.slice(poseStart, poseEnd) + '\nreturn {avatar,arms,statusBars,orbit,pose:value=>applyPose(value),dispose:()=>geometries.forEach(value=>value.dispose())};})()';
  const model = vm.runInNewContext(script, {THREE, quality: avatar.qualityFor({width: 390}), ...materials, AVATAR_SCENE_DECLARATIONS: {stateColors: {idle:'#4aa8ff',listening:'#49c9ff',thinking:'#ab87ff',speaking:'#ffcb79',success:'#72ddd1',error:'#ffc28e'}}});
  return {...model, destroy() {model.dispose(); Object.values(materials).forEach(value => value.dispose());}};
}

test('production mesh consumes cue accents without adding geometry or unsafe values', () => {
  const f = meshFixture();
  try {
    f.pose(avatar.poseFor({state: 'idle', time: .42}));
    const neutralBars = f.statusBars.map(bar => bar.scale.y);
    f.pose(avatar.poseFor({state: 'listening', time: .42, mannerism: 'acknowledge', mannerismElapsed: .42}));
    assert.notDeepEqual(f.statusBars.map(bar => bar.scale.y), neutralBars);
    const left = f.arms.find(item => item.side === -1).group, right = f.arms.find(item => item.side === 1).group;
    assert.ok(Math.abs(right.rotation.z) > Math.abs(left.rotation.z));
    f.pose(avatar.poseFor({state: 'success', emotion: 'celebrate', time: .42, elapsed: .42, mannerism: 'confirm', mannerismElapsed: .42}));
    assert.ok(Math.abs(right.rotation.z) > Math.abs(left.rotation.z));
    assert.ok(Number.isFinite(f.orbit.rotation.z));
    f.avatar.traverse(object => {
      if (!object.isObject3D) return;
      for (const vector of [object.position, object.rotation, object.scale]) for (const key of ['x', 'y', 'z']) assert.ok(Number.isFinite(vector[key]));
    });
  } finally {f.destroy();}
});
