(function (scope) {
  'use strict';
  const STATES = Object.freeze(['idle', 'listening', 'thinking', 'speaking', 'success', 'error']);
  const clamp = (value, low, high) => Math.max(low, Math.min(high, Number.isFinite(value) ? value : low));
  const validState = state => STATES.includes(state) ? state : 'idle';
  const MOODS = Object.freeze({
    idle: {color: '#4aa8ff', emotion: 'welcoming', label: 'Ready to help'},
    listening: {color: '#49c9ff', emotion: 'attentive', label: 'Listening'},
    thinking: {color: '#ab87ff', emotion: 'curious', label: 'Thinking'},
    speaking: {color: '#70d8f1', emotion: 'explaining', label: 'Speaking'},
    success: {color: '#72ddd1', emotion: 'delighted', label: 'Happy to help'},
    error: {color: '#ffc28e', emotion: 'considerate', label: 'Let\u2019s try another way'}
  });
  const EMOTIONS = Object.freeze(['calm', 'curious', 'celebrate', 'reassuring', 'warm', 'appreciated']);
  const validEmotion = emotion => EMOTIONS.includes(emotion) ? emotion : null;
  const MANNERISMS = Object.freeze({greet: 1.12, acknowledge: .9, focus: .86, explain: 1.05, confirm: 1.2, reassure: 1.05});
  // Irregular blinks avoid a perpetual attention animation.
  const BLINK_EVENTS = Object.freeze([
    Object.freeze({at: 3.3, duration: .2}),
    Object.freeze({at: 12.7, duration: .19}),
    Object.freeze({at: 23.6, duration: .21})
  ]);
  const BLINK_CYCLE = 35;
  const BEHAVIOR_CUES = Object.freeze({
    greet: 'anticipation', acknowledge: 'listening', focus: 'thinking', explain: 'speaking', confirm: 'celebrate', reassure: 'reassure'
  });
  const smooth = value => {const x = clamp(value, 0, 1); return x * x * (3 - 2 * x);};
  function pulse(elapsed, delay, duration) {
    const t = (elapsed - delay) / duration;
    return t <= 0 || t >= 1 ? 0 : t < .4 ? smooth(t / .4) : 1 - smooth((t - .4) / .6);
  }
  function blinkFor(time, reducedMotion = false) {
    if (reducedMotion || !Number.isFinite(time)) return 0;
    const phase = ((time % BLINK_CYCLE) + BLINK_CYCLE) % BLINK_CYCLE;
    for (const event of BLINK_EVENTS) {
      const progress = (phase - event.at) / event.duration;
      if (progress >= 0 && progress <= 1) return Math.sin(progress * Math.PI) ** .72;
    }
    return 0;
  }
  function mannerismFor({name = null, elapsed = 0, reducedMotion = false, emotion = null, intensity = 1, durationMs = null} = {}) {
    const strength = clamp(intensity, 0, 1);
    const quiet = emotion === 'calm' || emotion === 'reassuring' || emotion === 'appreciated';
    if (quiet && name === 'confirm') name = 'reassure';
    if (Object.hasOwn(MANNERISMS, name) && Number.isInteger(durationMs) && durationMs >= 400 && durationMs <= 2500) elapsed *= MANNERISMS[name] / (durationMs / 1000);
    const inactive = {name: null, cue: null, phase: null, active: false, eye: 0, head: 0, body: 0, anticipate: 0, listen: 0, think: 0, speak: 0, comfort: 0, celebrate: 0, nod: 0, offer: 0, lift: 0};
    if (!Object.hasOwn(MANNERISMS, name) || reducedMotion || !Number.isFinite(elapsed) || elapsed < 0 || elapsed >= MANNERISMS[name]) return inactive;
    const duration = MANNERISMS[name], cue = BEHAVIOR_CUES[name], gain = (quiet ? .35 : 1) * strength;
    const eye = pulse(elapsed, .02, Math.min(.65, duration * .72)) * strength;
    const head = pulse(elapsed, .12, Math.min(.8, duration * .82)) * gain;
    const body = pulse(elapsed, .21, Math.min(.82, duration * .76)) * gain;
    const anticipate = pulse(elapsed, .01, .24) * (quiet ? .5 : 1) * strength;
    const phase = elapsed < .12 ? 'anticipate' : elapsed < duration * .7 ? 'express' : 'settle';
    const listen = name === 'acknowledge' ? eye : 0, think = name === 'focus' ? eye : 0, speak = name === 'explain' ? body : 0;
    const comfort = name === 'reassure' ? head : 0, celebrate = name === 'confirm' && !quiet ? body : 0;
    const nod = name === 'acknowledge' ? pulse(elapsed, .16, .34) * .24 - pulse(elapsed, .48, .28) * .08 : name === 'confirm' ? pulse(elapsed, .18, .33) - .55 * pulse(elapsed, .56, .38) : name === 'reassure' ? pulse(elapsed, .2, .55) * .4 : 0;
    return {name, cue, phase, active: true, eye, head, body, anticipate, listen, think, speak, comfort, celebrate, nod: nod * gain, offer: ['greet', 'explain', 'confirm'].includes(name) && !quiet ? body : 0, lift: name === 'confirm' && !quiet ? Math.sin(Math.min(elapsed, duration) / duration * Math.PI) * .12 * strength : 0};
  }
  function productPhoto(value) {
    if (!value || !/^gid:\/\/shopify\/Product\/\d+$/.test(value.id || '') || !/^[a-z0-9]+(?:-[a-z0-9]+)*$/.test(value.handle || '')) return null;
    try {if (typeof value.image !== 'string' || value.image.length > 2048) return null; const url = new URL(value.image); if (url.protocol !== 'https:' || url.hostname !== 'cdn.shopify.com' || url.port || url.username || url.password || !url.pathname.startsWith('/s/files/')) return null;
      url.searchParams.set('width', '768'); url.searchParams.delete('height');
      return {id: value.id, handle: value.handle, title: String(value.title || 'Selected jewellery').replace(/[\u0000-\u001f]/g, '').slice(0, 140), image: url.href};} catch {return null;}
  }
  const PERFORMANCE_GESTURES = Object.freeze(['none', 'greet', 'acknowledge', 'focus', 'explain', 'present', 'reassure', 'confirm']);
  // Graphic expressions are geometry/path changes, not just another light colour.
  const FACE_EXPRESSIONS = Object.freeze({
    neutral: Object.freeze({faceBrowLift: .15, faceBrowTilt: 0, eyeSmile: .08, cheekGlow: .2, smileCurve: .34, faceSignal: .12}),
    attentive: Object.freeze({faceBrowLift: .7, faceBrowTilt: 0, eyeSmile: .02, cheekGlow: .3, smileCurve: .32, faceSignal: .9}),
    curious: Object.freeze({faceBrowLift: .78, faceBrowTilt: .55, eyeSmile: .02, cheekGlow: .3, smileCurve: .22, faceSignal: .55}),
    explaining: Object.freeze({faceBrowLift: .45, faceBrowTilt: .04, eyeSmile: .14, cheekGlow: .48, smileCurve: .46, faceSignal: .65}),
    delighted: Object.freeze({faceBrowLift: .6, faceBrowTilt: 0, eyeSmile: .48, cheekGlow: .84, smileCurve: .92, faceSignal: .8}),
    reassuring: Object.freeze({faceBrowLift: .42, faceBrowTilt: -.42, eyeSmile: .08, cheekGlow: .36, smileCurve: .2, faceSignal: .18}),
    warm: Object.freeze({faceBrowLift: .35, faceBrowTilt: 0, eyeSmile: .28, cheekGlow: .6, smileCurve: .72, faceSignal: .3})
  });
  // Conversational acts are presentation cues, never classifications of a
  // shopper's feelings. The caller supplies one bounded target and envelope.
  const EXPRESSION_KINDS = Object.freeze(['neutral', 'attentive', 'inquiry', 'explain', 'emphasize', 'reflect', 'support', 'celebrate', 'appreciate', 'resolve']);
  const EXPRESSION_FACES = Object.freeze({neutral: 'neutral', attentive: 'attentive', inquiry: 'curious', explain: 'explaining', emphasize: 'explaining', reflect: 'curious', support: 'reassuring', celebrate: 'delighted', appreciate: 'warm', resolve: 'warm'});
  function validateExpression(value) {
    try {if (!value || typeof value !== 'object' || Array.isArray(value) || Object.keys(value).length !== 2 || !Object.keys(value).every(key => key === 'kind' || key === 'intensity') || !EXPRESSION_KINDS.includes(value.kind) || !Number.isFinite(value.intensity) || value.intensity < 0 || value.intensity > 1) return null;
      return Object.freeze({kind: value.kind, intensity: value.intensity});} catch {return null;}
  }
  const EMPTY_SPEECH_SIGNAL = Object.freeze({amplitude: 0, bands: Object.freeze([0, 0, 0, 0, 0, 0]), brightness: 0, valid: false});
  function validateSpeechSignal(value) {
    try {if (!value || typeof value !== 'object' || Array.isArray(value) || Object.keys(value).length !== 4 || !Object.keys(value).every(key => ['amplitude', 'bands', 'brightness', 'valid'].includes(key)) || typeof value.valid !== 'boolean' || !Number.isFinite(value.amplitude) || value.amplitude < 0 || value.amplitude > 1 || !Number.isFinite(value.brightness) || value.brightness < 0 || value.brightness > 1 || !Array.isArray(value.bands) || value.bands.length !== 6 || [...value.bands].some(band => !Number.isFinite(band) || band < 0 || band > 1)) return null;
      return value.valid ? Object.freeze({amplitude: value.amplitude, bands: Object.freeze([...value.bands]), brightness: value.brightness, valid: true}) : EMPTY_SPEECH_SIGNAL;} catch {return null;}
  }
  function faceFor(state, emotion, intensity = 1) {
    const quiet = emotion === 'calm' || emotion === 'reassuring';
    const name = state === 'listening' ? 'attentive' : quiet ? 'reassuring' : emotion === 'warm' || emotion === 'appreciated' ? 'warm' : emotion === 'curious' ? 'curious' : emotion === 'celebrate' ? 'delighted' : ({thinking: 'curious', speaking: 'explaining', success: 'delighted', error: 'reassuring'}[state] || 'neutral');
    const neutral = FACE_EXPRESSIONS.neutral, chosen = FACE_EXPRESSIONS[name], amount = clamp(intensity, 0, 1);
    return {faceExpression: name, ...Object.fromEntries(Object.keys(neutral).map(key => {const target = state === 'listening' && quiet ? chosen[key] + (FACE_EXPRESSIONS.reassuring[key] - chosen[key]) * .4 : chosen[key]; return [key, neutral[key] + (target - neutral[key]) * amount];}))};
  }
  function expressionFace(base, value, quiet = false, listening = false) {
    const cue = validateExpression(value); if (!cue || cue.intensity === 0) return {...base, expressionKind: null, expressionHeadRoll: 0};
    const kind = quiet && cue.kind === 'celebrate' ? 'support' : cue.kind, name = EXPRESSION_FACES[kind];
    const target = {...FACE_EXPRESSIONS[name]};
    if (kind === 'emphasize') {target.faceBrowLift = .88; target.faceBrowTilt = .08; target.eyeSmile = .03; target.smileCurve = .42;}
    if (kind === 'reflect') {target.faceBrowLift = .32; target.faceBrowTilt = .24; target.eyeSmile = .04; target.smileCurve = .12;}
    if (kind === 'resolve') {target.faceBrowLift = .5; target.faceBrowTilt = -.08; target.eyeSmile = .12; target.smileCurve = .52;}
    const amount = cue.intensity * (quiet ? .65 : 1), face = {...base, faceExpression: name, expressionKind: kind, expressionHeadRoll: ({inquiry: .017, reflect: -.012, support: -.01, appreciate: -.008, resolve: .006}[kind] || 0) * amount};
    for (const key of Object.keys(FACE_EXPRESSIONS.neutral)) face[key] += (target[key] - face[key]) * amount;
    if (listening) {face.eyeSmile = Math.min(face.eyeSmile, .22); face.cheekGlow = Math.min(face.cheekGlow, .4); face.smileCurve = Math.min(face.smileCurve, .45);}
    return face;
  }
  function pointerGaze(clientX, clientY, box, anchor = {x: .5, y: .5}) {
    if (!Number.isFinite(clientX) || !Number.isFinite(clientY) || !box || !Number.isFinite(box.width) || !Number.isFinite(box.height) || !(box.width > 0) || !(box.height > 0) || !Number.isFinite(box.left) || !Number.isFinite(box.top)) return null;
    const x = clamp(Number.isFinite(anchor?.x) ? anchor.x : .5, 0, 1), y = clamp(Number.isFinite(anchor?.y) ? anchor.y : .5, 0, 1);
    return {x: clamp((clientX - box.left - box.width * x) / Math.max(80, box.width * .65), -1, 1), y: clamp((box.top + box.height * y - clientY) / Math.max(80, box.height * .6), -1, 1)};
  }
  function validateAvatarPerformance(value) {
    try {if (!value || typeof value !== 'object' || Array.isArray(value) || Object.keys(value).length !== 4 || !Object.keys(value).every(key => ['mood','gesture','intensity','durationMs'].includes(key)) || !EMOTIONS.includes(value.mood) || !PERFORMANCE_GESTURES.includes(value.gesture) || !Number.isFinite(value.intensity) || value.intensity < 0 || value.intensity > 1 || !Number.isInteger(value.durationMs) || value.durationMs < 400 || value.durationMs > 2500) return null;
      return Object.freeze({mood: value.mood, gesture: value.gesture, intensity: value.intensity, durationMs: value.durationMs});} catch {return null;}
  }
  let instanceCount = 0;
  function declaredScene(raw, textureSize) {
    if (!raw || ![1, 2, 3, 4, 5].includes(raw.schema) || !Array.isArray(raw.textures) || raw.textures.length < 1 || raw.textures.length > 16) return null;
    const textures = raw.textures.map(value => ({name: typeof value?.name === 'string' ? value.name.slice(0, 80) : '', kind: typeof value?.kind === 'string' ? value.kind.slice(0, 32) : '', width: textureSize, height: textureSize}));
    if (textures.some(value => !value.name || !value.kind)) return null;
    return {schema: raw.schema, source: 'loaded_scene_module', textures};
  }
  function qualityFor(hints = {}) {
    const mobile = hints.mobile === true || (hints.width > 0 && hints.width < 600) || (hints.memory > 0 && hints.memory <= 4);
    return {name: mobile ? 'adaptive' : 'high', pixelRatio: Math.min(mobile ? 1.5 : 2, Math.max(1, hints.pixelRatio || 1)), shadowSize: mobile ? 1024 : 2048, textureSize: mobile ? 1024 : 2048, fps: mobile ? 30 : 60, geometryScale: mobile ? .8 : 1, bloom: !mobile && hints.bloom !== false};
  }
  function poseFor({state = 'idle', time = 0, elapsed = 0, level = 0, speechSignal = null, gaze = {x: 0, y: 0}, headGaze = null, reducedMotion = false, emotion = null, expression = null, mannerism = null, mannerismElapsed = 0, productFocus = null, speechBeatElapsed = null, appreciationElapsed = 0, performance = null, mannerismDurationMs = null} = {}) {
    state = validState(state); time = reducedMotion ? 0 : Number.isFinite(time) ? time : 0; elapsed = Number.isFinite(elapsed) ? Math.max(0, elapsed) : 0;
    gaze = gaze && typeof gaze === 'object' ? gaze : {}; const gazeX = Number.isFinite(gaze.x) ? gaze.x : 0, gazeY = Number.isFinite(gaze.y) ? gaze.y : 0;
    const plan = validateAvatarPerformance(performance), emotionalGain = plan ? plan.intensity : 1;
    emotion = validEmotion(emotion); const warm = emotion === 'warm', curious = emotion === 'curious'; const calm = emotion === 'calm' || emotion === 'reassuring' || emotion === 'appreciated';
    const motion = reducedMotion ? 0 : calm ? .4 : 1, talking = state === 'speaking', thoughtful = state === 'thinking', listening = state === 'listening', happy = state === 'success' && !calm, concerned = state === 'error';
    const blink = motion ? blinkFor(time) : 0;
    const measured = speechSignal === null ? null : validateSpeechSignal(speechSignal), signalUsable = talking && !reducedMotion && (speechSignal === null || measured?.valid === true);
    const audioEnergy = signalUsable ? measured ? measured.amplitude : clamp(level, 0, 1) : 0, speech = .88 * audioEnergy;
    const speechBands = signalUsable && measured ? [...measured.bands] : [...EMPTY_SPEECH_SIGNAL.bands], speechBrightness = signalUsable && measured ? measured.brightness : 0;
    const face = expressionFace(faceFor(state, emotion, emotionalGain), reducedMotion ? null : expression, calm, listening);
    const expressive = mannerismFor({name: mannerism, elapsed: mannerismElapsed, reducedMotion, emotion: emotion === 'appreciated' ? 'reassuring' : emotion, intensity: emotionalGain, durationMs: mannerismDurationMs});
    const heart = emotion === 'appreciated' && !talking ? reducedMotion ? 1 : smooth(appreciationElapsed / .18) * (1 - smooth((appreciationElapsed - 1.17) / .28)) : 0;
    const lift = expressive.active ? expressive.lift * .06 : happy && motion && elapsed < 1.2 ? Math.sin(elapsed / 1.2 * Math.PI) * .006 : 0;
    const greeting = expressive.name === 'greet', acknowledgement = expressive.name === 'acknowledge', focus = expressive.name === 'focus';
    const apertureAccent = expressive.listen * .035 - expressive.think * .055 + expressive.speak * .025 - expressive.comfort * .018 + expressive.celebrate * .06;
    const stateEnergy = talking ? speech : thoughtful ? .22 : listening ? .1 : happy ? .34 : concerned ? .08 : .035;
    const phraseGesture = talking && motion && Number.isFinite(speechBeatElapsed) ? pulse(speechBeatElapsed, .025, .68) * audioEnergy : 0;
    const product = productFocus && typeof productFocus === 'object' ? {x: clamp(productFocus.x, -1, 1), y: clamp(productFocus.y, -1, 1)} : null;
    const present = product && !reducedMotion ? expressive.offer * (calm ? .3 : 1) : 0;
    const focusAge = product ? (Number.isFinite(productFocus.elapsed) ? Math.max(0, productFocus.elapsed) : 1) : 0;
    const eyeFollow = product ? reducedMotion ? 1 : smooth(focusAge / .18) : 1;
    const headFollow = product ? reducedMotion ? 1 : smooth((focusAge - .1) / .38) : 1;
    const targetX = product ? product.x * headFollow : Number.isFinite(headGaze?.x) ? headGaze.x : gazeX, targetY = product ? product.y * headFollow : Number.isFinite(headGaze?.y) ? headGaze.y : gazeY;
    const speechAccent = talking && motion ? audioEnergy : 0;
    const helloWave = greeting && motion ? Math.sin(Math.min(1, mannerismElapsed / MANNERISMS.greet) * Math.PI * 2) * expressive.body * .45 : 0;
    const leftArm = calm ? .025 : thoughtful ? .18 : happy ? .1 : listening ? .045 : 0;
    const rightArm = calm ? .025 : thoughtful ? .25 : happy ? .16 : listening ? .07 : 0;
    return {
      mannerism: expressive.name, mannerismCue: expressive.cue, mannerismPhase: expressive.phase, mannerismActive: expressive.active, nod: expressive.nod, offer: expressive.offer, helloWave, speechEnergy: audioEnergy, speechBands, speechBrightness, speechSignalValid: signalUsable && audioEnergy > .015, phraseGesture, productFocused: !!product, present, targetX: product?.x || 0, targetY: product?.y || 0, speechAccent, lean: expressive.body * (acknowledgement ? .04 : .02) + phraseGesture * .018,
      state, heart, ...face, lidClosure: clamp(blink + face.eyeSmile * .08, 0, 1), stanceScale: 1, bodyDepth: 0, bodyYaw: 0, eyeColor: heart > .01 ? '#ed93aa' : MOODS[state].color, emotion: emotion || MOODS[state].emotion, blink,
      eyeDeformation: (warm ? .1 : curious ? -.055 : emotion === 'reassuring' ? .05 : 0) * emotionalGain + (calm ? -.025 : happy ? .24 : thoughtful ? -.14 : listening ? .1 : concerned ? -.08 : greeting ? expressive.eye * .1 : 0) + apertureAccent,
      eyeScaleX: (curious ? 1 - .04 * emotionalGain : 1) * (happy ? 1.08 : thoughtful ? .93 : 1) + expressive.comfort * .025,
      eyeScaleY: (warm || emotion === 'reassuring' ? 1 + .025 * emotionalGain : curious ? 1 + .055 * emotionalGain : 1) * (happy ? 1.04 : listening ? 1.08 : talking ? 1.06 : 1) + expressive.listen * .025,
      ringRotation: thoughtful ? -.1 : expressive.celebrate * .09 - expressive.comfort * .04,
      ringRipple: clamp(stateEnergy + expressive.listen * .08 + expressive.think * .14 + expressive.speak * .18 + expressive.celebrate * .16, 0, 1),
      antennaTilt: (thoughtful ? -.14 : listening ? .08 : happy ? .18 : 0) + expressive.anticipate * .055 + expressive.comfort * -.035,
      bob: lift,
      bodyRoll: 0,
      headYaw: clamp(clamp(targetX, -1, 1) * .1 + (greeting ? -.018 * expressive.head : 0), -.1, .1),
      // A positive X rotation tips a forward-facing Three head DOWN. Gaze y is UP.
      headPitch: clamp(-clamp(targetY, -1, 1) * .09 + expressive.nod * .035 + expressive.head * (acknowledgement ? .018 : focus ? -.015 : 0), -.09, .09),
      headRoll: (curious ? .045 : warm ? -.02 : 0) * emotionalGain + (calm ? -.008 : concerned ? -.025 : thoughtful ? .03 : listening ? -.018 : happy ? .012 : 0) + expressive.head * (greeting ? -.03 : acknowledgement ? -.018 : focus ? .015 : 0),
      eyeOpen: Math.max(.035, (happy ? 1.02 : listening ? 1.06 : talking ? 1.08 : greeting ? 1 - expressive.eye * .04 : 1) * (1 - blink)),
      gazeX: clamp(product ? product.x * eyeFollow : gazeX, -1, 1) * (product ? .1 : .07),
      gazeY: clamp(product ? product.y * eyeFollow : gazeY, -1, 1) * .07,
      browLift: happy ? .08 : listening ? .045 : concerned ? .025 : thoughtful ? .02 : 0,
      browAngle: concerned ? .15 : thoughtful ? -.08 : happy ? -.08 : -.025,
      mouth: concerned || face.smileCurve < .28 ? 'reflective-curve' : 'smile-curve', mouthOpen: 0, mouthCurve: (face.smileCurve - .28) * .11,
      armLift: Math.max(leftArm, rightArm), armLiftLeft: leftArm + expressive.comfort * .02 + phraseGesture * .09 + (product && product.x < 0 ? present * .48 : 0), armLiftRight: rightArm + expressive.speak * .1 + expressive.celebrate * .08 + phraseGesture * .32 + (product && product.x >= 0 ? present * .48 : 0),
      gesture: expressive.offer * .075,
      armReach: present * .32,
      statusWave: expressive.listen * .2 + expressive.think * .45 + expressive.speak * .65 + expressive.comfort * .15 + expressive.celebrate * .8,
      lightPulse: thoughtful ? .22 : happy ? .45 : talking ? speech * .22 : expressive.anticipate * .08
    };
  }

  function create(options = {}) {
    const container = options.container;
    if (!container || typeof container.appendChild !== 'function') throw Error('The avatar needs a display container.');
    const doc = container.ownerDocument, win = doc.defaultView || scope;
    const base = new URL(options.assetBase || '.', doc.baseURI);
    const moduleUrl = new URL(options.sceneModuleUrl || 'assets/brites-concierge-avatar-scene.mjs', base).href;
    const cssUrl = new URL(options.cssUrl || 'brites-concierge-avatar.css', base).href;
    const frame = doc.createElement('div'); frame.className = 'brites-avatar'; frame.dataset.state = validState(options.initialState); frame.setAttribute('role', 'img'); frame.setAttribute('aria-label', 'Brites jewellery gift guide');
    const surface = doc.createElement('div'); surface.className = 'brites-avatar__surface'; surface.style.visibility = 'hidden';
    const fallback = doc.createElement('div'); fallback.className = 'brites-avatar__fallback'; fallback.hidden = true; fallback.style.display = 'none';
    const loadingNotice = doc.createElement('div'); loadingNotice.className = 'brites-avatar__loading'; loadingNotice.textContent = 'Preparing your guide\u2026'; loadingNotice.setAttribute('aria-hidden', 'true');
    const id = 'britesRobot' + (++instanceCount);
    fallback.innerHTML = `<svg viewBox="0 0 320 280" aria-hidden="true" xmlns="http://www.w3.org/2000/svg">
      <defs><linearGradient id="${id}Ivory" x2=".45" y2="1"><stop stop-color="#ffffff"/><stop offset=".65" stop-color="#f8f5ed"/><stop offset="1" stop-color="#e2e8ec"/></linearGradient><linearGradient id="${id}Gold" x2=".2" y2="1"><stop stop-color="#f2dda8"/><stop offset=".5" stop-color="#b99762"/><stop offset="1" stop-color="#e4c78e"/></linearGradient><radialGradient id="${id}Glass"><stop stop-color="#183247"/><stop offset="1" stop-color="#09121d"/></radialGradient></defs>
      <ellipse class="brites-avatar__shadow" cx="160" cy="264" rx="51" ry="5" fill="#243745" opacity=".10"/>
      <g class="brites-avatar__robot">
        <ellipse cx="160" cy="261" rx="48" ry="5" fill="#ebe9e1" stroke="#d8d6cd"/>
        <path class="brites-avatar__porcelain-body" d="M160 136C128 136 111 166 114 201C117 238 132 260 148 260H172C188 260 203 238 206 201C209 166 192 136 160 136Z" fill="url(#${id}Ivory)" stroke="#d8e1e7"/>
        <path d="M155 200l5-6 5 6-5 7Z" fill="#86cbdc" stroke="url(#${id}Gold)" stroke-width="1"/>
        <g class="brites-avatar__arm brites-avatar__arm--left"><path d="M117 169C101 170 91 191 94 221C96 238 103 242 110 232C116 214 119 190 117 169Z" fill="url(#${id}Ivory)" stroke="#d8e1e7"/><path d="M107 186Q98 206 103 225" fill="none" stroke="url(#${id}Gold)" stroke-width="1"/></g><g class="brites-avatar__arm brites-avatar__arm--right"><path d="M203 169C219 170 229 191 226 221C224 238 217 242 210 232C204 214 201 190 203 169Z" fill="url(#${id}Ivory)" stroke="#d8e1e7"/><path d="M213 186Q222 206 217 225" fill="none" stroke="url(#${id}Gold)" stroke-width="1"/></g>
        <g class="brites-avatar__head"><ellipse cx="160" cy="116" rx="82" ry="68" fill="url(#${id}Ivory)" stroke="#d8e1e7"/><rect x="89" y="73" width="142" height="101" rx="46" fill="url(#${id}Gold)"/><rect x="91" y="75" width="138" height="97" rx="45" fill="url(#${id}Glass)"/>
          <g class="brites-avatar__eye-gaze"><g class="brites-avatar__eye"><path class="brites-avatar__ribbon--left" d="M113 115Q113 108 120 108H141Q148 108 148 115Q148 122 141 122H120Q113 122 113 115Z"/><path class="brites-avatar__ribbon--right" d="M172 115Q172 108 179 108H200Q207 108 207 115Q207 122 200 122H179Q172 122 172 115Z"/></g><g class="brites-avatar__heart"><path d="M130 127C124 122 114 115 114 108C114 100 125 97 130 104C135 97 146 100 146 108C146 115 136 122 130 127Z"/><path d="M190 127C184 122 174 115 174 108C174 100 185 97 190 104C195 97 206 100 206 108C206 115 196 122 190 127Z"/></g></g>
          <g class="brites-avatar__face-design" fill="none" stroke="#9de7ff" stroke-linecap="round" stroke-linejoin="round">
            <path class="brites-avatar__brow--left" d="M115 94Q130 87 145 94" stroke-width="2.5"/><path class="brites-avatar__brow--right" d="M175 94Q190 87 205 94" stroke-width="2.5"/>
            <path class="brites-avatar__cheek--left" d="M103 138l4-4 4 4-4 4Z" fill="#e3b28c" stroke-width="1"/><path class="brites-avatar__cheek--right" d="M209 138l4-4 4 4-4 4Z" fill="#e3b28c" stroke-width="1"/>
            <path class="brites-avatar__smile-signal" d="M147 145Q160 151 173 145" stroke-width="2.5"/>
            <path class="brites-avatar__eye-smile" d="M115 121Q130 110 145 121M175 121Q190 110 205 121" stroke-width="2"/>
            <g class="brites-avatar__face-signal"><path d="M150 156v-2M155 158v-6M160 159v-8M165 158v-6M170 156v-2" stroke-width="2"/></g>
          </g><path d="M125 95Q130 89 134 87" fill="none" stroke="#fff" opacity=".16" stroke-width="3" stroke-linecap="round"/>
        </g>
      </g></svg>`;
    const speechMouthNode = doc.createElementNS('http://www.w3.org/2000/svg','path'); speechMouthNode.setAttribute('class','brites-avatar__speech-mouth'); speechMouthNode.setAttribute('d','M147 145Q160 146 173 145'); speechMouthNode.setAttribute('fill','none'); speechMouthNode.setAttribute('stroke-width','2.6'); speechMouthNode.setAttribute('opacity','0'); fallback.querySelector('.brites-avatar__face-design').appendChild(speechMouthNode);
    const speechBandNodes = [], speechRippleNodes = [];
    for (const [count, name, nodes] of [[6, 'speech-band', speechBandNodes], [2, 'speech-ripple', speechRippleNodes]]) for (let index = 0; index < count; index++) {const node = doc.createElementNS('http://www.w3.org/2000/svg','path'); node.setAttribute('class','brites-avatar__'+name); node.setAttribute('data-band',String(index)); node.setAttribute('fill','none'); node.setAttribute('opacity','0'); fallback.querySelector('.brites-avatar__face-design').appendChild(node); nodes.push(node);}
    const faceNodes = Object.fromEntries(['brow--left', 'brow--right', 'cheek--left', 'cheek--right', 'smile-signal', 'eye-smile', 'face-signal', 'ribbon--left', 'ribbon--right'].map(name => [name, fallback.querySelector('.brites-avatar__' + name)]));
    const caption = doc.createElement('div'); caption.className = 'brites-avatar__caption';
    caption.setAttribute('aria-hidden', 'true');
    caption.textContent = MOODS[validState(options.initialState)].label;
    const style = doc.createElement('link'); style.rel = 'stylesheet'; style.href = cssUrl;
    const productCard = doc.createElement('figure'); productCard.className = 'brites-avatar__product'; productCard.hidden = true;
    const productImage = doc.createElement('img'); productImage.alt = ''; productImage.referrerPolicy = 'no-referrer'; productImage.crossOrigin = 'anonymous';
    const productLabel = doc.createElement('figcaption'); productLabel.textContent = 'Product photo'; productCard.append(productLabel);
    frame.append(style, surface, fallback, loadingNotice, caption, productCard); container.appendChild(frame);
    let constructionCleanup = null;
    try {
    let state = validState(options.initialState), visible = options.visible === true, intersecting = true, destroyed = false, loading = false, engine = null, declarations = null, failed = false, paused = options.paused === true, emotion = options.emotion === 'appreciated' ? null : validEmotion(options.emotion), failureReason = null, level = 0, gaze = {x: 0, y: 0}, headGaze = {x: 0, y: 0}, gazeTarget = {x: 0, y: 0}, gazeAt = 0, pointerFrame = null, stateAt = 0, mannerism = null, mannerismAt = 0, mannerismId = 0, mannerismTimer = null, greetedThisOpening = false, productFocus = null, speechBeatAt = null, speechRested = true, appreciationAt = 0, appreciationTimer = null, appreciationEpoch = 0, previousEmotion = null, shownProduct = null, productEpoch = 0, floating = false, performance = null, performanceTimer = null, performanceEpoch = 0, performancePreviousEmotion = null, mannerismDurationMs = null;
    let frameReady = false, pendingReadyType = null, fallbackPresented = false, expression = null, blinkTimer = null, lastFacePose = null, speechSignal = EMPTY_SPEECH_SIGNAL, displayedSpeech = {...EMPTY_SPEECH_SIGNAL, bands: [...EMPTY_SPEECH_SIGNAL.bands]}, signalAt = null, signalSettling = false, lastSpeechVisual = null;
    const media = win.matchMedia ? win.matchMedia('(prefers-reduced-motion: reduce)') : null;
    let systemReducedMotion = !!media?.matches, manualReducedMotion = false, reducedMotion = systemReducedMotion, readyResolve;
    const ready = new Promise(resolve => {readyResolve = resolve;});
    const quality = qualityFor({width: win.innerWidth, mobile: options.mobile, memory: win.navigator?.deviceMemory, pixelRatio: win.devicePixelRatio, bloom: options.bloom});
    const now = () => {const stamp = win.performance?.now?.(); return (Number.isFinite(stamp) ? stamp : Date.now()) / 1000;};
    stateAt = gazeAt = now();
    function cancelAppreciation(restore = true) {if (appreciationTimer !== null) win.clearTimeout(appreciationTimer); appreciationTimer = null; appreciationEpoch++; if (restore && emotion === 'appreciated') emotion = previousEmotion; previousEmotion = null;}
    function cancelPerformance(restore = true) {if (performanceTimer !== null) win.clearTimeout(performanceTimer); performanceTimer = null; performanceEpoch++; if (!performance) return; const prior = performancePreviousEmotion; performance = null; performancePreviousEmotion = null; delete frame.dataset.performance; if (restore) {if (emotion === 'appreciated') previousEmotion = prior; else emotion = prior;} cancelMannerism();}
    function cancelMannerism() {if (mannerismTimer !== null) win.clearTimeout(mannerismTimer); mannerismTimer = null; mannerism = null; mannerismDurationMs = null; delete frame.dataset.mannerism;}
    function playMannerism(name, durationMs = null) {
      cancelMannerism();
      if (!Object.hasOwn(MANNERISMS, name) || !canDisplay() || paused || reducedMotion) return;
      if ((emotion === 'calm' || emotion === 'reassuring' || emotion === 'appreciated') && name === 'confirm') name = 'reassure';
      mannerism = name; mannerismDurationMs = Number.isInteger(durationMs) && durationMs >= 400 && durationMs <= 2500 ? durationMs : null; mannerismAt = now(); mannerismId++; frame.dataset.mannerism = name;
      mannerismTimer = win.setTimeout(() => {cancelMannerism(); if (!destroyed) sync();}, mannerismDurationMs || MANNERISMS[name] * 1000);
      emit('mannerism', {name, id: mannerismId, duration: MANNERISMS[name]});
    }
    function triggerGreeting() {if (destroyed || greetedThisOpening || !canDisplay() || paused || reducedMotion) return false; playMannerism('greet'); greetedThisOpening = mannerism === 'greet'; sync(); return greetedThisOpening;}
    function advanceGaze(time) {
      const delta = clamp(time - gazeAt, 0, .12); gazeAt = time;
      if (!canDisplay() || paused) return;
      if (reducedMotion) {gaze = {...gazeTarget}; headGaze = {...gazeTarget}; return;}
      for (const [point, duration] of [[gaze, .07], [headGaze, .18]]) {
        const follow = 1 - Math.exp(-delta / duration);
        for (const axis of ['x', 'y']) {point[axis] += (gazeTarget[axis] - point[axis]) * follow; if (Math.abs(gazeTarget[axis] - point[axis]) < .0005) point[axis] = gazeTarget[axis];}
      }
    }
    let displayedFace = null, faceAt = null, faceSettling = false;
    function clearSpeechSignal() {const changed = level !== 0 || speechSignal.valid || displayedSpeech.valid || signalSettling || lastFacePose?.speechEnergy > 0 || lastSpeechVisual?.rippleActive === true; level = 0; speechSignal = EMPTY_SPEECH_SIGNAL; displayedSpeech = {...EMPTY_SPEECH_SIGNAL, bands: [...EMPTY_SPEECH_SIGNAL.bands]}; signalAt = now(); signalSettling = false; if(lastFacePose){lastFacePose.speechEnergy=0;lastFacePose.mouthOpen=0;}lastSpeechVisual={curveClosed:true,rippleActive:false,amplitude:0,bands:[...EMPTY_SPEECH_SIGNAL.bands],brightness:0}; return changed;}
    function advanceSignal(time) {
      const delta = signalAt === null ? 0 : clamp(time - signalAt, 0, .12); signalAt = time;
      if (!speechSignal.valid || state !== 'speaking' || !canDisplay() || paused || reducedMotion) {displayedSpeech = {...EMPTY_SPEECH_SIGNAL, bands: [...EMPTY_SPEECH_SIGNAL.bands]}; signalSettling = false; return;}
      const blend = (target, current) => {const gain = 1 - Math.exp(-delta / (target > current ? .04 : .1)), next = current + (target - current) * gain; return Math.abs(target-next) < .0005 ? target : next;};
      displayedSpeech.amplitude = blend(speechSignal.amplitude, displayedSpeech.amplitude); displayedSpeech.brightness = blend(speechSignal.brightness, displayedSpeech.brightness); displayedSpeech.bands = speechSignal.bands.map((value,index) => blend(value,displayedSpeech.bands[index])); displayedSpeech.valid = true;
      signalSettling = Math.abs(speechSignal.amplitude-displayedSpeech.amplitude) >= .0005 || Math.abs(speechSignal.brightness-displayedSpeech.brightness) >= .0005 || speechSignal.bands.some((value,index) => Math.abs(value-displayedSpeech.bands[index]) >= .0005);
    }
    function poseAt(time, fallbackTarget = false) {advanceGaze(time); advanceSignal(time); const pose = poseFor({state, time, elapsed: time - stateAt, level, speechSignal: displayedSpeech, gaze, headGaze, productFocus: productFocus ? {...productFocus, elapsed: fallbackTarget ? Math.max(.44, time - productFocus.at) : time - productFocus.at} : null, reducedMotion, emotion, expression, mannerism, mannerismElapsed: fallbackTarget && productFocus && mannerism === 'explain' ? Math.max(.44, time - mannerismAt) : time - mannerismAt, speechBeatElapsed: speechBeatAt === null ? -1 : time - speechBeatAt, appreciationElapsed: time - appreciationAt, performance, mannerismDurationMs});
      const keys = [...Object.keys(FACE_EXPRESSIONS.neutral), 'eyeScaleX', 'eyeScaleY', 'eyeDeformation', 'expressionHeadRoll'];
      const delta = faceAt === null ? 0 : clamp(time - faceAt, 0, .12); faceAt = time;
      if (!displayedFace || reducedMotion || paused || !canDisplay()) displayedFace = Object.fromEntries(keys.map(key => [key, pose[key]]));
      else {const blend = 1 - Math.exp(-delta / .12); for (const key of keys) {displayedFace[key] += (pose[key] - displayedFace[key]) * blend; if(Math.abs(pose[key]-displayedFace[key])<.0005)displayedFace[key]=pose[key];}}
      faceSettling = !reducedMotion && !paused && keys.some(key => Math.abs(pose[key]-displayedFace[key]) >= .0005);
      const shown = {...pose, ...displayedFace}; shown.mouthCurve = (shown.smileCurve - .28) * .11;
      lastFacePose = Object.fromEntries(['faceBrowLift', 'faceBrowTilt', 'eyeSmile', 'eyeOpen', 'smileCurve', 'cheekGlow', 'mouthOpen', 'mouthCurve', 'speechEnergy'].map(key => [key, shown[key]])); lastFacePose.headRoll = shown.headRoll + shown.expressionHeadRoll;
      lastSpeechVisual = {curveClosed: true, rippleActive: shown.speechSignalValid, amplitude: shown.speechEnergy, bands: [...shown.speechBands], brightness: shown.speechBrightness};
      return shown;
    }
    function emit(type, detail) {frame.dispatchEvent(new win.CustomEvent('brites-avatar:' + type, {detail, bubbles: true, composed: true})); try {options.onStatus?.(type, detail);} catch {}}
    function canDisplay() {return visible && intersecting && !doc.hidden && !destroyed;}
    function active() {return canDisplay() && !paused && !failed;}
    function hasWebglFrame() {return !!engine && !failed && frameReady;}
    function fallbackMoving() {return fallbackPresented && canDisplay() && !paused && !reducedMotion && !hasWebglFrame();}
    function snapshot() {
      const scene = engine?.snapshot?.() || {};
      return {...scene, state, emotion, expression: expression ? {...expression} : null, facePose: lastFacePose ? {...lastFacePose} : null, speechSignal: {...speechSignal, bands: [...speechSignal.bands]}, speechVisual: lastSpeechVisual ? {...lastSpeechVisual, bands: [...lastSpeechVisual.bands]} : null, performance: performance ? {...performance} : null, gaze: {target: {...gazeTarget}, eye: {...gaze}, head: {...headGaze}, scope: 'visible-page-pointer'}, floating, shownProduct: shownProduct ? {id: shownProduct.id, handle: shownProduct.handle, title: shownProduct.title, format: 'verified-product-photo'} : null, productFocus: productFocus ? {...productFocus} : null, visible, intersecting, paused, reducedMotion, motionPreferences: {system: systemReducedMotion, manual: manualReducedMotion}, mode: hasWebglFrame() ? 'webgl' : failed ? 'fallback' : 'pending', loading, destroyed,
        mannerism: {name: mannerism, cue: BEHAVIOR_CUES[mannerism] || null, id: mannerismId, duration: mannerismDurationMs ? mannerismDurationMs / 1000 : MANNERISMS[mannerism] || 0, active: !!mannerism && canDisplay() && !paused && !reducedMotion, elapsed: mannerism ? Math.max(0, now() - mannerismAt) : 0},
        animated: hasWebglFrame() ? active() && !reducedMotion && scene.animated === true : fallbackMoving(),
        fallback: {format: 'animated_svg_2d', active: fallbackPresented && canDisplay() && !hasWebglFrame(), animated: fallbackMoving(), reason: failureReason},
        quality: {...quality}, declarations: declarations ? {schema: declarations.schema, source: declarations.source, textures: declarations.textures.map(value => ({...value}))} : null};
    }
    function syncLayers() {
      // Keep the renderer measurable, but show exactly one representation even
      // when the optional stylesheet is delayed, stale or unavailable.
      const webgl = hasWebglFrame(), showFallback = fallbackPresented && !webgl;
      frame.dataset.rendering = webgl ? 'webgl' : failed ? 'fallback' : loading || engine ? 'loading' : 'pending';
      surface.style.visibility = webgl ? 'visible' : 'hidden';
      fallback.hidden = !showFallback;
      fallback.style.display = showFallback ? '' : 'none';
      loadingNotice.hidden = webgl || showFallback;
      loadingNotice.style.display = loadingNotice.hidden ? 'none' : '';
      loadingNotice.textContent = paused ? 'Your guide will appear when animation resumes.' : 'Preparing your guide\u2026';
    }
    function syncFallback() {
      syncLayers();
      const pose = poseAt(now(), !hasWebglFrame());
      frame.style.setProperty('--brites-eye-color', emotion === 'appreciated' && state !== 'speaking' ? '#ed93aa' : pose.eyeColor);
      frame.style.setProperty('--brites-voice-color', `hsl(${(183 + pose.speechBrightness * 23).toFixed(1)} 85% ${(72+pose.speechBrightness*8).toFixed(1)}%)`);
      frame.style.setProperty('--brites-eye-x', String(pose.eyeScaleX));
      frame.style.setProperty('--brites-eye-y', String(pose.eyeScaleY));
      frame.style.setProperty('--brites-gaze-x', (pose.gazeX * 180).toFixed(2) + 'px');
      frame.style.setProperty('--brites-gaze-y', (-pose.gazeY * 180).toFixed(2) + 'px');
      frame.style.setProperty('--brites-speech-level', String(pose.speechEnergy));
      frame.style.setProperty('--brites-speech-scale', String(1 + pose.speechEnergy * .08));
      frame.style.setProperty('--brites-talk-head', ((pose.headRoll + pose.expressionHeadRoll) * 57.3).toFixed(2) + 'deg');
      frame.style.setProperty('--brites-head-gaze-x', (pose.headYaw * 22).toFixed(2) + 'px');
      frame.style.setProperty('--brites-head-gaze-y', (pose.headPitch * 22).toFixed(2) + 'px');
      frame.style.setProperty('--brites-face-cheek', String(pose.cheekGlow));
      frame.style.setProperty('--brites-face-smile', String(pose.smileCurve));
      const mouthActive = pose.speechSignalValid === true, width = 13 + pose.smileCurve * 4, bend = (pose.smileCurve - .28) * 16, mouthPath = `M${(160-width).toFixed(2)} 145Q160 ${(145+bend).toFixed(2)} ${(160+width).toFixed(2)} 145`;
      speechMouthNode.setAttribute('opacity', mouthActive ? String(.18 + pose.speechEnergy * .52) : '0'); speechMouthNode.setAttribute('d', mouthPath);
      frame.dataset.speechCurveClosed = 'true'; frame.dataset.speechRipple = String(mouthActive);
      speechBandNodes.forEach((node,index) => {const value = mouthActive ? pose.speechBands[index] : 0, x = 146 + index * 5.6, height = value * pose.speechEnergy * 4.8; node.setAttribute('d',`M${x.toFixed(2)} 154Q${(x+2.1).toFixed(2)} ${(154-height).toFixed(2)} ${(x+4.2).toFixed(2)} 154`); node.setAttribute('opacity',value > .002 ? String(.18 + Math.min(.5,value*.45+pose.speechEnergy*.12)) : '0'); node.setAttribute('stroke-width',(1.05+value*.7).toFixed(2));});
      speechRippleNodes.forEach((node,index) => {const offset = 2.8 + index * 2.7 + pose.speechEnergy * (1.1+index*.6), spread = width + 1.5 + index * 2.5 + pose.speechEnergy * 1.2; node.setAttribute('d',`M${(160-spread).toFixed(2)} ${(145+offset).toFixed(2)}Q160 ${(145+bend+offset).toFixed(2)} ${(160+spread).toFixed(2)} ${(145+offset).toFixed(2)}`); node.setAttribute('opacity',mouthActive ? String(pose.speechEnergy*(index ? .12 : .25)) : '0');});
      frame.style.setProperty('--brites-focus-head', (pose.productFocused ? pose.targetX * 8 : 0).toFixed(2) + 'deg');
      frame.style.setProperty('--brites-talk-arm', (-pose.armLiftRight * 75).toFixed(2) + 'deg');
      frame.style.setProperty('--brites-point-left', (pose.armLiftLeft * 75).toFixed(2) + 'deg');
      frame.style.setProperty('--brites-point-right', (-pose.armLiftRight * 75).toFixed(2) + 'deg');
      frame.dataset.productFocused = String(!!productFocus);
      frame.style.setProperty('--brites-performance-duration', (performance?.durationMs || 1050) + 'ms');
      frame.style.setProperty('--brites-performance-intensity', String(performance?.intensity ?? 1));
      frame.dataset.performanceMuted = String(performance?.intensity === 0);
      // Idle acknowledgement starts its CSS fade immediately; appreciation
      // never replaces the speaking eyes or covers measured mouth movement.
      frame.dataset.heart = String(emotion === 'appreciated' && state !== 'speaking');
      frame.dataset.expressionKind = pose.expressionKind || '';
      frame.dataset.faceExpression = pose.faceExpression;
      const browY = 94 - pose.faceBrowLift * 5, browArc = browY - 3 - pose.faceBrowLift * 3, browTilt = pose.faceBrowTilt * 5;
      faceNodes['brow--left'].setAttribute('d', `M115 ${(browY + browTilt * 1.2).toFixed(2)}Q130 ${(browArc + browTilt * .8).toFixed(2)} 145 ${(browY + browTilt * .4).toFixed(2)}`);
      faceNodes['brow--right'].setAttribute('d', `M175 ${(browY - browTilt * .4).toFixed(2)}Q190 ${(browArc - browTilt * .8).toFixed(2)} 205 ${(browY - browTilt * 1.2).toFixed(2)}`);
      faceNodes['smile-signal'].setAttribute('opacity', '1');
      faceNodes['smile-signal'].setAttribute('d', mouthPath);
      for (const [side, center, sign] of [['left',130,-1],['right',190,1]]) {
        const curve=pose.eyeSmile*4.5, tilt=pose.faceBrowTilt*sign*3, half=Math.max(1.1,9.2*(1-pose.lidClosure)*(pose.eyeOpen/1.08));
        faceNodes['ribbon--'+side].setAttribute('d', `M${center-17} ${115-half+tilt}Q${center} ${115-half-curve} ${center+17} ${115-half-tilt}Q${center+22} 115 ${center+17} ${115+half-tilt}Q${center} ${115+half-curve} ${center-17} ${115+half+tilt}Q${center-22} 115 ${center-17} ${115-half+tilt}Z`);
      }
      faceNodes['eye-smile'].setAttribute('opacity', '0');
      faceNodes['cheek--left'].setAttribute('opacity', String(.2 + pose.cheekGlow * .75));
      faceNodes['cheek--right'].setAttribute('opacity', String(.2 + pose.cheekGlow * .75));
      faceNodes['face-signal'].setAttribute('opacity', '0');
      frame.dataset.cue = pose.mannerismCue || '';
      frame.dataset.cuePhase = pose.mannerismPhase || '';
      frame.dataset.motion = canDisplay() && !paused && !reducedMotion ? 'running' : reducedMotion ? 'reduced' : 'paused';
      frame.dataset.fallbackFormat = 'animated-svg-2d';
      frame.dataset.emotion = emotion || MOODS[state].emotion;
      frame.dataset.fallbackAnimated = String(fallbackMoving());
      const statusLabel = state === 'success' && (emotion === 'calm' || emotion === 'reassuring') ? 'Here with you' : MOODS[state].label;
      const pending = !hasWebglFrame() && !fallbackPresented;
      caption.textContent = pending ? '' : statusLabel + (fallbackPresented ? ' \u00b7 2-D companion' : '');
      if(faceSettling || signalSettling) queuePointerFrame();
      scheduleBlink();
      frame.setAttribute('aria-label', 'Brites jewellery gift guide. ' + (pending ? 'Preparing your guide.' : statusLabel) + (fallbackPresented ? '. Animated 2-D companion; 3-D unavailable.' : '') + (paused ? '. Animation paused.' : ''));
    }
    function renderingFailure(reason = 'WebGL rendering is unavailable') {reason = typeof reason === 'string' ? reason : 'WebGL rendering is unavailable'; const old = engine; engine = null; frameReady = false; pendingReadyType = null; fallbackPresented = true; failed = true; failureReason = reason; loading = false; frame.dataset.rendering = 'fallback'; try {old?.destroy();} catch {} finally {surface.replaceChildren();} syncFallback(); emit('fallback', {reason: failureReason, ...snapshot()}); readyResolve(snapshot());}
    function sync() {
      if (!canDisplay() || paused) {stopPointerFrame(); stopBlink(); expression = null; gaze = {x: 0, y: 0}; headGaze = {...gaze}; gazeTarget = {...gaze}; gazeAt = now(); cancelPerformance(); clearProduct(); cancelAppreciation(); cancelMannerism(); productFocus = null; clearSpeechSignal(); speechBeatAt = null; speechRested = true;}
      else if (reducedMotion) {stopPointerFrame(); stopBlink(); expression = null; clearSpeechSignal(); cancelMannerism();}
      frame.hidden = !visible; syncFallback();
      if (engine) {
        try {
          engine.setMotion({active: active(), reducedMotion});
          if (active()) {
            const drawing = engine; drawing.render(poseAt(now()), true);
            if (engine === drawing && !failed && !destroyed) {
              frameReady = true; fallbackPresented = false; syncLayers();
              if (pendingReadyType) {const type = pendingReadyType; pendingReadyType = null; syncFallback(); emit(type, snapshot()); readyResolve(snapshot());}
            }
          } else if (paused && frameReady && canDisplay()) engine.render(poseAt(now()), true);
        } catch {renderingFailure();}
      }
      if (active() && !engine && !loading) load();
    }
    async function load() {
      loading = true; frameReady = false; pendingReadyType = 'ready'; frame.dataset.rendering = 'loading'; surface.replaceChildren();
      try {
        const sceneModule = options.loadScene ? await options.loadScene(moduleUrl) : await import(moduleUrl);
        if (destroyed) return;
        declarations = declaredScene(sceneModule.AVATAR_SCENE_DECLARATIONS, quality.textureSize);
        engine = sceneModule.createAvatarScene({container: surface, quality, onFrame: t => poseAt(t), onError: renderingFailure, onContext: lost => {failed = lost; if (lost) fallbackPresented = true; frameReady = false; pendingReadyType = lost ? null : 'restored'; failureReason = lost ? 'WebGL context was lost' : null; syncFallback(); if (lost) emit('fallback', snapshot()); sync();}});
        if (destroyed) {engine.destroy(); return;}
        failed = false; failureReason = null; loading = false; engine.setFloating?.(floating); if (shownProduct) engine.showProduct?.(shownProduct); fallback.setAttribute('aria-hidden', 'true');
        sync();
      } catch (error) {
        if (destroyed) return;
        renderingFailure();
      } finally {loading = false;}
    }
    function setState(value) {if (destroyed) return; const next = validState(value); if (next === state) return; state = next; expression = null; stateAt = now(); clearSpeechSignal(); speechBeatAt = null; speechRested = true; if (!performance) playMannerism({listening: 'acknowledge', thinking: 'focus', speaking: 'explain', success: 'confirm', error: 'reassure'}[state]); frame.dataset.state = state; try {engine?.invalidate();} catch {renderingFailure();} emit('state', {state}); sync();}
    function setVisible(value) {if (destroyed) return; const opening = value === true && !visible; visible = value === true; if (!visible || opening) greetedThisOpening = false; if (opening && options.greetingOnOpen !== false) triggerGreeting(); sync();}
    // Scene retry is explicit; fallback respects visibility and pause.
    function retry() {if (destroyed || loading || engine || !failed || !visible || !intersecting || doc.hidden) return false; failed = false; sync(); return loading;}
    function setEmotion(value, fromPerformance = false) {if (destroyed) return; if (performance && !fromPerformance) cancelPerformance(false); const next = validEmotion(value); if (next === 'appreciated' && (!canDisplay() || paused)) return; const prior = emotion === 'appreciated' ? previousEmotion : emotion; cancelAppreciation(false); emotion = next; if (next === 'appreciated') {previousEmotion = prior; appreciationAt = now(); const epoch = appreciationEpoch; appreciationTimer = win.setTimeout(() => {if (destroyed || epoch !== appreciationEpoch) return; appreciationTimer = null; emotion = previousEmotion; previousEmotion = null; sync();}, 1450); playMannerism('reassure');} if ((emotion === 'calm' || emotion === 'reassuring') && mannerism === 'confirm') playMannerism('reassure'); sync(); emit('emotion', {emotion});}
    function setPaused(value) {if (destroyed) return; paused = value === true; sync(); emit('pause', {paused});}
    function setReducedMotion(value) {if (destroyed) return false; manualReducedMotion = value === true; reducedMotion = systemReducedMotion || manualReducedMotion; sync(); emit('motion-preference', {reducedMotion}); return reducedMotion;}
    function setExpression(value) {if (destroyed) return false; const next = value === null ? null : validateExpression(value); if (value !== null && (!next || !canDisplay() || paused || reducedMotion)) return false; expression = next; sync(); emit('expression', next ? {...next} : null); return true;}
    function stopBlink() {if (blinkTimer !== null) win.clearTimeout(blinkTimer); blinkTimer = null;}
    function scheduleBlink() {
      if (!fallbackMoving()) {stopBlink(); return;} if (blinkTimer !== null) return;
      const phase = ((now() % BLINK_CYCLE) + BLINK_CYCLE) % BLINK_CYCLE, within = BLINK_EVENTS.some(event => phase >= event.at && phase < event.at + event.duration), next = BLINK_EVENTS.find(event => event.at > phase);
      const delay = within ? 16 : Math.max(16, ((next ? next.at : BLINK_CYCLE + BLINK_EVENTS[0].at) - phase) * 1000);
      blinkTimer = win.setTimeout(() => {blinkTimer = null; if (fallbackMoving()) syncFallback();}, delay);
    }
    function stopPointerFrame() {if (pointerFrame !== null) win.cancelAnimationFrame?.(pointerFrame); pointerFrame = null;}
    function queuePointerFrame() {
      if (pointerFrame !== null || !canDisplay() || paused || reducedMotion || !win.requestAnimationFrame) return;
      if (!faceSettling && !signalSettling && !['x', 'y'].some(axis => Math.abs(gaze[axis] - gazeTarget[axis]) > .0005 || Math.abs(headGaze[axis] - gazeTarget[axis]) > .0005)) return;
      pointerFrame = win.requestAnimationFrame(() => {
        pointerFrame = null;
        if (!canDisplay() || paused || reducedMotion) return;
        syncFallback();
        if (faceSettling || signalSettling || ['x', 'y'].some(axis => Math.abs(gaze[axis] - gazeTarget[axis]) > .0005 || Math.abs(headGaze[axis] - gazeTarget[axis]) > .0005)) queuePointerFrame();
      });
    }
    function lookAt(x, y, soften = false) {
      if (!canDisplay() || paused) return;
      gazeTarget = {x: clamp(Number.isFinite(x) ? x : 0, -1, 1), y: clamp(Number.isFinite(y) ? y : 0, -1, 1)};
      if (!soften || reducedMotion) {stopPointerFrame(); gaze = {...gazeTarget}; headGaze = {...gazeTarget}; gazeAt = now();}
      syncFallback(); if (reducedMotion) sync(); else if (soften) queuePointerFrame();
    }
    function focusProduct(value = {}) {
      if (destroyed || !canDisplay() || paused || !Number.isFinite(value.x) || !Number.isFinite(value.y)) return false;
      const x = clamp(value.x, -1, 1), y = clamp(value.y, -1, 1);
      if (productFocus && Math.abs(productFocus.x - x) < .025 && Math.abs(productFocus.y - y) < .025) return true;
      productFocus = {x, y, at: now()};
      playMannerism('explain'); sync(); emit('product-focus', {...productFocus}); return true;
    }
    function perform(value) {
      const plan = validateAvatarPerformance(value); if (!plan || destroyed || !canDisplay() || paused) return false;
      cancelPerformance(); performancePreviousEmotion = emotion === 'appreciated' ? previousEmotion : emotion; performance = plan; const epoch = performanceEpoch;
      if (emotion === 'appreciated' && plan.mood !== 'appreciated') previousEmotion = plan.mood; else if (emotion !== 'appreciated' || plan.mood !== 'appreciated') setEmotion(plan.mood, true);
      frame.dataset.performance = plan.gesture;
      if (plan.gesture !== 'none') playMannerism(plan.gesture === 'present' ? 'explain' : plan.gesture, plan.durationMs);
      performanceTimer = win.setTimeout(() => {if (destroyed || epoch !== performanceEpoch) return; cancelPerformance(); sync();}, plan.durationMs);
      sync(); emit('performance', {...plan}); return true;
    }
    function setFloating(value) {if (destroyed) return; floating = value === true; frame.dataset.floating = String(floating); try {engine?.setFloating?.(floating);} catch {renderingFailure();}}
    function clearProduct() {productEpoch++; shownProduct = null; productCard.hidden = true; productImage.onerror = null; productImage.removeAttribute('src'); try {engine?.clearProduct?.();} catch {}}
    function showProduct(value) {
      const photo = productPhoto(value); if (!photo) {clearProduct(); return false;} if (destroyed || !canDisplay() || paused) return false;
      if (shownProduct?.id === photo.id && shownProduct.image === photo.image) return true;
      clearProduct(); shownProduct = photo; const epoch = productEpoch;
      productCard.hidden = false; productCard.prepend(productImage); productImage.alt = photo.title + '. Product photo; not a virtual try-on.'; productLabel.textContent = 'Product photo';
      productImage.onerror = () => {if (epoch !== productEpoch || destroyed) return; productImage.removeAttribute('src'); productLabel.textContent = 'Product photo unavailable';}; productImage.src = photo.image;
      focusProduct({x: .72, y: -.35});
      if (engine?.showProduct) Promise.resolve(engine.showProduct(photo)).then(ok => {if (epoch !== productEpoch || destroyed) return; frame.dataset.productTexture = ok ? 'ready' : 'unavailable';}).catch(() => {if (epoch === productEpoch && !destroyed) frame.dataset.productTexture = 'unavailable';});
      emit('product-showcase', {id: photo.id, handle: photo.handle, format: 'product-photo'}); return true;
    }
    function clearFocus() {if (destroyed) return; if (productFocus && mannerism === 'explain') cancelMannerism(); productFocus = null; sync();}
    function cue(name) {if (destroyed || !canDisplay() || paused || reducedMotion) return false; const key = name === 'present' ? 'explain' : name; if (!Object.hasOwn(MANNERISMS, key)) return false; playMannerism(key); sync(); return mannerism === key;}
    function setSpeechSignal(value) {if (destroyed) return false; const next = value === null ? EMPTY_SPEECH_SIGNAL : validateSpeechSignal(value); if (!next || !next.valid || next.amplitude === 0 || state !== 'speaking' || paused || reducedMotion || !canDisplay()) {if (clearSpeechSignal()) sync(); return value === null || !!next && (!next.valid || next.amplitude === 0);}
      speechSignal = next; level = next.amplitude; if (level < .045) speechRested = true; else if (level > .1 && speechRested && (speechBeatAt === null || now()-speechBeatAt > .42)) {speechBeatAt = now(); speechRested = false;} syncFallback(); return true;}
    function setLevel(value) {if (destroyed) return; const amplitude = clamp(value, 0, 1); setSpeechSignal({amplitude, bands: [...EMPTY_SPEECH_SIGNAL.bands], brightness: 0, valid: amplitude > 0});}
    const visibility = () => sync(), motion = event => {systemReducedMotion = event.matches === true; reducedMotion = systemReducedMotion || manualReducedMotion; sync();};
    function currentGazeAnchor(box) {
      try {const projected = !failed && engine?.gazeAnchor?.(); if (projected && Number.isFinite(projected.x) && Number.isFinite(projected.y)) return projected;} catch {}
      const svgBox = fallback.querySelector('svg').getBoundingClientRect();
      const artWidth = svgBox.width > 0 ? svgBox.width : Math.min(box.width, 380), artHeight = svgBox.height > 0 ? svgBox.height : Math.min(box.height, 460);
      const scale = Math.min(artWidth / 320, artHeight / 280), left = svgBox.width > 0 ? svgBox.left : box.left + (box.width - artWidth) / 2, top = svgBox.height > 0 ? svgBox.top : box.top + (box.height - artHeight) / 2;
      return {x: (left - box.left + artWidth / 2) / box.width, y: (top - box.top + (artHeight - 280 * scale) / 2 + 118 * scale) / box.height};
    }
    const pointer = event => {
      if (!canDisplay() || paused || event.pointerType === 'touch') return;
      const box = frame.getBoundingClientRect(); if (!(box.width > 0) || !(box.height > 0)) return;
      const next = pointerGaze(event.clientX, event.clientY, box, currentGazeAnchor(box));
      if (next) lookAt(next.x, next.y, true);
    };
    const resetGaze = () => {if (canDisplay() && !paused) lookAt(0, 0, true);};
    const leavePage = event => {if (event.relatedTarget == null) resetGaze();};
    let observer = null;
    constructionCleanup = destroy;
    doc.addEventListener('visibilitychange', visibility); media?.addEventListener?.('change', motion);
    doc.addEventListener('pointermove', pointer, {passive: true, capture: true}); doc.addEventListener('pointerout', leavePage, {passive: true}); win.addEventListener('blur', resetGaze);
    observer = win.IntersectionObserver ? new win.IntersectionObserver(entries => {intersecting = entries.some(entry => entry.isIntersecting); sync();}, {threshold: 0}) : null;
    observer?.observe(frame);
    function destroy() {if (destroyed) return; destroyed = true; expression = null; clearSpeechSignal(); stopPointerFrame(); stopBlink(); cancelPerformance(); clearProduct(); cancelAppreciation(); cancelMannerism(); observer?.disconnect(); doc.removeEventListener('visibilitychange', visibility); media?.removeEventListener?.('change', motion); doc.removeEventListener('pointermove', pointer, true); doc.removeEventListener('pointerout', leavePage); win.removeEventListener('blur', resetGaze); try {engine?.destroy();} catch {} engine = null; frame.remove(); readyResolve(snapshot());}
    if (visible && options.greetingOnOpen !== false) triggerGreeting();
    sync(); if (options.emotion === 'appreciated') setEmotion('appreciated');
    return {ready, triggerGreeting, setState, setVisible, setPaused, setReducedMotion, setEmotion, setExpression, retry, setLevel, setSpeechSignal, lookAt, focusProduct, clearFocus, showProduct, clearProduct, setFloating, perform, cancelPerformance: () => {if (destroyed) return; cancelPerformance(); sync();}, cue, snapshot, destroy, element: frame};
    } catch (error) {
      try {constructionCleanup?.();} catch {} finally {frame.remove();}
      throw error;
    }
  }
  const api = {create, MANNERISMS, BEHAVIOR_CUES, BLINK_EVENTS, BLINK_CYCLE, blinkFor, mannerismFor, FACE_EXPRESSIONS, faceFor, EXPRESSION_KINDS, validateExpression, validateSpeechSignal, pointerGaze, STATES, EMOTIONS, validEmotion, validState, qualityFor, poseFor, productPhoto, validateAvatarPerformance, PERFORMANCE_GESTURES};
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  if (scope) scope.BritesConciergeAvatar = api;
})(typeof window === 'undefined' ? null : window);
