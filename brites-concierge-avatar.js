(function (scope) {
  'use strict';
  const STATES = Object.freeze(['idle', 'listening', 'thinking', 'speaking', 'success', 'error']);
  const clamp = (value, low, high) => Math.max(low, Math.min(high, Number.isFinite(value) ? value : low));
  const validState = state => STATES.includes(state) ? state : 'idle';
  const MOODS = Object.freeze({
    idle: {color: '#4aa8ff', emotion: 'welcoming', label: 'Ready to help'},
    listening: {color: '#49c9ff', emotion: 'attentive', label: 'Listening'},
    thinking: {color: '#ab87ff', emotion: 'curious', label: 'Thinking'},
    speaking: {color: '#ffcb79', emotion: 'explaining', label: 'Speaking'},
    success: {color: '#72ddd1', emotion: 'delighted', label: 'Happy to help'},
    error: {color: '#ffc28e', emotion: 'considerate', label: 'Let’s try another way'}
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
  function validateAvatarPerformance(value) {
    try {if (!value || typeof value !== 'object' || Array.isArray(value) || Object.keys(value).length !== 4 || !Object.keys(value).every(key => ['mood','gesture','intensity','durationMs'].includes(key)) || !EMOTIONS.includes(value.mood) || !PERFORMANCE_GESTURES.includes(value.gesture) || !Number.isFinite(value.intensity) || value.intensity < 0 || value.intensity > 1 || !Number.isInteger(value.durationMs) || value.durationMs < 400 || value.durationMs > 2500) return null;
      return Object.freeze({mood: value.mood, gesture: value.gesture, intensity: value.intensity, durationMs: value.durationMs});} catch {return null;}
  }
  let instanceCount = 0;
  function declaredScene(raw, textureSize) {
    if (!raw || ![1, 2].includes(raw.schema) || !Array.isArray(raw.textures) || raw.textures.length < 1 || raw.textures.length > 16) return null;
    const textures = raw.textures.map(value => ({name: typeof value?.name === 'string' ? value.name.slice(0, 80) : '', kind: typeof value?.kind === 'string' ? value.kind.slice(0, 32) : '', width: textureSize, height: textureSize}));
    if (textures.some(value => !value.name || !value.kind)) return null;
    return {schema: raw.schema, source: 'loaded_scene_module', textures};
  }
  function qualityFor(hints = {}) {
    const mobile = hints.mobile === true || (hints.width > 0 && hints.width < 600) || (hints.memory > 0 && hints.memory <= 4);
    return {name: mobile ? 'adaptive' : 'high', pixelRatio: Math.min(mobile ? 1.5 : 2, Math.max(1, hints.pixelRatio || 1)), shadowSize: mobile ? 1024 : 2048, textureSize: mobile ? 1024 : 2048, fps: mobile ? 30 : 60, geometryScale: mobile ? .8 : 1, bloom: !mobile && hints.bloom !== false};
  }
  function poseFor({state = 'idle', time = 0, elapsed = 0, level = 0, gaze = {x: 0, y: 0}, reducedMotion = false, emotion = null, mannerism = null, mannerismElapsed = 0, productFocus = null, speechBeatElapsed = null, appreciationElapsed = 0, performance = null, mannerismDurationMs = null} = {}) {
    state = validState(state); time = reducedMotion ? 0 : Number.isFinite(time) ? time : 0; elapsed = Number.isFinite(elapsed) ? Math.max(0, elapsed) : 0;
    gaze = gaze && typeof gaze === 'object' ? gaze : {}; const gazeX = Number.isFinite(gaze.x) ? gaze.x : 0, gazeY = Number.isFinite(gaze.y) ? gaze.y : 0;
    const plan = validateAvatarPerformance(performance), emotionalGain = plan ? plan.intensity : 1;
    emotion = validEmotion(emotion); const warm = emotion === 'warm', curious = emotion === 'curious'; const calm = emotion === 'calm' || emotion === 'reassuring' || emotion === 'appreciated';
    const motion = reducedMotion ? 0 : calm ? .4 : 1, talking = state === 'speaking', thoughtful = state === 'thinking', listening = state === 'listening', happy = state === 'success' && !calm, concerned = state === 'error';
    const blink = motion ? blinkFor(time) : 0;
    // Speech expression follows native audio energy.
    const audioEnergy = clamp(level, 0, 1), speech = talking ? (reducedMotion ? .3 : .1 + .82 * audioEnergy) : 0;
    const expressive = mannerismFor({name: mannerism, elapsed: mannerismElapsed, reducedMotion, emotion: emotion === 'appreciated' ? 'reassuring' : emotion, intensity: emotionalGain, durationMs: mannerismDurationMs});
    const heart = emotion === 'appreciated' ? reducedMotion ? 1 : smooth(appreciationElapsed / .18) * (1 - smooth((appreciationElapsed - 1.17) / .28)) : 0;
    const lift = expressive.active ? expressive.lift : happy && motion ? Math.sin(Math.min(elapsed, 1.2) / 1.2 * Math.PI) * .12 : 0;
    const greeting = expressive.name === 'greet', acknowledgement = expressive.name === 'acknowledge', focus = expressive.name === 'focus';
    const apertureAccent = expressive.listen * .035 - expressive.think * .055 + expressive.speak * .025 - expressive.comfort * .018 + expressive.celebrate * .06;
    const stateEnergy = talking ? speech : thoughtful ? (reducedMotion ? .22 : .22 + .08 * Math.sin(time * 2.15)) : listening ? (reducedMotion ? .1 : .1 + .035 * Math.sin(time * 1.05)) : happy ? .34 : concerned ? .08 : .035;
    const attentionDriftX = motion ? Math.sin(time * .73) * (listening ? .012 : thoughtful ? .014 : .009) : 0;
    const attentionDriftY = motion && (thoughtful || state === 'idle') ? Math.sin(time * .47 + .8) * .008 : 0;
    // Real audio weights a bounded phrase gesture, including soft rests.
    const phraseGesture = talking && motion ? (Number.isFinite(speechBeatElapsed) ? pulse(speechBeatElapsed, .025, .68) : Math.max(0, Math.sin(time * 1.8 + .4)) ** 2) * audioEnergy : 0;
    const product = productFocus && typeof productFocus === 'object' ? {x: clamp(productFocus.x, -1, 1), y: clamp(productFocus.y, -1, 1)} : null;
    const present = product && !reducedMotion ? expressive.offer * (calm ? .3 : 1) : 0;
    const focusAge = product ? (Number.isFinite(productFocus.elapsed) ? Math.max(0, productFocus.elapsed) : 1) : 0;
    const eyeFollow = product ? reducedMotion ? 1 : smooth(focusAge / .18) : 1;
    const headFollow = product ? reducedMotion ? 1 : smooth((focusAge - .1) / .38) : 1;
    const targetX = product ? product.x * headFollow : gazeX, targetY = product ? product.y * headFollow : gazeY;
    const speechAccent = talking && motion ? audioEnergy * (.35 + .65 * Math.max(0, Math.sin(time * 2.1 - .3))) : 0;
    const helloWave = greeting && motion ? Math.sin(mannerismElapsed * 15) * expressive.body : 0;
    const leftArm = calm ? .025 : thoughtful ? .18 : happy ? .1 : listening ? .045 : 0;
    const rightArm = calm ? .025 : thoughtful ? .25 : happy ? .16 : listening ? .07 : 0;
    return {
      mannerism: expressive.name, mannerismCue: expressive.cue, mannerismPhase: expressive.phase, mannerismActive: expressive.active, nod: expressive.nod, offer: expressive.offer, helloWave, speechEnergy: audioEnergy, phraseGesture, productFocused: !!product, present, targetX: product?.x || 0, targetY: product?.y || 0, speechAccent, lean: expressive.body * (acknowledgement ? .04 : .02) + phraseGesture * .018,
      state, heart, stanceScale: 1 + (plan?.gesture === 'present' ? expressive.body * .055 : plan?.gesture === 'focus' ? -expressive.body * .035 : 0), bodyDepth: plan?.gesture === 'present' ? expressive.body * .13 : plan?.gesture === 'focus' ? -expressive.body * .09 : 0, bodyYaw: expressive.celebrate * .38 * motion, eyeColor: heart > .01 ? '#ed93aa' : MOODS[state].color, emotion: emotion || MOODS[state].emotion, blink,
      eyeDeformation: speechAccent * .075 + (warm ? .1 : curious ? -.055 : emotion === 'reassuring' ? .05 : 0) * emotionalGain + (calm ? -.025 : happy ? .24 : thoughtful ? -.14 : listening ? .1 : concerned ? -.08 : greeting ? expressive.eye * .1 : 0) + apertureAccent,
      eyeScaleX: (curious ? 1 - .04 * emotionalGain : 1) * (happy ? 1.08 : thoughtful ? .93 : 1) + expressive.comfort * .025,
      eyeScaleY: (warm || emotion === 'reassuring' ? 1 - .06 * emotionalGain : curious ? 1 + .035 * emotionalGain : 1) * (happy ? .82 : listening ? 1.06 : concerned ? .9 : 1) + expressive.listen * .025 - expressive.speak * .015,
      ringRotation: thoughtful ? time * .18 : expressive.celebrate * .18 - expressive.comfort * .06,
      ringRipple: clamp(stateEnergy + expressive.listen * .08 + expressive.think * .14 + expressive.speak * .18 + expressive.celebrate * .16, 0, 1),
      antennaTilt: (thoughtful ? -.14 : listening ? .08 : happy ? .18 : 0) + expressive.anticipate * .055 + expressive.comfort * -.035,
      bob: Math.sin(time * 1.35) * .045 * motion + lift + phraseGesture * .02,
      bodyRoll: Math.sin(time * .7) * .018 * motion,
      headYaw: clamp(clamp(targetX, -1, 1) * .1 + (greeting ? -.035 * expressive.head : 0) + (thoughtful ? Math.sin(time * .65) * .055 * motion : state === 'idle' ? Math.sin(time * .48) * .038 * motion : 0) - (product ? 0 : expressive.anticipate * .012), -.1, .1),
      headPitch: clamp((emotion === 'reassuring' ? expressive.comfort * .04 : 0) + clamp(targetY, -1, 1) * .065 + expressive.nod * .1 + expressive.head * (acknowledgement ? -.045 : focus ? .035 : 0) + (listening ? -.055 : 0) + (talking ? Math.sin(time * 2.5) * .038 * motion * audioEnergy : state === 'idle' ? Math.sin(time * .85 + .3) * .026 * motion : 0), -.09, .09),
      headRoll: (curious ? .105 : warm ? -.038 : 0) * emotionalGain + (calm ? -.012 : concerned ? -.055 : thoughtful ? .075 : listening ? -.065 : happy ? .025 : 0) + expressive.head * (greeting ? -.09 : acknowledgement ? -.055 : focus ? .035 : 0) + (state === 'idle' ? Math.sin(time * .61) * .028 * motion : 0),
      eyeOpen: Math.max(.035, (happy ? .7 : concerned ? .85 : listening ? 1.06 : greeting ? 1 - expressive.eye * .12 : 1) * (1 - blink)),
      gazeX: clamp(product ? product.x * eyeFollow : targetX, -1, 1) * (product ? .1 : .045) + (thoughtful ? -.035 : 0) + (product ? 0 : attentionDriftX),
      gazeY: clamp(product ? product.y * eyeFollow : targetY, -1, 1) * (product ? .07 : .038) + (thoughtful ? .04 : 0) + (product ? 0 : attentionDriftY),
      browLift: happy ? .08 : listening ? .045 : concerned ? .025 : thoughtful ? .02 : 0,
      browAngle: concerned ? .15 : thoughtful ? -.08 : happy ? -.08 : -.025,
      mouth: talking ? 'open' : concerned ? 'concern' : 'smile', mouthOpen: speech,
      armLift: Math.max(leftArm, rightArm), armLiftLeft: leftArm + expressive.comfort * .02 + phraseGesture * .09 + (product && product.x < 0 ? present * .48 : 0), armLiftRight: rightArm + expressive.speak * .1 + expressive.celebrate * .08 + phraseGesture * .32 + (product && product.x >= 0 ? present * .48 : 0),
      gesture: expressive.offer * .075,
      armReach: present * .32,
      statusWave: expressive.listen * .2 + expressive.think * .45 + expressive.speak * .65 + expressive.comfort * .15 + expressive.celebrate * .8,
      lightPulse: thoughtful ? (reducedMotion ? .3 : .18 + .18 * Math.sin(time * 2)) : happy ? .45 : talking ? speech * .22 : expressive.anticipate * .08
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
    const surface = doc.createElement('div'); surface.className = 'brites-avatar__surface';
    const fallback = doc.createElement('div'); fallback.className = 'brites-avatar__fallback';
    const id = 'britesRobot' + (++instanceCount);
    fallback.innerHTML = `<svg viewBox="0 0 320 280" aria-hidden="true" xmlns="http://www.w3.org/2000/svg"><defs><linearGradient id="${id}Ivory" x2=".4" y2="1"><stop stop-color="#fffefa"/><stop offset="1" stop-color="#d9dfe4"/></linearGradient><linearGradient id="${id}Gold" x2=".2" y2="1"><stop stop-color="#f2dda8"/><stop offset=".5" stop-color="#ac824b"/><stop offset="1" stop-color="#e4c78e"/></linearGradient><radialGradient id="${id}Glass"><stop stop-color="#1e3848"/><stop offset="1" stop-color="#09121d"/></radialGradient></defs><ellipse class="brites-avatar__shadow" cx="160" cy="246" rx="53" ry="8" fill="#243745" opacity=".13"/><g class="brites-avatar__robot"><g class="brites-avatar__antenna"><path d="M160 63V40" stroke="url(#${id}Gold)" stroke-width="5" stroke-linecap="round"/><circle cx="160" cy="36" r="5" class="brites-avatar__signal"/></g><ellipse cx="160" cy="197" rx="34" ry="38" fill="url(#${id}Ivory)" stroke="#ccd5dc"/><path d="M131 214Q160 226 189 214" fill="none" stroke="url(#${id}Gold)" stroke-width="4"/><path d="M154 188l6-7 6 7-6 8z" fill="url(#${id}Gold)"/><g class="brites-avatar__arm brites-avatar__arm--left"><rect x="107" y="180" width="14" height="31" rx="7" fill="url(#${id}Ivory)" stroke="#ccd5dc"/></g><g class="brites-avatar__arm brites-avatar__arm--right"><rect x="199" y="180" width="14" height="31" rx="7" fill="url(#${id}Ivory)" stroke="#ccd5dc"/></g><g class="brites-avatar__head"><circle cx="160" cy="118" r="66" fill="url(#${id}Ivory)" stroke="#ccd5dc"/><circle cx="160" cy="118" r="55" fill="url(#${id}Gold)"/><circle cx="160" cy="118" r="51" fill="url(#${id}Glass)"/><circle class="brites-avatar__halo" cx="160" cy="118" r="40" fill="none" stroke-width="2" stroke-dasharray="185 66"/><g class="brites-avatar__eye-gaze"><g class="brites-avatar__eye"><circle cx="160" cy="118" r="28" fill="none" stroke-width="10"/><circle cx="160" cy="118" r="15" fill="none" stroke-width="1.4" opacity=".36"/><circle cx="176" cy="102" r="3" fill="#eefaff" opacity=".9"/></g><path class="brites-avatar__heart" d="M160 138C154 132 135 121 135 109C135 95 152 92 160 104C168 92 185 95 185 109C185 121 166 132 160 138Z"/></g><path d="M134 83Q145 77 155 78" fill="none" stroke="#fff" opacity=".16" stroke-width="3" stroke-linecap="round"/></g></g></svg>`;
    const caption = doc.createElement('div'); caption.className = 'brites-avatar__caption';
    caption.setAttribute('aria-hidden', 'true');
    caption.textContent = MOODS[validState(options.initialState)].label;
    const style = doc.createElement('link'); style.rel = 'stylesheet'; style.href = cssUrl;
    const productCard = doc.createElement('figure'); productCard.className = 'brites-avatar__product'; productCard.hidden = true;
    const productImage = doc.createElement('img'); productImage.alt = ''; productImage.referrerPolicy = 'no-referrer'; productImage.crossOrigin = 'anonymous';
    const productLabel = doc.createElement('figcaption'); productLabel.textContent = 'Product photo'; productCard.append(productLabel);
    frame.append(style, surface, fallback, caption, productCard); container.appendChild(frame);
    let state = validState(options.initialState), visible = options.visible === true, intersecting = true, destroyed = false, loading = false, engine = null, declarations = null, failed = false, paused = options.paused === true, emotion = options.emotion === 'appreciated' ? null : validEmotion(options.emotion), failureReason = null, level = 0, gaze = {x: 0, y: 0}, stateAt = 0, mannerism = null, mannerismAt = 0, mannerismId = 0, mannerismTimer = null, greetedThisOpening = false, productFocus = null, speechBeatAt = null, speechRested = true, appreciationAt = 0, appreciationTimer = null, appreciationEpoch = 0, previousEmotion = null, shownProduct = null, productEpoch = 0, floating = false, performance = null, performanceTimer = null, performanceEpoch = 0, performancePreviousEmotion = null, mannerismDurationMs = null;
    const media = win.matchMedia ? win.matchMedia('(prefers-reduced-motion: reduce)') : null;
    let reducedMotion = !!media?.matches, readyResolve;
    const ready = new Promise(resolve => {readyResolve = resolve;});
    const quality = qualityFor({width: win.innerWidth, mobile: options.mobile, memory: win.navigator?.deviceMemory, pixelRatio: win.devicePixelRatio, bloom: options.bloom});
    const now = () => {const stamp = win.performance?.now?.(); return (Number.isFinite(stamp) ? stamp : Date.now()) / 1000;};
    stateAt = now();
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
    // One optional shopper-initiated greeting per opening.
    function triggerGreeting() {if (destroyed || greetedThisOpening || !canDisplay() || paused || reducedMotion) return false; playMannerism('greet'); greetedThisOpening = mannerism === 'greet'; sync(); return greetedThisOpening;}
    function poseAt(time, fallbackTarget = false) {return poseFor({state, time, elapsed: time - stateAt, level, gaze, productFocus: productFocus ? {...productFocus, elapsed: fallbackTarget ? Math.max(.44, time - productFocus.at) : time - productFocus.at} : null, reducedMotion, emotion, mannerism, mannerismElapsed: fallbackTarget && productFocus && mannerism === 'explain' ? Math.max(.44, time - mannerismAt) : time - mannerismAt, speechBeatElapsed: speechBeatAt === null ? -1 : time - speechBeatAt, appreciationElapsed: time - appreciationAt, performance, mannerismDurationMs});}
    function emit(type, detail) {frame.dispatchEvent(new win.CustomEvent('brites-avatar:' + type, {detail, bubbles: true, composed: true})); try {options.onStatus?.(type, detail);} catch {}}
    function canDisplay() {return visible && intersecting && !doc.hidden && !destroyed;}
    function active() {return canDisplay() && !paused && !failed;}
    function fallbackMoving() {return canDisplay() && !paused && !reducedMotion && (!engine || failed);}
    function snapshot() {
      const scene = engine?.snapshot?.() || {};
      return {...scene, state, emotion, performance: performance ? {...performance} : null, floating, shownProduct: shownProduct ? {id: shownProduct.id, handle: shownProduct.handle, title: shownProduct.title, format: 'verified-product-photo'} : null, productFocus: productFocus ? {...productFocus} : null, visible, intersecting, paused, reducedMotion, mode: engine && !failed ? 'webgl' : failed ? 'fallback' : 'pending', loading, destroyed,
        mannerism: {name: mannerism, cue: BEHAVIOR_CUES[mannerism] || null, id: mannerismId, duration: mannerismDurationMs ? mannerismDurationMs / 1000 : MANNERISMS[mannerism] || 0, active: !!mannerism && canDisplay() && !paused && !reducedMotion, elapsed: mannerism ? Math.max(0, now() - mannerismAt) : 0},
        animated: engine && !failed ? active() && !reducedMotion && scene.animated === true : fallbackMoving(),
        fallback: {format: 'animated_svg_2d', active: canDisplay() && (!engine || failed), animated: fallbackMoving(), reason: failureReason},
        quality: {...quality}, declarations: declarations ? {schema: declarations.schema, source: declarations.source, textures: declarations.textures.map(value => ({...value}))} : null};
    }
    function syncFallback() {
      const pose = poseAt(now(), !engine || failed);
      frame.style.setProperty('--brites-eye-color', emotion === 'appreciated' ? '#ed93aa' : pose.eyeColor);
      frame.style.setProperty('--brites-eye-x', String(pose.eyeScaleX));
      frame.style.setProperty('--brites-eye-y', String(pose.eyeScaleY));
      frame.style.setProperty('--brites-gaze-x', (pose.gazeX * 180).toFixed(2) + 'px');
      frame.style.setProperty('--brites-gaze-y', (-pose.gazeY * 180).toFixed(2) + 'px');
      frame.style.setProperty('--brites-speech-level', String(level));
      frame.style.setProperty('--brites-speech-scale', String(1 + level * .14));
      frame.style.setProperty('--brites-talk-head', (pose.headRoll * 57.3).toFixed(2) + 'deg');
      frame.style.setProperty('--brites-focus-head', (pose.productFocused ? pose.targetX * 8 : 0).toFixed(2) + 'deg');
      frame.style.setProperty('--brites-talk-arm', (-pose.armLiftRight * 75).toFixed(2) + 'deg');
      frame.style.setProperty('--brites-point-left', (pose.armLiftLeft * 75).toFixed(2) + 'deg');
      frame.style.setProperty('--brites-point-right', (-pose.armLiftRight * 75).toFixed(2) + 'deg');
      frame.dataset.productFocused = String(!!productFocus);
      frame.style.setProperty('--brites-performance-duration', (performance?.durationMs || 1050) + 'ms');
      frame.style.setProperty('--brites-performance-intensity', String(performance?.intensity ?? 1));
      frame.dataset.performanceMuted = String(performance?.intensity === 0);
      frame.dataset.heart = String(emotion === 'appreciated');
      frame.dataset.cue = pose.mannerismCue || '';
      frame.dataset.cuePhase = pose.mannerismPhase || '';
      frame.dataset.motion = canDisplay() && !paused && !reducedMotion ? 'running' : reducedMotion ? 'reduced' : 'paused';
      frame.dataset.fallbackFormat = 'animated-svg-2d';
      frame.dataset.emotion = emotion || MOODS[state].emotion;
      frame.dataset.fallbackAnimated = String(fallbackMoving());
      const statusLabel = state === 'success' && (emotion === 'calm' || emotion === 'reassuring') ? 'Here with you' : MOODS[state].label;
      caption.textContent = statusLabel + (failed ? ' · 2-D companion' : '');
      frame.setAttribute('aria-label', 'Brites jewellery gift guide. ' + statusLabel + (failed ? '. Animated 2-D companion; 3-D unavailable.' : '') + (paused ? '. Animation paused.' : ''));
    }
    function renderingFailure(reason = 'WebGL rendering is unavailable') {reason = typeof reason === 'string' ? reason : 'WebGL rendering is unavailable'; const old = engine; engine = null; failed = true; failureReason = reason; loading = false; frame.dataset.rendering = 'fallback'; try {old?.destroy();} catch {} syncFallback(); emit('fallback', {reason: failureReason, ...snapshot()}); readyResolve(snapshot());}
    function sync() {if (!canDisplay() || paused) {cancelPerformance(); clearProduct(); cancelAppreciation(); cancelMannerism(); productFocus = null; level = 0; speechBeatAt = null; speechRested = true;} else if (reducedMotion) cancelMannerism(); frame.hidden = !visible; syncFallback(); if (engine) {try {engine.setMotion({active: active(), reducedMotion}); if (active()) engine.render(poseAt(now()), true);} catch {renderingFailure();}} if (active() && !engine && !loading) load();}
    async function load() {
      loading = true; frame.dataset.rendering = 'loading';
      try {
        const sceneModule = options.loadScene ? await options.loadScene(moduleUrl) : await import(moduleUrl);
        if (destroyed) return;
        declarations = declaredScene(sceneModule.AVATAR_SCENE_DECLARATIONS, quality.textureSize);
        engine = sceneModule.createAvatarScene({container: surface, quality, onFrame: t => poseAt(t), onError: renderingFailure, onContext: lost => {failed = lost; failureReason = lost ? 'WebGL context was lost' : null; frame.dataset.rendering = lost ? 'fallback' : 'webgl'; syncFallback(); emit(lost ? 'fallback' : 'restored', snapshot()); sync();}});
        if (destroyed) {engine.destroy(); return;}
        failed = false; failureReason = null; loading = false; frame.dataset.rendering = 'webgl'; engine.setFloating?.(floating); if (shownProduct) engine.showProduct?.(shownProduct); fallback.setAttribute('aria-hidden', 'true');
        emit('ready', snapshot()); readyResolve(snapshot()); sync();
      } catch (error) {
        if (destroyed) return;
        renderingFailure();
      } finally {loading = false;}
    }
    function setState(value) {if (destroyed) return; const next = validState(value); if (next === state) return; state = next; stateAt = now(); if (next !== 'speaking') level = 0; speechBeatAt = null; speechRested = true; if (!performance) playMannerism({listening: 'acknowledge', thinking: 'focus', speaking: 'explain', success: 'confirm', error: 'reassure'}[state]); frame.dataset.state = state; try {engine?.invalidate();} catch {renderingFailure();} emit('state', {state}); sync();}
    function setVisible(value) {if (destroyed) return; const opening = value === true && !visible; visible = value === true; if (!visible || opening) greetedThisOpening = false; if (opening && options.greetingOnOpen !== false) triggerGreeting(); sync();}
    // Scene retry is explicit; fallback respects visibility and pause.
    function retry() {if (destroyed || loading || engine || !failed || !visible || !intersecting || doc.hidden) return false; failed = false; sync(); return loading;}
    function setEmotion(value, fromPerformance = false) {if (destroyed) return; if (performance && !fromPerformance) cancelPerformance(false); const next = validEmotion(value); if (next === 'appreciated' && (!canDisplay() || paused)) return; const prior = emotion === 'appreciated' ? previousEmotion : emotion; cancelAppreciation(false); emotion = next; if (next === 'appreciated') {previousEmotion = prior; appreciationAt = now(); const epoch = appreciationEpoch; appreciationTimer = win.setTimeout(() => {if (destroyed || epoch !== appreciationEpoch) return; appreciationTimer = null; emotion = previousEmotion; previousEmotion = null; sync();}, 1450); playMannerism('reassure');} if ((emotion === 'calm' || emotion === 'reassuring') && mannerism === 'confirm') playMannerism('reassure'); sync(); emit('emotion', {emotion});}
    function setPaused(value) {if (destroyed) return; paused = value === true; sync(); emit('pause', {paused});}
    function lookAt(x, y) {if (destroyed) return; gaze = {x: clamp(Number.isFinite(x) ? x : 0, -1, 1), y: clamp(Number.isFinite(y) ? y : 0, -1, 1)}; syncFallback(); if (reducedMotion) sync();}
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
    function setLevel(value) {if (destroyed || paused || !canDisplay()) return; level = clamp(value, 0, 1); if (state === 'speaking' && !paused && canDisplay()) {if (level < .045) speechRested = true; else if (level > .1 && speechRested && (speechBeatAt === null || now() - speechBeatAt > .42)) {speechBeatAt = now(); speechRested = false;}} syncFallback(); if (reducedMotion && state === 'speaking') sync();}
    const visibility = () => sync(), motion = event => {reducedMotion = event.matches; sync();};
    const pointer = event => {const box = frame.getBoundingClientRect(); if (box.width && box.height) lookAt((event.clientX - box.left) / box.width * 2 - 1, 1 - (event.clientY - box.top) / box.height * 2);};
    const resetGaze = () => lookAt(0, 0);
    doc.addEventListener('visibilitychange', visibility); media?.addEventListener?.('change', motion); frame.addEventListener('pointermove', pointer, {passive: true}); frame.addEventListener('pointerleave', resetGaze);
    const observer = win.IntersectionObserver ? new win.IntersectionObserver(entries => {intersecting = entries.some(entry => entry.isIntersecting); sync();}, {threshold: 0}) : null;
    observer?.observe(frame);
    function destroy() {if (destroyed) return; destroyed = true; cancelPerformance(); clearProduct(); cancelAppreciation(); cancelMannerism(); observer?.disconnect(); doc.removeEventListener('visibilitychange', visibility); media?.removeEventListener?.('change', motion); frame.removeEventListener('pointermove', pointer); frame.removeEventListener('pointerleave', resetGaze); try {engine?.destroy();} catch {} engine = null; frame.remove(); readyResolve(snapshot());}
    if (visible && options.greetingOnOpen !== false) triggerGreeting();
    sync(); if (options.emotion === 'appreciated') setEmotion('appreciated');
    return {ready, triggerGreeting, setState, setVisible, setPaused, setEmotion, retry, setLevel, lookAt, focusProduct, clearFocus, showProduct, clearProduct, setFloating, perform, cancelPerformance: () => {if (destroyed) return; cancelPerformance(); sync();}, cue, snapshot, destroy, element: frame};
  }
  const api = {create, MANNERISMS, BEHAVIOR_CUES, BLINK_EVENTS, BLINK_CYCLE, blinkFor, mannerismFor, STATES, EMOTIONS, validEmotion, validState, qualityFor, poseFor, productPhoto, validateAvatarPerformance, PERFORMANCE_GESTURES};
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  if (scope) scope.BritesConciergeAvatar = api;
})(typeof window === 'undefined' ? null : window);
