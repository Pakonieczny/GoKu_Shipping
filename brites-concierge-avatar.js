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
  const EMOTIONS = Object.freeze(['calm', 'curious', 'celebrate', 'reassuring', 'warm']);
  const validEmotion = emotion => EMOTIONS.includes(emotion) ? emotion : null;
  let instanceCount = 0;
  function declaredScene(raw, textureSize) {
    if (!raw || raw.schema !== 1 || !Array.isArray(raw.textures) || raw.textures.length < 1 || raw.textures.length > 16) return null;
    const textures = raw.textures.map(value => ({name: typeof value?.name === 'string' ? value.name.slice(0, 80) : '', kind: typeof value?.kind === 'string' ? value.kind.slice(0, 32) : '', width: textureSize, height: textureSize}));
    if (textures.some(value => !value.name || !value.kind)) return null;
    return {schema: 1, source: 'loaded_scene_module', textures};
  }
  function qualityFor(hints = {}) {
    const mobile = hints.mobile === true || (hints.width > 0 && hints.width < 600) || (hints.memory > 0 && hints.memory <= 4);
    return {name: mobile ? 'adaptive' : 'high', pixelRatio: Math.min(mobile ? 1.5 : 2, Math.max(1, hints.pixelRatio || 1)), shadowSize: mobile ? 1024 : 2048, textureSize: 2048, fps: mobile ? 30 : 60, geometryScale: mobile ? .8 : 1, bloom: hints.bloom !== false};
  }
  function poseFor({state = 'idle', time = 0, elapsed = 0, level = 0, gaze = {x: 0, y: 0}, reducedMotion = false, emotion = null} = {}) {
    state = validState(state); time = reducedMotion ? 0 : Number.isFinite(time) ? time : 0; elapsed = Number.isFinite(elapsed) ? Math.max(0, elapsed) : 0;
    gaze = gaze && typeof gaze === 'object' ? gaze : {}; const gazeX = Number.isFinite(gaze.x) ? gaze.x : 0, gazeY = Number.isFinite(gaze.y) ? gaze.y : 0;
    emotion = validEmotion(emotion); const calm = emotion === 'calm' || emotion === 'reassuring';
    const motion = reducedMotion ? 0 : calm ? .4 : 1, talking = state === 'speaking', thoughtful = state === 'thinking', listening = state === 'listening', happy = state === 'success' && !calm, concerned = state === 'error';
    const cycle = ((time + 1.4) % 4.7 + 4.7) % 4.7, blink = motion && cycle < .18 ? Math.sin(cycle / .18 * Math.PI) : 0;
    const speech = talking ? (reducedMotion ? .3 : .25 + .45 * Math.abs(Math.sin(time * 7.6)) + .3 * clamp(level, 0, 1)) : 0;
    const lift = happy && motion ? Math.sin(Math.min(elapsed, 1.2) / 1.2 * Math.PI) * .12 : 0;
    return {
      state, eyeColor: MOODS[state].color, emotion: emotion || MOODS[state].emotion,
      eyeDeformation: calm ? -.025 : happy ? .24 : thoughtful ? -.14 : listening ? .1 : concerned ? -.08 : 0,
      eyeScaleX: happy ? 1.08 : thoughtful ? .93 : 1,
      eyeScaleY: happy ? .82 : listening ? 1.06 : concerned ? .9 : 1,
      ringRotation: thoughtful ? time * .3 : 0,
      antennaTilt: thoughtful ? -.14 : listening ? .08 : happy ? .18 : 0,
      bob: Math.sin(time * 1.35) * .045 * motion + lift,
      bodyRoll: Math.sin(time * .7) * .018 * motion,
      headYaw: clamp(gazeX, -1, 1) * .1 + (thoughtful ? Math.sin(time * .65) * .055 * motion : 0),
      headPitch: clamp(gazeY, -1, 1) * .065 + (listening ? -.045 : 0) + (talking ? Math.sin(time * 2.5) * .025 * motion : 0),
      headRoll: calm ? -.012 : concerned ? -.055 : thoughtful ? .055 : happy ? .025 : 0,
      eyeOpen: Math.max(.035, (happy ? .7 : concerned ? .85 : listening ? 1.06 : 1) * (1 - blink)),
      gazeX: clamp(gazeX, -1, 1) * .045 + (thoughtful ? -.035 : 0),
      gazeY: clamp(gazeY, -1, 1) * .038 + (thoughtful ? .04 : 0),
      browLift: happy ? .08 : listening ? .045 : concerned ? .025 : thoughtful ? .02 : 0,
      browAngle: concerned ? .15 : thoughtful ? -.08 : happy ? -.08 : -.025,
      mouth: talking ? 'open' : concerned ? 'concern' : 'smile', mouthOpen: speech,
      armLift: calm ? .025 : thoughtful ? .23 : happy ? .15 : listening ? .06 : 0,
      gesture: Math.sin(time * 2.4) * .035 * motion * (talking || happy ? 1 : 0),
      lightPulse: thoughtful ? (reducedMotion ? .3 : .18 + .18 * Math.sin(time * 2)) : happy ? .45 : talking ? speech * .22 : 0
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
    // This is an animated vector companion, visibly separate from the PBR
    // WebGL mesh. It keeps state feedback usable without pretending to be 3-D.
    const id = 'britesRobot' + (++instanceCount);
    fallback.innerHTML = `<svg viewBox="0 0 320 280" aria-hidden="true" xmlns="http://www.w3.org/2000/svg"><defs><linearGradient id="${id}Ivory" x2=".4" y2="1"><stop stop-color="#fffefa"/><stop offset="1" stop-color="#d9dfe4"/></linearGradient><linearGradient id="${id}Gold" x2=".2" y2="1"><stop stop-color="#f2dda8"/><stop offset=".5" stop-color="#ac824b"/><stop offset="1" stop-color="#e4c78e"/></linearGradient><radialGradient id="${id}Glass"><stop stop-color="#1e3848"/><stop offset="1" stop-color="#09121d"/></radialGradient></defs><ellipse class="brites-avatar__shadow" cx="160" cy="246" rx="53" ry="8" fill="#243745" opacity=".13"/><g class="brites-avatar__robot"><g class="brites-avatar__antenna"><path d="M160 63V40" stroke="url(#${id}Gold)" stroke-width="5" stroke-linecap="round"/><circle cx="160" cy="36" r="5" class="brites-avatar__signal"/></g><ellipse cx="160" cy="197" rx="34" ry="38" fill="url(#${id}Ivory)" stroke="#ccd5dc"/><path d="M131 214Q160 226 189 214" fill="none" stroke="url(#${id}Gold)" stroke-width="4"/><path d="M154 188l6-7 6 7-6 8z" fill="url(#${id}Gold)"/><g class="brites-avatar__arm brites-avatar__arm--left"><rect x="107" y="180" width="14" height="31" rx="7" fill="url(#${id}Ivory)" stroke="#ccd5dc"/></g><g class="brites-avatar__arm brites-avatar__arm--right"><rect x="199" y="180" width="14" height="31" rx="7" fill="url(#${id}Ivory)" stroke="#ccd5dc"/></g><g class="brites-avatar__head"><circle cx="160" cy="118" r="66" fill="url(#${id}Ivory)" stroke="#ccd5dc"/><circle cx="160" cy="118" r="55" fill="url(#${id}Gold)"/><circle cx="160" cy="118" r="51" fill="url(#${id}Glass)"/><circle class="brites-avatar__halo" cx="160" cy="118" r="40" fill="none" stroke-width="2" stroke-dasharray="185 66"/><g class="brites-avatar__eye-gaze"><g class="brites-avatar__eye"><circle cx="160" cy="118" r="28" fill="none" stroke-width="10"/><circle cx="160" cy="118" r="15" fill="none" stroke-width="1.4" opacity=".36"/><circle cx="176" cy="102" r="3" fill="#eefaff" opacity=".9"/></g></g><path d="M134 83Q145 77 155 78" fill="none" stroke="#fff" opacity=".16" stroke-width="3" stroke-linecap="round"/></g></g></svg>`;
    const caption = doc.createElement('div'); caption.className = 'brites-avatar__caption';
    caption.setAttribute('aria-hidden', 'true');
    caption.textContent = MOODS[validState(options.initialState)].label;
    const style = doc.createElement('link'); style.rel = 'stylesheet'; style.href = cssUrl;
    frame.append(style, surface, fallback, caption); container.appendChild(frame);
    let state = validState(options.initialState), visible = options.visible === true, intersecting = true, destroyed = false, loading = false, engine = null, declarations = null, failed = false, paused = options.paused === true, emotion = validEmotion(options.emotion), failureReason = null, level = 0, gaze = {x: 0, y: 0}, stateAt = 0;
    const media = win.matchMedia ? win.matchMedia('(prefers-reduced-motion: reduce)') : null;
    let reducedMotion = !!media?.matches, readyResolve;
    const ready = new Promise(resolve => {readyResolve = resolve;});
    const quality = qualityFor({width: win.innerWidth, mobile: options.mobile, memory: win.navigator?.deviceMemory, pixelRatio: win.devicePixelRatio, bloom: options.bloom});
    const now = () => (win.performance?.now?.() || Date.now()) / 1000;
    stateAt = now();
    function emit(type, detail) {frame.dispatchEvent(new win.CustomEvent('brites-avatar:' + type, {detail, bubbles: true, composed: true})); try {options.onStatus?.(type, detail);} catch {}}
    function canDisplay() {return visible && intersecting && !doc.hidden && !destroyed;}
    function active() {return canDisplay() && !paused && !failed;}
    function fallbackMoving() {return canDisplay() && !paused && !reducedMotion && (!engine || failed);}
    function snapshot() {
      const scene = engine?.snapshot?.() || {};
      return {...scene, state, emotion, visible, intersecting, paused, reducedMotion, mode: engine && !failed ? 'webgl' : failed ? 'fallback' : 'pending', loading, destroyed,
        animated: engine && !failed ? active() && !reducedMotion && scene.animated === true : fallbackMoving(),
        fallback: {format: 'animated_svg_2d', active: canDisplay() && (!engine || failed), animated: fallbackMoving(), reason: failureReason},
        quality: {...quality}, declarations: declarations ? {schema: declarations.schema, source: declarations.source, textures: declarations.textures.map(value => ({...value}))} : null};
    }
    function syncFallback() {
      const pose = poseFor({state, time: now(), elapsed: now() - stateAt, level, gaze, reducedMotion, emotion});
      frame.style.setProperty('--brites-eye-color', pose.eyeColor);
      frame.style.setProperty('--brites-eye-x', String(pose.eyeScaleX));
      frame.style.setProperty('--brites-eye-y', String(pose.eyeScaleY));
      frame.style.setProperty('--brites-gaze-x', (pose.gazeX * 180).toFixed(2) + 'px');
      frame.style.setProperty('--brites-gaze-y', (-pose.gazeY * 180).toFixed(2) + 'px');
      frame.style.setProperty('--brites-speech-level', String(level));
      frame.style.setProperty('--brites-speech-scale', String(1 + level * .14));
      frame.dataset.motion = canDisplay() && !paused && !reducedMotion ? 'running' : reducedMotion ? 'reduced' : 'paused';
      frame.dataset.fallbackFormat = 'animated-svg-2d';
      frame.dataset.emotion = emotion || MOODS[state].emotion;
      frame.dataset.fallbackAnimated = String(fallbackMoving());
      const statusLabel = state === 'success' && (emotion === 'calm' || emotion === 'reassuring') ? 'Here with you' : MOODS[state].label;
      caption.textContent = statusLabel + (failed ? ' · 2-D companion' : '');
      frame.setAttribute('aria-label', 'Brites jewellery gift guide. ' + statusLabel + (failed ? '. Animated 2-D companion; 3-D unavailable.' : '') + (paused ? '. Animation paused.' : ''));
    }
    function renderingFailure(reason = 'WebGL rendering is unavailable') {reason = typeof reason === 'string' ? reason : 'WebGL rendering is unavailable'; const old = engine; engine = null; failed = true; failureReason = reason; loading = false; frame.dataset.rendering = 'fallback'; try {old?.destroy();} catch {} syncFallback(); emit('fallback', {reason: failureReason, ...snapshot()}); readyResolve(snapshot());}
    function sync() {frame.hidden = !visible; syncFallback(); if (engine) {try {engine.setMotion({active: active(), reducedMotion}); if (active()) engine.render(poseFor({state, time: now(), elapsed: now() - stateAt, level, gaze, reducedMotion, emotion}), true);} catch {renderingFailure();}} if (active() && !engine && !loading) load();}
    async function load() {
      loading = true; frame.dataset.rendering = 'loading';
      try {
        const sceneModule = options.loadScene ? await options.loadScene(moduleUrl) : await import(moduleUrl);
        if (destroyed) return;
        declarations = declaredScene(sceneModule.AVATAR_SCENE_DECLARATIONS, quality.textureSize);
        engine = sceneModule.createAvatarScene({container: surface, quality, onFrame: t => poseFor({state, time: t, elapsed: t - stateAt, level, gaze, reducedMotion, emotion}), onError: renderingFailure, onContext: lost => {failed = lost; failureReason = lost ? 'WebGL context was lost' : null; frame.dataset.rendering = lost ? 'fallback' : 'webgl'; syncFallback(); emit(lost ? 'fallback' : 'restored', snapshot()); sync();}});
        if (destroyed) {engine.destroy(); return;}
        failed = false; failureReason = null; loading = false; frame.dataset.rendering = 'webgl'; fallback.setAttribute('aria-hidden', 'true');
        emit('ready', snapshot()); readyResolve(snapshot()); sync();
      } catch (error) {
        if (destroyed) return;
        renderingFailure();
      } finally {loading = false;}
    }
    function setState(value) {if (destroyed) return; state = validState(value); stateAt = now(); frame.dataset.state = state; try {engine?.invalidate();} catch {renderingFailure();} emit('state', {state}); sync();}
    function setVisible(value) {if (destroyed) return; visible = value === true; sync();}
    // A failed 3-D scene is retried only explicitly; state updates still animate
    // the separate vector companion within the same visibility/pause bounds.
    function retry() {if (destroyed || loading || engine || !failed || !visible || !intersecting || doc.hidden) return false; failed = false; sync(); return loading;}
    function setEmotion(value) {if (destroyed) return; emotion = validEmotion(value); sync(); emit('emotion', {emotion});}
    function setPaused(value) {if (destroyed) return; paused = value === true; sync(); emit('pause', {paused});}
    function lookAt(x, y) {gaze = {x: clamp(Number.isFinite(x) ? x : 0, -1, 1), y: clamp(Number.isFinite(y) ? y : 0, -1, 1)}; syncFallback(); if (reducedMotion) sync();}
    function setLevel(value) {level = clamp(value, 0, 1); syncFallback(); if (reducedMotion && state === 'speaking') sync();}
    const visibility = () => sync(), motion = event => {reducedMotion = event.matches; sync();};
    const pointer = event => {const box = frame.getBoundingClientRect(); if (box.width && box.height) lookAt((event.clientX - box.left) / box.width * 2 - 1, 1 - (event.clientY - box.top) / box.height * 2);};
    const resetGaze = () => lookAt(0, 0);
    doc.addEventListener('visibilitychange', visibility); media?.addEventListener?.('change', motion); frame.addEventListener('pointermove', pointer, {passive: true}); frame.addEventListener('pointerleave', resetGaze);
    const observer = win.IntersectionObserver ? new win.IntersectionObserver(entries => {intersecting = entries.some(entry => entry.isIntersecting); sync();}, {threshold: 0}) : null;
    observer?.observe(frame);
    function destroy() {if (destroyed) return; destroyed = true; observer?.disconnect(); doc.removeEventListener('visibilitychange', visibility); media?.removeEventListener?.('change', motion); frame.removeEventListener('pointermove', pointer); frame.removeEventListener('pointerleave', resetGaze); try {engine?.destroy();} catch {} engine = null; frame.remove(); readyResolve(snapshot());}
    sync();
    return {ready, setState, setVisible, setPaused, setEmotion, retry, setLevel, lookAt, snapshot, destroy, element: frame};
  }
  const api = {create, STATES, EMOTIONS, validEmotion, validState, qualityFor, poseFor};
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  if (scope) scope.BritesConciergeAvatar = api;
})(typeof window === 'undefined' ? null : window);
