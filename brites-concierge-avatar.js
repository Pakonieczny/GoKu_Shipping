(function (scope) {
  'use strict';
  const STATES = Object.freeze(['idle', 'listening', 'thinking', 'speaking', 'success', 'error']);
  const clamp = (value, low, high) => Math.max(low, Math.min(high, Number.isFinite(value) ? value : low));
  const validState = state => STATES.includes(state) ? state : 'idle';
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
  function poseFor({state = 'idle', time = 0, elapsed = 0, level = 0, gaze = {x: 0, y: 0}, reducedMotion = false} = {}) {
    state = validState(state); time = reducedMotion ? 0 : Number.isFinite(time) ? time : 0; elapsed = Math.max(0, elapsed || 0);
    gaze = gaze && typeof gaze === 'object' ? gaze : {}; const gazeX = Number.isFinite(gaze.x) ? gaze.x : 0, gazeY = Number.isFinite(gaze.y) ? gaze.y : 0;
    const motion = reducedMotion ? 0 : 1, talking = state === 'speaking', thoughtful = state === 'thinking', listening = state === 'listening', happy = state === 'success', concerned = state === 'error';
    const cycle = ((time + 1.4) % 4.7 + 4.7) % 4.7, blink = motion && cycle < .18 ? Math.sin(cycle / .18 * Math.PI) : 0;
    const speech = talking ? (reducedMotion ? .3 : .25 + .45 * Math.abs(Math.sin(time * 7.6)) + .3 * clamp(level, 0, 1)) : 0;
    const lift = happy && motion ? Math.sin(Math.min(elapsed, 1.2) / 1.2 * Math.PI) * .12 : 0;
    return {
      state, bob: Math.sin(time * 1.35) * .045 * motion + lift,
      bodyRoll: Math.sin(time * .7) * .018 * motion,
      headYaw: clamp(gazeX, -1, 1) * .1 + (thoughtful ? Math.sin(time * .65) * .055 * motion : 0),
      headPitch: clamp(gazeY, -1, 1) * .065 + (listening ? -.045 : 0) + (talking ? Math.sin(time * 2.5) * .025 * motion : 0),
      headRoll: concerned ? -.055 : thoughtful ? .055 : happy ? .025 : 0,
      eyeOpen: Math.max(.035, (happy ? .7 : concerned ? .85 : listening ? 1.06 : 1) * (1 - blink)),
      gazeX: clamp(gazeX, -1, 1) * .045 + (thoughtful ? -.035 : 0),
      gazeY: clamp(gazeY, -1, 1) * .038 + (thoughtful ? .04 : 0),
      browLift: happy ? .08 : listening ? .045 : concerned ? .025 : thoughtful ? .02 : 0,
      browAngle: concerned ? .15 : thoughtful ? -.08 : happy ? -.08 : -.025,
      mouth: talking ? 'open' : concerned ? 'concern' : 'smile', mouthOpen: speech,
      armLift: thoughtful ? .23 : happy ? .15 : listening ? .06 : 0,
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
    // This original illustration is a clearly separate fallback when WebGL is
    // unavailable; the live avatar is a mesh scene, not this image animated.
    fallback.innerHTML = '<svg viewBox="0 0 320 280" aria-hidden="true" xmlns="http://www.w3.org/2000/svg"><defs><linearGradient id="britesIvory" x2="1" y2="1"><stop stop-color="#fffef9"/><stop offset="1" stop-color="#d7c8b0"/></linearGradient><linearGradient id="britesGold" x2=".3" y2="1"><stop stop-color="#f5ddab"/><stop offset=".5" stop-color="#b98e51"/><stop offset="1" stop-color="#edd0a0"/></linearGradient></defs><ellipse cx="160" cy="249" rx="64" ry="11" fill="#564533" opacity=".12"/><ellipse cx="160" cy="184" rx="43" ry="48" fill="url(#britesIvory)"/><ellipse cx="160" cy="205" rx="41" ry="9" fill="url(#britesGold)"/><rect x="78" y="55" width="164" height="114" rx="43" fill="url(#britesGold)"/><rect x="84" y="59" width="152" height="105" rx="40" fill="url(#britesIvory)"/><rect x="97" y="77" width="126" height="72" rx="28" fill="#102831"/><ellipse cx="133" cy="111" rx="15" ry="21" fill="#9beaf0"/><ellipse cx="187" cy="111" rx="15" ry="21" fill="#9beaf0"/><path d="M149 134Q160 144 171 134" fill="none" stroke="#9beaf0" stroke-width="3" stroke-linecap="round"/><path d="M160 164l9 13-9 15-9-15z" fill="#c3f3ec" stroke="#b68d4c" stroke-width="3"/><ellipse cx="106" cy="184" rx="13" ry="18" fill="url(#britesIvory)"/><ellipse cx="214" cy="184" rx="13" ry="18" fill="url(#britesIvory)"/></svg>';
    const fallbackImage = doc.createElement('img'); fallbackImage.alt = ''; fallbackImage.setAttribute('aria-hidden', 'true'); fallbackImage.onload = () => {fallback.dataset.image = 'ready';}; fallback.appendChild(fallbackImage);
    const style = doc.createElement('link'); style.rel = 'stylesheet'; style.href = cssUrl;
    frame.append(style, surface, fallback); container.appendChild(frame);
    let state = validState(options.initialState), visible = options.visible === true, intersecting = true, destroyed = false, loading = false, engine = null, declarations = null, failed = false, level = 0, gaze = {x: 0, y: 0}, stateAt = 0;
    const media = win.matchMedia ? win.matchMedia('(prefers-reduced-motion: reduce)') : null;
    let reducedMotion = !!media?.matches, readyResolve;
    const ready = new Promise(resolve => {readyResolve = resolve;});
    const quality = qualityFor({width: win.innerWidth, mobile: options.mobile, memory: win.navigator?.deviceMemory, pixelRatio: win.devicePixelRatio, bloom: options.bloom});
    const now = () => (win.performance?.now?.() || Date.now()) / 1000;
    stateAt = now();
    function emit(type, detail) {frame.dispatchEvent(new win.CustomEvent('brites-avatar:' + type, {detail, bubbles: true, composed: true})); try {options.onStatus?.(type, detail);} catch {}}
    function snapshot() {return {state, visible, intersecting, reducedMotion, mode: engine && !failed ? 'webgl' : failed ? 'fallback' : 'pending', loading, destroyed, quality: {...quality}, ...(engine?.snapshot?.() || {}), declarations: declarations ? {schema: declarations.schema, source: declarations.source, textures: declarations.textures.map(value => ({...value}))} : null};}
    function active() {return visible && intersecting && !doc.hidden && !destroyed && !failed;}
    function renderingFailure() {const old = engine; engine = null; failed = true; loading = false; frame.dataset.rendering = 'fallback'; frame.setAttribute('aria-label', 'Brites jewellery gift guide illustration'); try {old?.destroy();} catch {} emit('fallback', {reason: 'WebGL rendering is unavailable', ...snapshot()}); readyResolve(snapshot());}
    function sync() {frame.hidden = !visible; if (engine) {try {engine.setMotion({active: active(), reducedMotion}); if (active()) engine.render(poseFor({state, time: now(), elapsed: now() - stateAt, level, gaze, reducedMotion}), true);} catch {renderingFailure();}} if (active() && !engine && !loading) load();}
    async function load() {
      loading = true; frame.dataset.rendering = 'loading';
      if (options.fallbackImageUrl !== false) fallbackImage.src = new URL(options.fallbackImageUrl || 'assets/brites-concierge/avatar-concept.png', base).href;
      try {
        const sceneModule = options.loadScene ? await options.loadScene(moduleUrl) : await import(moduleUrl);
        if (destroyed) return;
        declarations = declaredScene(sceneModule.AVATAR_SCENE_DECLARATIONS, quality.textureSize);
        engine = sceneModule.createAvatarScene({container: surface, quality, onFrame: t => poseFor({state, time: t, elapsed: t - stateAt, level, gaze, reducedMotion}), onError: renderingFailure, onContext: lost => {failed = lost; frame.dataset.rendering = lost ? 'fallback' : 'webgl'; frame.setAttribute('aria-label', lost ? 'Brites jewellery gift guide illustration' : 'Brites jewellery gift guide'); emit(lost ? 'fallback' : 'restored', snapshot()); sync();}});
        if (destroyed) {engine.destroy(); return;}
        failed = false; loading = false; frame.dataset.rendering = 'webgl'; fallback.setAttribute('aria-hidden', 'true');
        emit('ready', snapshot()); readyResolve(snapshot()); sync();
      } catch (error) {
        if (destroyed) return;
        renderingFailure();
      } finally {loading = false;}
    }
    function setState(value) {if (destroyed) return; state = validState(value); stateAt = now(); frame.dataset.state = state; try {engine?.invalidate();} catch {renderingFailure();} emit('state', {state}); sync();}
    function setVisible(value) {if (destroyed) return; visible = value === true; sync();}
    function lookAt(x, y) {gaze = {x: clamp(Number.isFinite(x) ? x : 0, -1, 1), y: clamp(Number.isFinite(y) ? y : 0, -1, 1)}; if (reducedMotion) sync();}
    function setLevel(value) {level = clamp(value, 0, 1); if (reducedMotion && state === 'speaking') sync();}
    const visibility = () => sync(), motion = event => {reducedMotion = event.matches; sync();};
    const pointer = event => {const box = frame.getBoundingClientRect(); if (box.width && box.height) lookAt((event.clientX - box.left) / box.width * 2 - 1, 1 - (event.clientY - box.top) / box.height * 2);};
    const resetGaze = () => lookAt(0, 0);
    doc.addEventListener('visibilitychange', visibility); media?.addEventListener?.('change', motion); frame.addEventListener('pointermove', pointer, {passive: true}); frame.addEventListener('pointerleave', resetGaze);
    const observer = win.IntersectionObserver ? new win.IntersectionObserver(entries => {intersecting = entries.some(entry => entry.isIntersecting); sync();}, {threshold: 0}) : null;
    observer?.observe(frame);
    function destroy() {if (destroyed) return; destroyed = true; observer?.disconnect(); doc.removeEventListener('visibilitychange', visibility); media?.removeEventListener?.('change', motion); frame.removeEventListener('pointermove', pointer); frame.removeEventListener('pointerleave', resetGaze); try {engine?.destroy();} catch {} engine = null; frame.remove(); readyResolve(snapshot());}
    sync();
    return {ready, setState, setVisible, setLevel, lookAt, snapshot, destroy, element: frame};
  }
  const api = {create, STATES, validState, qualityFor, poseFor};
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  if (scope) scope.BritesConciergeAvatar = api;
})(typeof window === 'undefined' ? null : window);
