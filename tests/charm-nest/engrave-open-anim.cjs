// ENGANIM (engrave-ui-1012 round 2b): pressing the engraving of a piece in the order window runs the order window's own "goes back into what it opened" flight (Paul, 12 Oct 2026: "you broken the Beautiful process
// flow we had where we had nice transitional animations from the detailed order view to the engraving detailed modal popup ... Now you remove that button and completely removed the animation").
// The REAL page with REAL charm drawings and REAL Engrave jobs (tests/charm-nest/_engraveRealWorld.cjs: offline, no Etsy, no paid AI, nothing written anywhere real), one world, one order (front + back).
//   PW_DIR=/opt/node22/lib/node_modules/playwright/node_modules node tests/charm-nest/engrave-open-anim.cjs      (SHOTS=<dir> overrides where the ANIM-*.png frames go)
// What it proves
//   1  from the Orders list (the window grew out of its row), a press on a thumbnail of the card: #orderWin runs ONE clip-path animation, 420 ms on cubic-bezier(.3,0,.1,1) (the flight it always had), from the whole
//      screen to the rect of the Engrave detail's card, with its tint layer; the visible rect really changes over the frames and only shrinks; the Engrave tab is drawn BEHIND the window already (the card of THAT piece
//      is mounted on every frame of the flight), so nothing is blank and nothing cuts; the card does not move once the window lets go (no scroll jump); the gold ring marks the card it landed on, not a row of a tab
//      that is no longer shown, and goes; the window ends closed and the Engrave detail is mounted on the face pressed
//   2  the same press from a window opened with no clicked element (a search, a link): the same flight, into the card
//   3  prefers-reduced-motion: no clip-path flight, the window only fades (opacity, 110 ms) and the Engrave detail is mounted
//   4  EngraveLink.open straight (what the Sheet tab's button calls) with the window open: the same flight; with no window open it just lands (nothing to fly)
//   5  a caller that closes its own window (closeFirst: the sheet window) keeps the old order: it closes first, then the detail is drawn
//   6  four real frames of the flight are saved as ANIM-1-pressed.png ... ANIM-4-landed.png (the window at rest, held at 30% and 75% of its flight by pausing the animation, the card just landed with its ring)
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const R = require('./_engraveRealWorld.cjs');
const SHOTS = process.env.SHOTS || '/mnt/project-files/plans/engrave-ui-1012/round2/images';
let n = 0; const ok = (name, c, extra) => { assert(c !== false && c !== undefined && c !== null && c !== '', name + (extra ? ' :: ' + extra : '')); n++; console.log('  ✓ ' + name); };

/** a frame sampler in the page, kept light (the machine may be busy): what the window, the Engrave tab and the ring look like on every frame */
const SAMPLER = () => {
  const S = window.__an = { frames: [], on: true, kf: null, timing: null, own: new Set() };
  const win = document.getElementById('orderWin');
  const PROPS = new Set(['offset', 'easing', 'composite', 'computedOffset']);
  const tick = () => {
    if (!S.on) return;
    const card = document.querySelector('#engraveView #egQueue .rvItem'), r = card && card.getClientRects().length ? card.getBoundingClientRect() : null, back = win.classList.contains('owBack');
    if (win.open) for (const a of win.getAnimations()) {
      if (a.effect.target !== win) continue;
      const kf = a.effect.getKeyframes(), t = a.effect.getTiming(), props = [...new Set(kf.flatMap(k => Object.keys(k).filter(p => !PROPS.has(p))))].sort().join();
      S.own.add(props + '|' + t.duration);
      if (kf.some(k => k.clipPath) && !S.kf) {
        S.kf = kf.map(k => k.clipPath); S.timing = { dur: t.duration, ease: t.easing };
        // (what the window looks like at five points of the flight, whatever the machine's frame rate: the animation is set to each time in turn and its computed clip read, then put back)
        const cur = a.currentTime; S.probe = [0, .25, .5, .75, 1].map(p => { a.currentTime = p * t.duration; return getComputedStyle(win).clipPath; }); a.currentTime = cur;
      }
    }
    const g = document.querySelector('.owGlow'), gr = g && g.getBoundingClientRect();
    S.frames.push({ t: performance.timeOrigin + performance.now(), open: win.open, back, clip: back ? getComputedStyle(win).clipPath : '', mode: CN.S.mode, card: r ? [r.left, r.top, r.width, r.height] : null, cardKey: card ? card.dataset.key : null,
      glow: gr ? [gr.left, gr.top, gr.width, gr.height] : null, sy: window.scrollY, tint: back && !!win.querySelector('.owTint') });
    requestAnimationFrame(tick);
  };
  requestAnimationFrame(tick);
};
/** inset(a b c d round r) -> the visible rect of a full-screen window */
const visible = (clip, W, H) => {
  const m = /inset\(([^)]*?)(?:\s+round[^)]*)?\)/.exec(clip || ''); if (!m) return null;
  const v = m[1].trim().split(/\s+/).map(x => parseFloat(x)); if (!v.length || v.some(isNaN)) return null;
  const [t, r, b, l] = v.length === 1 ? [v[0], v[0], v[0], v[0]] : v.length === 2 ? [v[0], v[1], v[0], v[1]] : v.length === 3 ? [v[0], v[1], v[2], v[1]] : v;
  return [l, t, W - r - l, H - b - t];
};
const near = (a, b, tol) => !!a && !!b && a.every((v, i) => Math.abs(v - b[i]) <= tol);

(async () => {
  const w = await R.world({ viewport: { width: 1440, height: 900 }, shots: SHOTS });
  if (!w) { console.log('engrave-open-anim: no playwright-core, skipped'); return; }
  const { page } = w; let failed = null;
  try {
    const made = await R.build(w, ['both']), b = made.both, key = b.key, rid = b.rid;
    const frontJob = b.jobs.find(j => j.face === 'front').key, backJob = b.jobs.find(j => j.face === 'back').key;
    const W = 1440, H = 900;

    const closeWin = async () => { await page.evaluate(() => { if (document.getElementById('orderWin').open) OrderWin.close(); }); await page.waitForFunction(() => !document.getElementById('orderWin').open); };
    const toOrders = async () => { await closeWin(); await page.evaluate(() => { CN.setMode('orders'); Orders.render(); }); await R.settle(page, 300); };
    const openWin = async (click) => {
      if (click) await page.click(`[data-key="${key}"]`); else await page.evaluate(k => OrderWin.open(k), key);
      await page.waitForFunction(() => document.getElementById('orderWin').open && document.querySelector('#owEng .engCard .engThumb'));
      await R.settle(page, 1100);
    };
    const startSampling = () => page.evaluate(SAMPLER);
    const stopSampling = async (ms = 500) => { await page.waitForFunction(() => CN.S.mode === 'engrave' && !document.getElementById('orderWin').open && !document.querySelector('.owGlow'), null, { timeout: 8000 }).catch(() => {}); await R.settle(page, ms); return page.evaluate(() => { __an.on = false; return { fr: __an.frames, kf: __an.kf, timing: __an.timing, own: [...__an.own], probe: __an.probe }; }); };
    const landed = k => page.evaluate(k => { const c = document.querySelector('#egQueue .rvItem'); return { mode: CN.S.mode, focus: Engrave.view().focus, open: document.getElementById('orderWin').open, card: c ? c.dataset.key : null, want: k }; }, k);

    // ── 1 · from the Orders list, a press on a thumbnail of the card (up to four tries when the machine is too busy to draw more than a frame or two of the flight) ──
    const flightOnce = async () => {
      await toOrders(); await openWin(true);
      await startSampling(); await R.settle(page, 100);
      await page.click('#owEng .engFace[data-face="back"] .engThumb');
      const s = await stopSampling(1200);
      const fly = s.fr.filter(f => f.open && f.back), moving = fly.filter(f => f.clip && f.clip !== 'none');
      const widths = moving.map(f => visible(f.clip, W, H)[2]);
      return { s, fr: s.fr, fly, moving, widths, distinct: new Set(widths.map(x => Math.round(x))).size };
    };
    let one = await flightOnce();
    for (let i = 1; i < 4 && one.distinct < 4; i++) { const again = await flightOnce(); if (again.distinct > one.distinct) one = again; }
    let { s, fr, fly, moving, widths } = one;
    ok('the window flew: the clip of #orderWin.owBack was drawn on ' + moving.length + ' frames', moving.length >= 1, 'flight frames ' + fly.length + ', moving ' + moving.length);
    ok('ONE clip-path animation on the window: 420 ms, cubic-bezier(.3,0,.1,1): ' + JSON.stringify(s.timing), s.own.some(x => /^clipPath\|420$/.test(x)) && s.timing.dur === 420 && /cubic-bezier\(0\.3, 0, 0\.1, 1\)/.test(s.timing.ease), JSON.stringify([s.own, s.timing]));
    ok('the tint layer runs with it', fly.some(f => f.tint));
    ok('into the card the window also dissolves over its last 160 ms (opacity, delayed 260 ms), so the card is not swapped in blank-to-full: ' + JSON.stringify(s.own), s.own.some(x => /^opacity\|160$/.test(x)), JSON.stringify(s.own));
    const cardRect = (fr.filter(f => f.open && f.card).pop() || {}).card;
    ok('its keyframes go from the whole screen to the rect of the Engrave card: ' + JSON.stringify(s.kf.map(k => visible(k, W, H))), near(visible(s.kf[0], W, H), [0, 0, W, H], 1) && near(visible(s.kf[s.kf.length - 1], W, H), cardRect, 3), JSON.stringify([s.kf, cardRect]));
    const pr = s.probe.map(c => visible(c, W, H));
    ok('the animation moves the window rect from the whole screen into the card at 0, 25, 50, 75, 100% of its time, shrinking all the way: ' + JSON.stringify(pr.map(r => r.map(Math.round))), near(pr[0], [0, 0, W, H], 1) && near(pr[4], cardRect, 3) && pr.every((r, i) => i === 0 || (r[2] < pr[i - 1][2] && r[3] < pr[i - 1][3])), JSON.stringify(pr));
    if (one.distinct >= 3) ok('on the frames drawn the visible rect changes (' + one.distinct + ' different widths) and only shrinks', widths.every((x, i) => i === 0 || x <= widths[i - 1] + 1), JSON.stringify(widths));
    else console.log('  - (this machine is too busy to draw more than ' + moving.length + ' frame(s) of the flight (' + one.distinct + ' different widths): the animation itself was probed above instead)');
    ok('the Engrave detail of THAT piece is already mounted behind the window on every frame of the flight (' + fly.length + ')', fly.length >= 1 && fly.every(f => f.mode === 'engrave' && f.card && f.cardKey === backJob));
    const closedAt = fr.findIndex((f, i) => !f.open && fr[i - 1] && fr[i - 1].open);
    const after = fr.slice(closedAt).filter(f => !f.open);
    ok('the window closes with the card already in front (' + after.length + ' frames after it closed, every one with the card)', closedAt > 0 && after.length >= 2 && after.every(f => f.mode === 'engrave' && f.card));
    ok('the card never moves from the first frame it is drawn to the last, and the page never scrolls', new Set(fr.filter(f => f.card).map(f => f.card.map(v => Math.round(v)).join())).size <= 2 && fr.every(f => f.sy === 0), JSON.stringify([...new Set(fr.filter(f => f.card).map(f => f.card.map(v => Math.round(v)).join()))]));
    const ring = fr.filter(f => f.glow);
    ok('the gold ring marks the card it landed on (' + ring.length + ' frames), then is gone', ring.length >= 1 && near(ring[0].glow, cardRect, 3) && !fr[fr.length - 1].glow, JSON.stringify([ring[0] && ring[0].glow, cardRect]));
    let end = await landed(backJob);
    ok('the window is closed and the Engrave detail is mounted on the face pressed', end.mode === 'engrave' && !end.open && end.focus === backJob && end.card === backJob, JSON.stringify(end));
    // four real frames of it (6): the window at rest, held 30% and 75% of the way into the card (the animation is paused there by the test, the shot taken, the animation let go), and the card just landed with its ring
    fs.mkdirSync(SHOTS, { recursive: true });
    const freeze = async (p, name) => {
      await toOrders(); await openWin(true);
      if (name === 'ANIM-3-into-card') await page.screenshot({ path: path.join(SHOTS, 'ANIM-1-pressed.png') });
      await page.evaluate(p => {
        const ot = window.setTimeout; window.__ot = ot; window.setTimeout = (f, ms, ...a) => ot(f, ms === 600 ? 30000 : ms, ...a);   // (the window's own 600 ms "never wait on a hidden tab" limit waits for the paused flight; put back below)
        const win = document.getElementById('orderWin'); window.__held = null;
        const look = () => { const a = win.getAnimations().find(x => x.effect.target === win && x.effect.getKeyframes().some(k => k.clipPath)); if (a) { const d = a.effect.getTiming().duration; window.__all = document.getAnimations().filter(x => x.playState === 'running' && x.effect && x.effect.getComputedTiming().endTime <= 700); for (const x of window.__all) { x.pause(); x.currentTime = p * d; } window.__held = a; } else requestAnimationFrame(look); };
        requestAnimationFrame(look);
      }, p);
      await page.click('#owEng .engFace[data-face="back"] .engThumb');
      await page.waitForFunction(() => window.__held);
      await R.settle(page, 50);
      await page.screenshot({ path: path.join(SHOTS, name + '.png') });
      const held = await page.evaluate(() => ({ t: __held.currentTime, dur: __held.effect.getTiming().duration, open: document.getElementById('orderWin').open }));
      await page.evaluate(() => { for (const x of __all) x.play(); });
      if (name === 'ANIM-3-into-card') { await page.waitForFunction(() => document.querySelector('.owGlow'), null, { timeout: 5000 }).catch(() => {}); await page.screenshot({ path: path.join(SHOTS, 'ANIM-4-landed.png') }); }
      await page.waitForFunction(() => !document.getElementById('orderWin').open);
      await R.settle(page, 300);
      await page.evaluate(() => { window.setTimeout = window.__ot; });
      return held;
    };
    const h1 = await freeze(.3, 'ANIM-2-shrinking'), h2 = await freeze(.75, 'ANIM-3-into-card');
    ok('frames held at 30% and 75% of the flight (' + Math.round(h1.t) + ' and ' + Math.round(h2.t) + ' of ' + h1.dur + ' ms), the window open at both', Math.abs(h1.t - .3 * h1.dur) < 2 && Math.abs(h2.t - .75 * h2.dur) < 2 && h1.open && h2.open);
    ok('four frames saved: ANIM-1-pressed, ANIM-2-shrinking, ANIM-3-into-card, ANIM-4-landed', ['ANIM-1-pressed', 'ANIM-2-shrinking', 'ANIM-3-into-card', 'ANIM-4-landed'].every(f => fs.statSync(path.join(SHOTS, f + '.png')).size > 5000));
    end = await landed(backJob);
    ok('after the frames were taken the flight ran on and landed on the same face', end.mode === 'engrave' && !end.open && end.card === backJob, JSON.stringify(end));

    // ── 2 · a window opened with no clicked element (a search, a link) ──
    await toOrders(); await openWin(false);
    await startSampling();
    await page.click('#owEng .engFace[data-face="front"] .engThumb');
    s = await stopSampling(900); fr = s.fr;
    const fly2 = fr.filter(f => f.open && f.back && f.clip && f.clip !== 'none');
    ok('a window with no clicked element: the same flight, into the card (' + fly2.length + ' frames)', fly2.length >= 1 && s.timing.dur === 420 && near(visible(s.kf[s.kf.length - 1], W, H), (fr.filter(f => f.open && f.card).pop() || {}).card, 3), JSON.stringify([s.timing, s.kf]));
    end = await landed(frontJob);
    ok('and it ends on the FRONT face pressed', end.mode === 'engrave' && !end.open && end.focus === frontJob && end.card === frontJob, JSON.stringify(end));

    // ── 3 · prefers-reduced-motion ──
    await page.emulateMedia({ reducedMotion: 'reduce' });
    await toOrders(); await openWin(true);
    await startSampling();
    await page.click('#owEng .engFace[data-face="back"] .engThumb');
    s = await stopSampling(900); fr = s.fr;
    ok('reduced motion: no clip-path flight, no tint, the window only fades: ' + JSON.stringify(s.own), !s.own.some(x => /clipPath/.test(x)) && s.own.some(x => /^opacity/.test(x) && +x.split('|')[1] <= 170) && !fr.some(f => f.back), JSON.stringify(s.own));
    end = await landed(backJob);
    ok('reduced motion: the Engrave detail is mounted on the face pressed', end.mode === 'engrave' && !end.open && end.focus === backJob && end.card === backJob, JSON.stringify(end));
    await page.emulateMedia({ reducedMotion: 'no-preference' });

    // ── 4 · EngraveLink.open straight: with the window open it flies, with no window it just lands ──
    await toOrders(); await openWin(true);
    await startSampling();
    const res4 = await page.evaluate(a => EngraveLink.open(a), { rid, key, piece: backJob, face: 'back' });
    s = await stopSampling(300); fr = s.fr;
    ok('EngraveLink.open with the order window open (the Sheet tab\'s button): the same flight, then true', res4 === true && fr.some(f => f.open && f.back) && s.timing && s.timing.dur === 420 && near(visible(s.kf[s.kf.length - 1], W, H), (fr.filter(f => f.open && f.card).pop() || {}).card, 3));
    end = await landed(backJob);
    ok('and the window is closed, the detail mounted', end.mode === 'engrave' && !end.open && end.card === backJob, JSON.stringify(end));
    await toOrders(); await startSampling();
    const res4b = await page.evaluate(a => EngraveLink.open(a), { rid, key, piece: frontJob, face: 'front' });
    s = await stopSampling(300);
    ok('no window open: nothing flies, the detail is mounted (true)', res4b === true && s.own.length === 0 && (await landed(frontJob)).card === frontJob);

    // ── 5 · a caller that closes its own window keeps the old order: it closes first, then the detail is drawn ──
    await toOrders(); await openWin(true);
    const o5 = await page.evaluate(async a => {
      const seen = []; const ok = await EngraveLink.open(Object.assign({}, a, { closeFirst: () => { seen.push(CN.S.mode); return OrderWin.close(); } }));
      const c = document.querySelector('#egQueue .rvItem'); return { ok, seen, mode: CN.S.mode, card: c && c.dataset.key, open: document.getElementById('orderWin').open };
    }, { rid, key, piece: backJob, face: 'back' });
    ok('closeFirst runs before the Engrave tab is shown (the tab was still Orders), then the detail is mounted', o5.ok === true && o5.seen.length === 1 && o5.seen[0] === 'orders' && o5.mode === 'engrave' && o5.card === backJob && !o5.open, JSON.stringify(o5));
    ok('no page error', w.errors.length === 0, JSON.stringify(w.errors));
    console.log('\nengrave-open-anim: ' + n + ' checks passed');
  } catch (e) { failed = e; console.error('FAIL:', e && e.message || e); }
  finally { await w.close(); }
  if (failed) process.exit(1);
})().catch(e => { console.error(e); process.exit(1); });
