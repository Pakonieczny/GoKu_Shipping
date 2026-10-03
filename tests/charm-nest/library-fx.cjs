// Browser test of the Library flight (charm-nest-library-fx.js, window.LibraryFx), on a small fixture page that has the
// Library's own structure (a top bar over a scrolling stage, the two sections "Laser cutting" and "In progress", sheet
// cards with preview, canvas, seals and counter, a set card, the Completed tab) and no network.
// Paul, 3 Oct: "a beautiful animation ... shows it moving from its current location with all the parts and pieces on it
// until its new location". What it holds to: the copy sets off from the card (or from the copy a drag carries), flies along
// an arc and ends exactly on the card at its new place (also when the page is scrolled meanwhile, or the card is drawn
// anew mid-flight), the real card shows as the copy lands, nothing stays behind on any path (a missing target, a target that
// vanishes, a hidden page, a repeated flight, the hard cap, reduced motion), nothing it hides stays hidden, and it never throws.
//   node tests/charm-nest/library-fx.cjs [playwright-core dir]      (PW_DIR=…, CHROMIUM=…; SHOTS=<dir> saves a few frames)
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
let chromium;
try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser check was not run'); process.exit(0); }
const SHOTS = process.env.SHOTS || '';

const PAGE = `<!doctype html><meta charset="utf-8"><title>fx</title>
<style>
body{margin:0;font:13px system-ui,sans-serif;background:#f6f1e7;color:#2b2620}
#bar{position:fixed;top:0;left:0;right:0;height:56px;background:#2b2620;z-index:300;display:flex;align-items:center;padding:0 16px;gap:10px}
#libTab button{font:600 12px system-ui;padding:5px 14px;border-radius:999px;border:1px solid #c8bfae;background:#fff;color:#2b2620}
.libBar{padding:0 0 12px}
#stage{position:fixed;top:56px;left:0;right:0;bottom:0;overflow:auto;padding:16px}
.laserSection{margin:0 0 24px;border-top:1px solid #d8d0c0;padding-top:14px}
.laserSection h2{font:600 15px system-ui;margin:0 0 10px}
.laserAreaItems{display:flex;flex-wrap:wrap;align-items:flex-start;gap:16px;min-height:60px}
.laserAreaItems>.setCard{width:100%}
.librarySheet{width:300px;flex:0 0 300px;display:flex;flex-direction:column;gap:12px}
.libCard{position:relative;background:#fff;border:1px solid #d8d0c0;border-left:5px solid #c9a65c;border-radius:12px;box-shadow:0 1px 5px rgba(30,24,16,.18);padding:10px 12px;display:flex;flex-direction:column;gap:6px}
.libCard .pv{width:100%;height:120px;object-fit:contain;background:#faf7f0;border:1px solid #e8e2d4;border-radius:8px}
.libCard canvas{width:100%;height:24px}
.libCard .seal{position:absolute;right:14px;bottom:-26px;width:56px;height:56px;border-radius:50%;mix-blend-mode:multiply}
.libCard.outline{opacity:.3}
.setCard{background:#fff;border:1px solid #d8d0c0;border-radius:12px;padding:12px 14px;display:grid;gap:8px}
.sheetsRow{display:flex;flex-wrap:wrap;gap:16px;align-items:flex-start;min-height:40px}
.pad{height:900px}
#dragbox{position:fixed;left:520px;top:330px;width:300px;pointer-events:none}
</style>
<div id="bar"></div>
<div id="stage"><div class="libBar"><span id="libTab"><button data-t="current">Current</button><button data-t="done">Completed</button></span></div><div id="libBody">
  <section class="laserSection" data-laser-area="ready"><h2>Laser cutting</h2><div class="laserAreaItems" id="readyList"></div></section>
  <section class="laserSection" data-laser-area="pending"><h2>In progress</h2><div class="laserAreaItems" id="pendingList"></div></section>
  <div class="pad"></div>
</div></div>
<script>
const PV = 'data:image/svg+xml;utf8,' + encodeURIComponent('<svg xmlns="http://www.w3.org/2000/svg" width="260" height="120"><rect width="260" height="120" fill="#faf7f0"/><g fill="#c9a65c"><circle cx="40" cy="40" r="14"/><circle cx="90" cy="70" r="10"/><rect x="130" y="30" width="30" height="30" rx="6"/><circle cx="200" cy="60" r="16"/></g></svg>');
const SEAL = '<svg viewBox="0 0 120 120"><circle cx="60" cy="60" r="54" fill="none" stroke="#296c58" stroke-width="5"/><text x="60" y="66" text-anchor="middle" font-size="16" fill="#296c58">LASER READY</text></svg>';
function card(id, label) {
  const it = document.createElement('div'); it.className = 'librarySheet'; it.dataset.laserCard = 'sheet';
  it.innerHTML = '<div class="libCard" data-id="' + id + '"><b>' + label + '</b><img class="pv" src="' + PV + '" alt=""><canvas width="200" height="24"></canvas><span class="m">77 / 77 · 73% · 28 / 28</span><span class="seal" tabindex="0" title="Laser ready by Anna">' + SEAL + '</span></div>';
  const c = it.querySelector('canvas').getContext('2d'); c.fillStyle = '#c9a65c'; c.fillRect(0, 0, 90, 24); c.fillStyle = '#296c58'; c.fillRect(90, 0, 60, 24);
  return it;
}
const at = (list, id, label) => { const n = card(id, label); document.getElementById(list).appendChild(n); return n; };
at('pendingList', 'a', 'Sheet A'); at('pendingList', 'b', 'Sheet B'); at('pendingList', 'c', 'Sheet C'); at('pendingList', 'd', 'Sheet D');
at('readyList', 'r', 'Sheet R');
const q = s => document.querySelector(s), item = id => q('.libCard[data-id="' + id + '"]').closest('.librarySheet');
/** The frames of a flight: where the copy is each frame, and where its target is. */
window.rec = (targetId) => {
  const out = []; let on = true; const t0 = performance.now();
  (function f() {
    const b = q('.fxBox');
    if (b) { const r = b.getBoundingClientRect(), cs = getComputedStyle(b), tg = targetId && q('.libCard[data-id="' + targetId + '"]'), tr = tg && tg.closest('.librarySheet').getBoundingClientRect();
      out.push({ t: Math.round(performance.now() - t0), x: r.left + r.width / 2, y: r.top + r.height / 2, w: r.width, op: +cs.opacity, seals: b.querySelectorAll('.seal').length, imgs: b.querySelectorAll('img.pv').length, canvases: b.querySelectorAll('canvas').length, tx: tr ? tr.left + tr.width / 2 : null, ty: tr ? tr.top + tr.height / 2 : null }); }
    if (on) requestAnimationFrame(f);
  })();
  return () => { on = false; return out; };
};
window.centre = el => { const r = el.getBoundingClientRect(); return { x: r.left + r.width / 2, y: r.top + r.height / 2, w: r.width }; };
window.state = () => ({ copies: document.querySelectorAll('.fxBox').length, hiddenAttr: document.querySelectorAll('[data-fx-hide]').length, active: LibraryFx.active(), layer: (document.getElementById('motionLayer') || { children: [] }).children.length,
  hiddenCards: [...document.querySelectorAll('.libCard')].filter(c => getComputedStyle(c.closest('.librarySheet')).visibility === 'hidden').length });
window.errors = []; addEventListener('error', e => errors.push(e.message)); addEventListener('unhandledrejection', e => errors.push(String(e.reason)));
</script>`;

const dist = (a, b) => Math.hypot(a.x - b.x, a.y - b.y);
function arc(frames) {
  const S = frames[0], E = frames[frames.length - 1], L = dist(S, E) || 1;
  let dev = 0, back = 0, jump = 0;
  for (let i = 0; i < frames.length; i++) {
    const f = frames[i]; dev = Math.max(dev, Math.abs((E.x - S.x) * (S.y - f.y) - (S.x - f.x) * (E.y - S.y)) / L);
    if (i) { jump = Math.max(jump, dist(f, frames[i - 1]) / L); back = Math.max(back, dist(S, frames[i - 1]) - dist(S, f)); }
  }
  return { dev, back, jump, L };
}

(async () => {
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const ok = [];
  try {
    const ctx = await browser.newContext({ viewport: { width: 1200, height: 1000 } });
    const page = await ctx.newPage(), perr = [];
    page.on('pageerror', e => perr.push(e.message));
    await page.setContent(PAGE);
    await page.addScriptTag({ path: path.join(root, 'charm-nest-library-fx.js') });
    const reset = () => page.evaluate(() => { LibraryFx.stop(); document.querySelectorAll('.fxBox,#motionLayer').forEach(n => n.remove()); document.getElementById('stage').scrollTop = 0; });
    const rest = async () => { const s = await page.evaluate(() => state()); assert.deepEqual([s.copies, s.hiddenAttr, s.active, s.layer, s.hiddenCards], [0, 0, 0, 0, 0], 'nothing is left on the page: ' + JSON.stringify(s)); };
    const shot = async name => { if (SHOTS) await page.screenshot({ path: path.join(SHOTS, name + '.png') }); };
    assert.equal(await page.evaluate(() => typeof LibraryFx.fly + typeof LibraryFx.flyBack + typeof LibraryFx.pulse), 'functionfunctionfunction');

    // 1 · a card in its list flies to the other section: the page moves the very card there a moment after the drop
    {
      const r = await page.evaluate(async () => {
        const a = item('a'), calls = []; const stop = rec('a'); const t0 = performance.now();
        const flight = LibraryFx.fly(a, q('[data-laser-area=ready]'), { kind: 'sheet', onDone: x => calls.push(x.how) });
        const first = { hidden: getComputedStyle(a).visibility, copies: document.querySelectorAll('.fxBox').length, copyHasSeals: document.querySelector('.fxBox').querySelectorAll('.seal').length, own: a.querySelectorAll('.seal').length };
        const from = centre(a);
        await new Promise(r => setTimeout(r, 70)); document.getElementById('readyList').appendChild(a);          // (the render: the same node moves)
        const hiddenAtNew = getComputedStyle(a).visibility;
        const res = await flight; const frames = stop();
        return { frames, res, calls, first, from, hiddenAtNew, ms: performance.now() - t0, end: centre(a), vis: getComputedStyle(a).visibility, st: state() };
      });
      assert.equal(r.first.hidden, 'hidden', 'the card is hidden the moment its copy lifts off');
      assert.equal(r.first.copies, 1); assert.equal(r.first.copyHasSeals, r.first.own, 'the copy carries the same seals');
      assert.equal(r.hiddenAtNew, 'hidden', 'drawn at its new place it waits, hidden, for the copy');
      const F = r.frames, S = F[0], E = F[F.length - 1];
      assert(dist(S, r.from) < 30, 'sets off from the card: ' + dist(S, r.from));
      assert(F.every(f => f.seals === r.first.own && f.imgs === 1 && f.canvases === 1), 'preview, canvas and seals ride along on every frame');
      assert(dist(E, r.end) < 12 && Math.abs(E.w - r.end.w) < 10, 'ends on the card at its new place: ' + dist(E, r.end));
      const a = arc(F); assert(a.dev > 8, 'one arc, not a straight line: ' + a.dev.toFixed(1)); assert(a.back < 6, 'never backs up: ' + a.back); assert(a.jump < .3, 'no jump between frames: ' + a.jump.toFixed(2));
      assert(r.ms >= 600 && r.ms <= 950, 'the flight takes 600-900 ms: ' + r.ms);
      assert.equal(r.vis, 'visible'); assert.equal(r.res.how, 'landed'); assert.deepEqual(r.calls, ['landed'], 'onDone once');
      assert.deepEqual([r.st.copies, r.st.hiddenAttr, r.st.active], [0, 0, 0]);
      ok.push(`a card flies to the other section on one arc (${a.dev.toFixed(0)} px off the chord of ${a.L.toFixed(0)}), with its preview, canvas and ${r.first.own} seal, ends on its real card (${dist(E, r.end).toFixed(1)} px), ${Math.round(r.ms)} ms, the copy is gone`);
    }
    await reset(); await rest();

    // 2 · the page draws the card anew (a new node) mid-flight: the copy lands on that one, which was hidden when drawn
    {
      const r = await page.evaluate(async () => {
        const b = item('b'), stop = rec('b'); const flight = LibraryFx.fly(b, q('[data-laser-area=ready]'), { kind: 'sheet' });
        await new Promise(r => setTimeout(r, 160)); b.remove(); const n = card('b', 'Sheet B'); document.getElementById('readyList').appendChild(n);
        await Promise.resolve(); const hidden = getComputedStyle(n).visibility;   // (the page's observer has run: before any paint)
        const res = await flight; const frames = stop();
        return { frames, res, hidden, end: centre(n), vis: getComputedStyle(n).visibility };
      });
      const E = r.frames[r.frames.length - 1];
      assert.equal(r.hidden, 'hidden'); assert(dist(E, r.end) < 12, 'lands on the new node: ' + dist(E, r.end)); assert.equal(r.vis, 'visible'); assert.equal(r.res.how, 'landed');
      assert(arc(r.frames).jump < .3, 'the target appearing does not make the copy jump: ' + arc(r.frames).jump.toFixed(2));
      ok.push('the page draws the card anew mid-flight: the new card is hidden when drawn, the copy settles onto it without a jump');
    }
    await reset(); await rest();

    // 3 · a drag's own copy (a fixed box under the pointer) is flown from where it is, the card in the list was a faint outline
    {
      const r = await page.evaluate(async () => {
        const c = item('c'); c.querySelector('.libCard').classList.add('outline');
        const box = document.createElement('div'); box.id = 'dragbox'; box.appendChild(c.cloneNode(true)); document.body.appendChild(box);
        const from = centre(box), stop = rec('c'), calls = []; const t0 = performance.now();
        const flight = LibraryFx.fly(box, q('[data-laser-area=ready]'), { kind: 'sheet', duration: 720, onDone: x => calls.push([x.how, document.body.contains(box)]) });
        await new Promise(r => setTimeout(r, 120)); c.remove(); const n = card('c', 'Sheet C'); document.getElementById('readyList').appendChild(n);
        const res = await flight; const frames = stop();
        return { frames, from, res, calls, gone: !document.body.contains(box), end: centre(n), vis: getComputedStyle(n).visibility, ms: performance.now() - t0 };
      });
      const F = r.frames, S = F[0], E = F[F.length - 1];
      assert(dist(S, r.from) < 30, 'starts from the carried copy, not from the list: ' + dist(S, r.from));
      assert(dist(E, r.end) < 12, 'ends on the card at its new place: ' + dist(E, r.end));
      assert(r.gone && r.res.how === 'landed' && r.vis === 'visible'); assert.equal(r.calls.length, 1); assert.equal(r.calls[0][1], true, 'onDone comes before the copy goes (the caller clears its outline first)');
      ok.push('a drag\'s carried copy is adopted: it flies on from where it is under the pointer, lands, is removed; onDone once, before its last fade');
    }
    await reset(); await rest();

    // 4 · the page is scrolled while it flies: the copy follows its target and ends on it
    {
      const r = await page.evaluate(async () => {
        const st = document.getElementById('stage'); const d = item('d'); const stop = rec('d');
        const flight = LibraryFx.fly(d, q('[data-laser-area=ready]'), { kind: 'sheet' });
        await new Promise(r => setTimeout(r, 50)); document.getElementById('readyList').appendChild(d);
        await new Promise(r => setTimeout(r, 150)); st.scrollTop = 60;
        await flight; return { frames: stop() };
      });
      const E = r.frames[r.frames.length - 1];
      assert(Math.hypot(E.x - E.tx, E.y - E.ty) < 14, 'ends on its target although the page scrolled: ' + Math.hypot(E.x - E.tx, E.y - E.ty).toFixed(1));
      ok.push('the page scrolled mid-flight: the copy ends on its card all the same');
    }
    await reset(); await rest();

    // 5 · a tab: the copy shrinks into it, fades, the tab answers; the card's old place is given back if nobody took it
    {
      await page.evaluate(() => { const n = card('t', 'Sheet T'); document.getElementById('pendingList').appendChild(n); });
      const r = await page.evaluate(async () => {
        const t = item('t'), tab = q('#libTab [data-t=done]'), stop = rec(null); const t0 = performance.now(), calls = [];
        const res = await LibraryFx.fly(t, tab, { kind: 'sheet', onDone: x => calls.push(x.how) });
        const frames = stop(); const tr = centre(tab);
        return { frames, res, calls, tr, ms: performance.now() - t0, vis: getComputedStyle(t).visibility, st: state() };
      });
      const F = r.frames, E = F[F.length - 1];
      assert(dist(E, r.tr) < 25 && E.w < 150, 'shrinks into the tab: ' + dist(E, r.tr).toFixed(1) + ' px, ' + E.w.toFixed(0) + ' wide');
      assert(F.every(f => f.y > 50), 'never under the top bar');
      assert.equal(r.res.how, 'absorbed'); assert.deepEqual(r.calls, ['absorbed']);
      await page.waitForFunction(() => LibraryFx.active() === 0 && document.querySelector('.libCard[data-id=t]').closest('.librarySheet').style.visibility !== 'hidden' && !document.querySelector('[data-fx-hide]'), null, { timeout: 3000 });
      ok.push('flight to a tab: shrinks into it and fades, never under the top bar; the card is shown again at the cap when the page never took it away');
      await page.evaluate(() => item('t').remove());
    }
    await reset(); await rest();

    // 6 · reduced motion: no travel, a short cross-fade
    {
      await page.emulateMedia({ reducedMotion: 'reduce' });
      await page.evaluate(() => { const n = card('m', 'Sheet M'); document.getElementById('pendingList').appendChild(n); });
      const r = await page.evaluate(async () => {
        const m = item('m'), stop = rec('m'); const t0 = performance.now();
        const flight = LibraryFx.fly(m, q('[data-laser-area=ready]'), { kind: 'sheet' });
        await new Promise(r => setTimeout(r, 40)); document.getElementById('readyList').appendChild(m);
        const res = await flight; return { frames: stop(), res, ms: performance.now() - t0, vis: getComputedStyle(m).visibility, st: state() };
      });
      const F = r.frames, S = F[0];
      assert(F.every(f => dist(f, S) < 3), 'the copy does not travel under reduced motion');
      assert(r.ms < 700 && r.res.how === 'faded' && r.vis === 'visible', 'a short fade: ' + Math.round(r.ms) + ' ms, ' + r.res.how);
      assert.deepEqual([r.st.copies, r.st.hiddenAttr, r.st.active], [0, 0, 0]);
      ok.push(`reduced motion: no travel, a ${Math.round(r.ms)} ms cross-fade, the card shown, nothing left`);
      await page.emulateMedia({ reducedMotion: 'no-preference' });
      await page.evaluate(() => item('m').remove());
    }
    await reset(); await rest();

    // 7 · a target that is missing, detached, hidden or never given, and calls with nothing sensible: never throws, never leaves anything
    {
      const r = await page.evaluate(async () => {
        const a = item('a'), out = [];
        const gone = document.createElement('div'); const hid = document.createElement('div'); hid.style.display = 'none'; document.body.appendChild(hid);
        for (const to of [null, undefined, gone, hid, 'str', 42, {}]) { const t0 = performance.now(); const res = await LibraryFx.fly(a, to, { kind: 'sheet' }); out.push([res.how, Math.round(performance.now() - t0), getComputedStyle(a).visibility, document.querySelectorAll('.fxBox').length]); }
        for (const [f, t] of [[null, null], [undefined, undefined], ['x', null], [{}, {}], [document.createElement('div'), q('#bar')], [q('#bar'), q('#stage')]]) { const res = await LibraryFx.fly(f, t); out.push([res.how, 0, 'visible', document.querySelectorAll('.fxBox').length]); const b = await LibraryFx.flyBack(f, t); out.push([b.how, 0, 'visible', document.querySelectorAll('.fxBox').length]); }
        const bad = [LibraryFx.pulse(null), LibraryFx.pulse(undefined), LibraryFx.pulse(gone), LibraryFx.pulse('x'), LibraryFx.pulse(hid)];
        hid.remove();
        return { out, bad, pulsed: LibraryFx.pulse(item('b')), st: state(), errors };
      });
      for (const [how, ms, vis, copies] of r.out) { assert(/faded|none|landed|cap/.test(how), 'resolves: ' + how); assert(ms < 900, 'quick: ' + ms); assert.equal(vis, 'visible', 'the card is back where it was'); assert.equal(copies, 0); }
      assert.deepEqual(r.bad, [false, false, false, false, false]); assert.equal(r.pulsed, true);
      assert.deepEqual([r.st.copies, r.st.hiddenAttr, r.st.active], [0, 0, 0]); assert.deepEqual(r.errors, []);
      ok.push(`a missing, detached, hidden or nonsense target (and nonsense arguments, flyBack and pulse too): resolves in under 900 ms, the card is back in its place, no throw (${r.out.length} calls)`);
    }
    await reset(); await rest();

    // 8 · the target vanishes mid-flight (a drop zone gone at the drop): the last place is used, then the real card
    {
      const r = await page.evaluate(async () => {
        const a = item('a'); const zone = document.createElement('div'); zone.dataset.laserArea = 'ready'; zone.style.cssText = 'position:fixed;left:700px;top:90px;width:400px;height:200px'; document.body.appendChild(zone);
        const stop = rec('a'); const flight = LibraryFx.fly(a, zone, { kind: 'sheet' });
        await new Promise(r => setTimeout(r, 120)); zone.remove();                                       // (the zone is gone)
        await new Promise(r => setTimeout(r, 120)); a.remove(); const n = card('a', 'Sheet A'); document.getElementById('readyList').appendChild(n);
        const res = await flight; return { frames: stop(), res, end: centre(n), st: state(), vis: getComputedStyle(n).visibility };
      });
      const E = r.frames[r.frames.length - 1];
      assert(r.res.how === 'landed' && dist(E, r.end) < 12 && r.vis === 'visible', 'lands on the real card though the zone went: ' + r.res.how + ' ' + dist(E, r.end));
      assert.deepEqual([r.st.copies, r.st.hiddenAttr, r.st.active], [0, 0, 0]);
      ok.push('the drop zone vanishes mid-flight: the flight goes on to its last place and lands on the real card when it is drawn');
      await page.evaluate(() => { item('a').remove(); const n = card('a', 'Sheet A'); document.getElementById('pendingList').appendChild(n); });
    }
    await reset(); await rest();

    // 9 · the page never draws the card at its new place (a refused or slow write): the copy waits, then goes by the cap; the card is given back
    {
      const r = await page.evaluate(async () => {
        const a = item('a'), t0 = performance.now(); const stop = rec(null);
        const res = await LibraryFx.fly(a, q('[data-laser-area=ready]'), { kind: 'sheet', duration: 5000 });
        return { res, ms: performance.now() - t0, vis: getComputedStyle(a).visibility, st: state(), frames: stop(), inList: a.parentNode.id };
      });
      assert(r.ms <= 1600, 'the hard cap holds (duration asked: 5000 ms): ' + Math.round(r.ms));
      assert(r.vis === 'visible' && r.inList === 'pendingList', 'the card is back in its place');
      assert.deepEqual([r.st.copies, r.st.hiddenAttr, r.st.active], [0, 0, 0]);
      ok.push(`no card is ever drawn at the target: the copy waits above its place, the hard cap ends it at ${Math.round(r.ms)} ms (1500 max), the card is given back`);
    }
    await reset(); await rest();

    // 10 · a hidden page ends it at once
    {
      const r = await page.evaluate(async () => {
        const a = item('a'); const flight = LibraryFx.fly(a, q('[data-laser-area=ready]'), { kind: 'sheet' });
        await new Promise(r => setTimeout(r, 150));
        Object.defineProperty(document, 'hidden', { configurable: true, get: () => true }); const t0 = performance.now();
        document.dispatchEvent(new Event('visibilitychange')); const res = await flight; const ms = performance.now() - t0;
        const st = state(); delete document.hidden; return { res, ms, st, vis: getComputedStyle(a).visibility };
      });
      assert(r.res.how === 'hidden' && r.ms < 100 && r.vis === 'visible', 'ended where it was: ' + r.res.how + ' ' + Math.round(r.ms));
      assert.deepEqual([r.st.copies, r.st.hiddenAttr, r.st.active], [0, 0, 0]);
      ok.push('a hidden page ends the flight at once, copy removed, card given back');
    }
    await reset(); await rest();

    // 11 · the same card flown again and again, and several cards at once: nothing piles up
    {
      const r = await page.evaluate(async () => {
        const a = item('a'), seen = []; let n = 0;
        const ps = []; for (let i = 0; i < 6; i++) { ps.push(LibraryFx.fly(a, q('[data-laser-area=ready]'), { kind: 'sheet' })); await new Promise(r => setTimeout(r, 60)); seen.push(document.querySelectorAll('.fxBox').length); }
        const hows = (await Promise.all(ps)).map(x => x.how);
        const many = [item('b'), item('c'), item('d'), item('r')].map(x => LibraryFx.fly(x, q('[data-laser-area=ready]'), { kind: 'sheet' }));
        const peak = document.querySelectorAll('.fxBox').length; await Promise.all(many);
        return { hows, seen, peak, st: state(), errors };
      });
      assert(r.seen.every(n => n === 1), 'one copy of a card at a time: ' + r.seen); assert.equal(r.hows.filter(h => h === 'superseded').length, 5); assert(r.peak >= 3 && r.peak <= 6, 'several cards fly at once: ' + r.peak);
      assert.deepEqual([r.st.copies, r.st.hiddenAttr, r.st.active, r.st.layer], [0, 0, 0, 0]); assert.deepEqual(r.errors, []);
      ok.push(`six flights of one card in a row (five superseded, one copy at a time) and ${r.peak} cards at once: nothing left over`);
    }
    await reset(); await rest();

    // 12 · a card that could not move goes home: the copy resting at the target flies back onto the original, which is only landed on
    {
      const r = await page.evaluate(async () => {
        const a = item('a'); a.querySelector('.libCard').classList.add('outline');
        const box = document.createElement('div'); box.id = 'dragbox'; box.style.left = '700px'; box.style.top = '120px'; box.appendChild(a.cloneNode(true)); document.body.appendChild(box);
        const stop = rec(null), calls = []; const t0 = performance.now(), from = centre(box);
        const res = await LibraryFx.flyBack(box, a, { kind: 'sheet', duration: 700, onDone: x => { calls.push([x.how, document.body.contains(box), !!document.querySelector('[data-fx-hide]')]); a.querySelector('.libCard').classList.remove('outline'); } });
        return { frames: stop(), res, calls, from, end: centre(a), ms: performance.now() - t0, gone: !document.body.contains(box), vis: getComputedStyle(a).visibility, st: state() };
      });
      const F = r.frames, S = F[0], E = F[F.length - 1];
      assert(dist(S, r.from) < 30, 'sets off from the copy at the target'); assert(dist(E, r.end) < 12, 'ends on the original: ' + dist(E, r.end));
      assert(arc(F).dev > 6, 'an arc home too'); assert(r.gone && r.res.how === 'landed' && r.vis === 'visible');
      assert.deepEqual(r.calls, [['landed', true, false]], 'onDone once, copy still there, nothing hidden by it');
      assert.deepEqual([r.st.copies, r.st.hiddenAttr, r.st.active], [0, 0, 0]);
      ok.push('flyBack: the copy resting at the target flies home on an arc and lands on the original; onDone clears the outline before the last fade');
    }
    await reset(); await rest();

    // 13 · a set card flies as one whole (its sheets and their seals ride along), and a sheet into a set's row
    {
      await page.evaluate(() => {
        for (const id of ['b', 'c', 'd']) item(id).remove();
        const s = document.createElement('div'); s.className = 'setCard'; s.dataset.laserCard = 'set'; s.id = 'setX';
        s.innerHTML = '<b>Set 1 · 2026-10-03</b><div class="sheetsRow"></div>'; document.getElementById('pendingList').appendChild(s);
        for (const [id, l] of [['s1', 'S1'], ['s2', 'S2']]) s.querySelector('.sheetsRow').appendChild(card(id, l));
        const t = document.createElement('div'); t.className = 'setCard'; t.id = 'setY'; t.innerHTML = '<b>Set 2</b><div class="sheetsRow"></div>'; document.getElementById('readyList').appendChild(t); t.querySelector('.sheetsRow').appendChild(card('y1', 'Y1'));
      });
      const r = await page.evaluate(async () => {
        const set = q('#setX'), stop = rec(null); const seals = set.querySelectorAll('.seal').length;
        const flight = LibraryFx.fly(set, q('[data-laser-area=ready]'), { kind: 'set' });
        const mid = await new Promise(r => setTimeout(() => r({ seals: document.querySelector('.fxBox').querySelectorAll('.seal').length, imgs: document.querySelector('.fxBox').querySelectorAll('img.pv').length }), 250));
        document.getElementById('readyList').appendChild(set); const res = await flight;
        const x = item('y1'); const stop2 = rec(null); const f2 = LibraryFx.fly(x, q('#setY'), { kind: 'sheet' }); await new Promise(r => setTimeout(r, 40)); const row = q('#setX .sheetsRow'); row.appendChild(x);
        const r2 = await f2; return { seals, mid, res, r2, end: centre(set), st: state(), vis: getComputedStyle(set).visibility, vis2: getComputedStyle(x).visibility };
      });
      assert.equal(r.mid.seals, r.seals, 'the whole set card flies with all its seals'); assert.equal(r.mid.imgs, 2);
      assert(r.res.how === 'landed' && r.vis === 'visible' && r.r2.how === 'landed' && r.vis2 === 'visible', 'both land: ' + r.res.how + ' ' + r.r2.how);
      assert.deepEqual([r.st.copies, r.st.hiddenAttr, r.st.active], [0, 0, 0]);
      ok.push(`a set card flies whole (${r.seals} seals and both previews in the air), and a sheet flies into a set card's row; both land and nothing is left`);
    }
    await reset(); await rest();

    // frames for the eye (SHOTS=<dir>): the fixture at five moments of one flight
    if (SHOTS) {
      await page.evaluate(() => { window.__frames = []; });
      for (const [i, ms] of [[0, 0], [1, 110], [2, 260], [3, 430], [4, 640], [5, 900]]) {
        await page.evaluate(async () => { document.querySelectorAll('.libCard').forEach(c => c.classList.remove('outline')); });
        await page.evaluate(ms0 => { window.__f = LibraryFx.fly(item('a'), q('[data-laser-area=ready]'), { kind: 'sheet' }); setTimeout(() => document.getElementById('readyList').appendChild(item('a')), 40); }, ms);
        await page.waitForTimeout(Math.max(1, ms)); await shot('fx-' + i + '-' + ms + 'ms'); await page.evaluate(() => window.__f); await reset();
        await page.evaluate(() => document.getElementById('pendingList').appendChild(item('a')));
      }
    }
    assert.deepEqual(perr, [], 'no page errors: ' + perr);
    console.log('library-fx: all passed'); for (const l of ok) console.log('  ✓ ' + l);
  } finally { await browser.close(); }
})().catch(e => { console.error(e); process.exit(1); });
