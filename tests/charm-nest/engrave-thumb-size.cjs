// ENGBIG (Paul, 13 Oct 2026 03:04 UTC): "Enlarge the engraving thumbnail size in this modal to be 2x larger."
// The pictures in the order window's Engraving cards are twice as large: the card's own rules gave them 80px (a card under 232px wide) or 104px (a wider one); in the order window's grid they are now
// 160px and 208px (charm-nest-1.html: --engPx on #owEng, set by the section's width at the same cards; the Engrave tab's detail card keeps the card file's own rules).
// This test runs the REAL page in headless Chromium over ENGLIVE's real-drawing world (tests/charm-nest/_engraveRealWorld.cjs: real .ai drawings, real intake, pool, classifier, fit; offline, nothing
// written anywhere real, no Etsy, no paid call, made-up names), at a screen density of 2 (a retina screen), at 1024x768, 1280x720, 1356x768, 1440x900 and 1920x1080, and checks, for a single-face card
// (back only), a front + back card, and three single-face cards (discs):
//   1  every picture is square and exactly the edge the rule gives for that width: 160px, or 208px where the old one was 104px (the grid's own --engPx says the same)
//   2  the tracks: 3 from a 548px section (4 from 840px, 5 from 1060px), 2 under 548px, 1 under 362px; a front + back card spans two tracks (one in a one-track grid)
//   3  nothing clips or overlaps: every part of a card is inside it, a card is inside the section, cards never overlap, no sideways scroll anywhere; the Engraving button is whole and below the pictures
//   4  one face: the label, the picture and the words one under the other, the card as wide as one track; front + back: the two pictures side by side (one over the other only in a card too narrow for both)
//   5  the pictures are drawn crisp: the canvas of every picture is at least as many pixels as the box shows (css pixels x screen density), not a small bitmap stretched
//   6  the Engrave tab's detail card is NOT enlarged (its pictures keep the card file's own sizes)
//   7  PRINTED (the intended trade, no assertion): at 1280x720 and 1440x900, the cards per row, the card heights, and how much of the section lies below the fold; the window's own scrolling reaches the last card
//   PW_DIR=/opt/node22/lib/node_modules/playwright/node_modules node tests/charm-nest/engrave-thumb-size.cjs      (about 2-3 minutes; SHOTS=<dir> keeps screenshots)
// What a pass does NOT prove: a live order, real hands on goldenspike.app, any browser but headless Chromium, screen densities other than 2 (the canvas rule is arithmetic, checked at 2).
'use strict';
const fs = require('fs'), path = require('path');
const R = require('./_engraveRealWorld.cjs');
const SHOTS = process.env.SHOTS || '';
const pwDir = ['/opt/node22/lib/node_modules/playwright/node_modules', path.join(R.root, 'node_modules')].find(d => fs.existsSync(path.join(d, 'playwright-core')));
if (!process.env.PW_DIR && pwDir) process.env.PW_DIR = pwDir;
if (!process.env.CHROMIUM) { const c = ['/opt/pw-browsers/chromium-1194/chrome-linux/chrome', '/opt/pw-browsers/chromium'].find(x => fs.existsSync(x)); if (c) process.env.CHROMIUM = c; }

let failed = 0, passed = 0;
const check = (ok, msg, extra) => { if (ok) { passed++; console.log('  PASS ' + msg); } else { failed++; console.log('  FAIL ' + msg + (extra !== undefined ? ' :: ' + JSON.stringify(extra) : '')); } };
const near = (a, b, e = 0.6) => Math.abs(a - b) <= e;
const SIZES = [[1024, 768], [1280, 720], [1356, 768], [1440, 900], [1920, 1080]];

// the rule (charm-nest-1.html), written again here on purpose: the picture's edge and the number of tracks for a section of S css px
const edgeFor = S => ((S >= 714 && S <= 839) || (S >= 955 && S <= 1059) || S >= 1196 ? 208 : 160);
const colsFor = S => (S < 362 ? 1 : S < 548 ? 2 : S < 840 ? 3 : S < 1060 ? 4 : 5);

/** everything read from the order window's section, in css px (window coordinates) */
const READ = () => {
  const q = s => document.querySelector(s), rect = e => { if (!e) return null; const r = e.getBoundingClientRect(); return { l: r.left, t: r.top, r: r.right, b: r.bottom, w: r.width, h: r.height }; };
  const sec = q('#owEngSec'), grid = q('#owEng'), sc = q('#orderWin .owVInfo .owMain') || q('#orderWin .owMain'), dl = q('#orderWin');
  const cards = [...document.querySelectorAll('#owEng > .engCard')];
  const gcs = grid ? getComputedStyle(grid) : null;
  return {
    dpr: devicePixelRatio, vw: innerWidth, vh: innerHeight, sec: rect(sec), grid: rect(grid), sc: rect(sc), dl: rect(dl), tracks: gcs ? gcs.gridTemplateColumns.split(' ').length : 0, px: gcs ? gcs.getPropertyValue('--engPx').trim() : '',
    hscroll: { page: document.documentElement.scrollWidth - document.documentElement.clientWidth, grid: grid ? grid.scrollWidth - grid.clientWidth : 0, sec: sec ? sec.scrollWidth - sec.clientWidth : 0, main: sc ? sc.scrollWidth - sc.clientWidth : 0 },
    scrollable: sc ? sc.scrollHeight - sc.clientHeight : 0,
    cards: cards.map(c => {
      const r = c.getBoundingClientRect(), faces = [...c.querySelectorAll('.engFace')];
      const spill = [...c.querySelectorAll('*')].filter(e => { const b = e.getBoundingClientRect(); return b.width > 0 && b.height > 0 && (b.right > r.right + 0.6 || b.left < r.left - 0.6 || b.bottom > r.bottom + 0.6 || b.top < r.top - 0.6); }).slice(0, 3).map(e => (e.className && String(e.className).split(' ')[0]) + ':' + e.tagName.toLowerCase());
      const btn = c.querySelector('.egOpen'), br = rect(btn);
      return { r: rect(c), n: faces.length, title: (c.querySelector('.engName') || {}).textContent, spill, scrollW: c.scrollWidth - c.clientWidth, btn: br, btnClip: btn ? btn.scrollWidth - btn.clientWidth : 0,
        faces: faces.map(f => {
          const th = f.querySelector('.engThumb'), lb = f.querySelector('.engLbl'), tx = f.querySelector('.engTxt'), cv = [...f.querySelectorAll('.engThumb .engpvDraw canvas')];
          return { face: f.dataset.face, thumb: rect(th), label: rect(lb), words: rect(tx), canv: cv.map(k => { const b = k.getBoundingClientRect(); return { px: [k.width, k.height], css: [b.width, b.height] }; }), drawn: !!f.querySelector('.engpv') };
        }) };
    })
  };
};

async function main() {
  const w = await R.world({ viewport: { width: 1280, height: 720 }, shots: SHOTS || undefined, scale: 2 });
  if (!w) { console.log('  - no playwright-core: the browser checks were not run (set PW_DIR=<dir holding playwright-core>)'); return 0; }
  const page = w.page;
  try {
    const made = await R.build(w, ['back', 'both', 'discs']);
    for (const n of ['back', 'both', 'discs']) if (!made[n] || !made[n].jobs.length) { check(false, `the world made no engraving job for ${n}`); return finish(); }
    const info = {};
    for (const [W, H] of SIZES) {
      const at = `${W}x${H}`;
      await page.setViewportSize({ width: W, height: H }); await R.settle(page, 400);
      for (const n of ['back', 'both', 'discs']) {
        await R.openOrderWindow(w, n); await page.evaluate(() => { const sc = document.querySelector('#orderWin .owVInfo .owMain') || document.querySelector('#orderWin .owMain'); if (sc) sc.scrollTop = 0; });
        const m = await page.evaluate(READ), S = m.sec.w, id = `${at} ${n}`;
        info[`${at}:${n}`] = m;
        check(m.cards.length === (n === 'discs' ? 3 : 1), `${id}: ${n === 'discs' ? 3 : 1} card(s) drawn`, m.cards.length);
        const edge = S < 548 ? 160 : edgeFor(S);
        check(m.tracks === colsFor(S) && m.px === edge + 'px', `${id}: the section is ${Math.round(S)}px wide: ${colsFor(S)} track(s) and a ${edge}px picture (the grid has ${m.tracks}, --engPx ${m.px})`, { tracks: m.tracks, px: m.px });
        const track = (m.grid.w - 10 * (m.tracks - 1)) / m.tracks;
        for (const c of m.cards) {
          const cid = `${id}${n === 'discs' ? ' disc' : ''}`;
          check(c.faces.length === c.n && c.faces.every(f => f.thumb && near(f.thumb.w, edge) && near(f.thumb.h, edge)), `${cid}: every picture is ${edge}x${edge}px (${c.faces.map(f => f.thumb && f.thumb.w.toFixed(1) + 'x' + f.thumb.h.toFixed(1)).join(', ')})`);
          const wantW = c.n === 2 && m.tracks > 1 ? 2 * track + 10 : track;
          check(near(c.r.w, wantW, 1.5), `${cid}: the card is ${c.r.w.toFixed(1)}px wide, ${c.n === 2 && m.tracks > 1 ? 'two tracks' : 'one track'} (${wantW.toFixed(1)})`);
          check(c.spill.length === 0 && c.scrollW <= 1 && c.btnClip <= 1, `${cid}: every part of the card is inside it; the button is whole`, { spill: c.spill, scrollW: c.scrollW, btnClip: c.btnClip });
          check(c.r.l >= m.sec.l - 0.6 && c.r.r <= m.sec.r + 0.6, `${cid}: the card is inside the section`);
          const fs = c.faces, lowest = Math.max(...fs.map(f => (f.words ? f.words.b : f.thumb.b)));
          check(c.btn && c.btn.t >= lowest - 0.6 && c.btn.b <= c.r.b + 0.6, `${cid}: the Engraving button is under the pictures and inside the card`, { btn: c.btn, lowest, card: c.r.b });
          if (c.n === 1) { const f = fs[0]; check(f.label.b <= f.thumb.t + 1 && (!f.words || f.thumb.b <= f.words.t + 1) && near(f.thumb.l, c.r.l + 8, 3), `${cid}: label, picture, words one under the other, the picture at the card's left`, f); }
          else {
            const side = near(fs[0].thumb.t, fs[1].thumb.t, 1) && fs[0].thumb.r <= fs[1].thumb.l + 0.6, stack = fs[0].thumb.b <= fs[1].label.t + 1;
            check(side || stack, `${cid}: front and back stand ${side ? 'side by side' : 'one over the other'}`, fs.map(f => f.thumb));
            check(side || (c.r.w - 16) < 2 * edge + 6, `${cid}: they stand one over the other only when the card is too narrow for both`, { side, w: c.r.w });
          }
          for (const f of fs) for (const k of f.canv) {
            const need = Math.max(k.css[0], k.css[1]) * m.dpr;
            check(Math.max(k.px[0], k.px[1]) >= need - 0.5 && f.drawn, `${cid} ${f.face}: the picture is drawn crisp (canvas ${k.px.join('x')} for ${k.css.map(x => x.toFixed(0)).join('x')} css px at density ${m.dpr}: needs ${Math.ceil(need)})`);
          }
        }
        const cs = m.cards, over = cs.some((a, i) => cs.some((b, j) => j > i && a.r.l < b.r.r - 0.6 && b.r.l < a.r.r - 0.6 && a.r.t < b.r.b - 0.6 && b.r.t < a.r.b - 0.6));
        check(!over, `${id}: no two cards overlap`);
        check(m.hscroll.page <= 1 && m.hscroll.grid <= 1 && m.hscroll.sec <= 1 && m.hscroll.main <= 1, `${id}: nothing scrolls sideways`, m.hscroll);
        if (SHOTS && (n !== 'discs' || W === 1280)) await R.shot(w, `ENGBIG-${at}-${n}`);
      }
    }

    /* the narrow case: a section under 362px (a phone) has one track, and a front + back card stacks its pictures */
    await page.setViewportSize({ width: 1280, height: 720 }); await R.settle(page, 300);
    await R.openOrderWindow(w, 'both');
    await page.evaluate(() => { document.getElementById('owEngSec').style.width = '330px'; }); await R.settle(page, 250);
    {
      const m = await page.evaluate(READ), c = m.cards[0], f = c && c.faces;
      check(m.sec.w <= 331 && m.tracks === 1 && c && near(c.r.w, m.grid.w, 1.5), `330px section: one track, the card as wide as the section (${c && c.r.w.toFixed(1)}px)`, { tracks: m.tracks });
      check(f && f.length === 2 && f[0].thumb.b <= f[1].label.t + 1 && f.every(x => near(x.thumb.w, 160) && x.thumb.r <= c.r.r + 0.6), `330px section: front over back, both 160px, whole`, f && f.map(x => x.thumb));
      check(c && c.spill.length === 0 && c.btn && c.btn.r <= c.r.r + 0.6, `330px section: nothing clipped`, c && c.spill);
      if (SHOTS) await R.shot(w, 'ENGBIG-330px-both');
      await page.evaluate(() => { document.getElementById('owEngSec').style.width = ''; });
    }

    /* the Engrave tab's detail card keeps its own (smaller) pictures */
    const jk = await R.openEngraveDetail(w, 'both', { face: 'back' });
    if (jk) {
      const d = await page.evaluate(() => [...document.querySelectorAll('.engpCard .engThumb')].map(t => { const r = t.getBoundingClientRect(); return [r.width, r.height]; }));
      check(d.length === 2 && d.every(x => x[0] <= 105 && x[1] <= 105 && x[0] > 20), `the Engrave tab's detail card is not enlarged: its pictures are ${d.map(x => x[0].toFixed(0) + 'x' + x[1].toFixed(0)).join(', ')} (the card file's own sizes, at most 104px)`, d);
    } else check(false, 'the Engrave detail opened for the front + back order');

    /* PRINTED: what the section does at 1280x720 and 1440x900 */
    for (const at of ['1280x720', '1440x900']) for (const n of ['back', 'both', 'discs']) {
      const m = info[`${at}:${n}`]; if (!m) continue;
      const vis = Math.min(m.vh, m.dl.b, m.sc.b), bottom = Math.max(...m.cards.map(c => c.r.b)), rowTop = m.cards[0].r.t;
      const perRow = m.cards.filter(c => near(c.r.t, rowTop, 2)).length;
      console.log(`  info ${at} ${n}: ${m.cards.length} card(s), ${perRow} in the first row, card ${m.cards.map(c => c.r.w.toFixed(0) + 'x' + c.r.h.toFixed(0)).join(' + ')}, picture ${m.px}, cards end at y=${bottom.toFixed(0)} of ${vis.toFixed(0)} visible (${bottom > vis ? (bottom - vis).toFixed(0) + 'px below the fold' : (vis - bottom).toFixed(0) + 'px to spare'}), the window scrolls ${m.scrollable.toFixed(0)}px`);
    }
    const both = info['1280x720:both'];
    if (both) {
      await page.setViewportSize({ width: 1280, height: 720 }); await R.settle(page, 300); await R.openOrderWindow(w, 'both');
      const reach = await page.evaluate(() => { const sc = document.querySelector('#orderWin .owVInfo .owMain') || document.querySelector('#orderWin .owMain'); sc.scrollTop = sc.scrollHeight; const c = document.querySelector('#owEng > .engCard'), r = c.getBoundingClientRect(), b = sc.getBoundingClientRect(); const out = { cardBottom: r.bottom, scBottom: b.bottom, btnBottom: c.querySelector('.egOpen').getBoundingClientRect().bottom }; sc.scrollTop = 0; return out; });
      check(reach.cardBottom <= reach.scBottom + 0.6 && reach.btnBottom <= reach.scBottom + 0.6, `1280x720 front + back: scrolling the window to its end brings the whole card, with its button, into view (card bottom ${reach.cardBottom.toFixed(0)}, window ${reach.scBottom.toFixed(0)})`, reach);
    }
    check(w.errors.length === 0, 'no script error on the page throughout', w.errors.slice(0, 3));
  } finally { await w.close(); }
  return finish();
}
function finish() { console.log(`\nengrave-thumb-size: ${passed} passed, ${failed} failed`); return failed ? 1 : 0; }
main().then(c => process.exit(c)).catch(e => { console.error(e); process.exit(1); });
