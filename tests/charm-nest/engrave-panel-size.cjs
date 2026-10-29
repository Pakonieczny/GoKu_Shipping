// BIGPANEL (Paul, 29 Oct 2026, the engraving card's left column: "Enlarge this yellow shaded areas including the text box and buttons and remove the complete "Position" title and all of the below buttons
// completely. Make sure it's tasteful, easy to use, easy to understand at a glance and does not overpower the other surrounding UI.")
// On the real sorter page over the repo's fake site (_engraveRealWorld.cjs: offline, made-up names), the Engrave detail of the order "both" (front + back), at 1440, 1280 and 1024 px wide:
//   B1  no Position title and no position buttons in the panel; the work area keeps its own Centre
//   B2  None | Front | Back 40-44 px tall, label 13-14 px; the words box shows about 3 lines at 17-18 px; Aα and Emoji at least 34 px; the font picker about 44 px, its text 15 px
//   B3  nothing overflows or overlaps: the toggles, the words box, its two buttons and the font picker stay inside the panel; the buttons sit inside the words box, beside the typed lines
//   B4  the column is no taller than it was with the Position row (MAX_COL: measured on main before this change, the tallest of the three widths, plus 2 px)
//   B5  typing still lands at once on the thumbnail (LIVEWORDS); no page errors
//   PW_DIR=<playwright node_modules> CHROMIUM=<chrome> node tests/charm-nest/engrave-panel-size.cjs      (SHOTS=<dir> for screenshots)
'use strict';
const RESULTS = [];
const check = (id, ok, evidence) => { RESULTS.push({ id, ok: !!ok }); console.log(`${ok ? 'PASS' : 'FAIL'}  ${id}  ${ok ? '' : evidence || ''}`.trimEnd()); };
const MAX_COL = +(process.env.MAX_COL || 386);   // (main before BIGPANEL, measured with this very test at 1440 / 1280 / 1024: 383 / 384 / 384 px, + 2)

(async () => {
  const R = require('./_engraveRealWorld.cjs');
  const w = await R.world({ viewport: { width: 1440, height: 900 }, shots: process.env.SHOTS || '/tmp/engrave-panel-size-shots' });
  if (!w) { console.log('SKIP no playwright-core'); return; }
  const page = w.page, errors = [];
  page.on('pageerror', e => errors.push(e.message));
  await page.waitForFunction(() => !window.CNRole || ['ask', 'role', 'admin'].includes(CNRole.state()), null, { timeout: 20000 }).catch(() => {}).then(() => page.evaluate(() => { try { if (window.CNRole && CNRole.state() === 'ask') CNRole.choose('design'); } catch (_) { /* none */ } }));
  const Q = '#egQueue .rvItem[data-kind=placement]';
  try {
    await R.build(w, ['both']);
    const heights = {};
    for (const W of [1440, 1280, 1024]) {
      await page.setViewportSize({ width: W, height: 900 });
      await R.openEngraveDetail(w, 'both', { face: 'front' }); await R.settle(page, 800);
      const m = await page.evaluate(q => {
        const c = document.querySelector(q), p = c && c.querySelector('.pvSetup .engp'), col = c && c.querySelector('.pvSetup'); if (!p) return null;
        const r = e => { const b = e.getBoundingClientRect(); return { l: b.left, r: b.right, t: b.top, b: b.bottom, w: b.width, h: b.height }; }, fs = e => parseFloat(getComputedStyle(e).fontSize), lh = e => parseFloat(getComputedStyle(e).lineHeight);
        const segs = [...p.querySelectorAll('.engpMode .engpSeg')], ta = p.querySelector('.engpWords'), tools = [...p.querySelectorAll('.engpTools button')], font = p.querySelector('.engpFont'), pr = r(p);
        const inside = e => { const b = r(e); return b.l >= pr.l - 0.5 && b.r <= pr.r + 0.5; };
        const tb = r(ta), cs = getComputedStyle(ta), padV = parseFloat(cs.paddingTop) + parseFloat(cs.paddingBottom) + parseFloat(cs.borderTopWidth) + parseFloat(cs.borderBottomWidth), textRight = tb.r - parseFloat(cs.paddingRight);
        return {
          pos: p.querySelectorAll('.engpPos,.engpPb,[data-engp="pos"]').length, posWord: /Position/.test(p.textContent), centre: !!c.querySelector('.ctl [data-a="centre"]'),
          seg: { h: Math.min(...segs.map(s => r(s).h)), fs: fs(segs[0]) }, words: { h: tb.h, fs: fs(ta), lines: Math.floor((tb.h - padV + 1) / lh(ta)) }, tools: tools.map(b => ({ h: r(b).h, inBox: r(b).t >= tb.t - 0.5 && r(b).b <= tb.b + 0.5 && r(b).r <= tb.r + 0.5, beside: r(b).l >= textRight - 0.5 })),
          font: { h: r(font).h, fs: fs(font) }, inside: segs.concat([ta, font], tools).every(inside), over: p.scrollWidth > p.clientWidth + 1, pageOver: document.documentElement.scrollWidth > innerWidth + 1,
          colH: col ? Math.round(r(col).h) : 0, panelW: Math.round(pr.w)
        };
      }, Q);
      heights[W] = m && m.colH;
      console.log(`   ${W}: ${JSON.stringify(m)}`);
      if (w.shots) await R.shot(w, 'panel-' + W).catch(() => {});
      if (!m) { check(`B0 ${W}: the panel is drawn`, false, 'no panel'); continue; }
      check(`B1 ${W}: no Position title, no position buttons; the work area keeps its Centre`, m.pos === 0 && !m.posWord && m.centre, JSON.stringify({ pos: m.pos, word: m.posWord, centre: m.centre }));
      check(`B2 ${W}: None | Front | Back 40-44 px tall, label 13-14 px`, m.seg.h >= 39.5 && m.seg.h <= 44.5 && m.seg.fs >= 13 && m.seg.fs <= 14, JSON.stringify(m.seg));
      check(`B2 ${W}: the words box shows about 3 lines at 17-18 px`, m.words.fs >= 17 && m.words.fs <= 18 && m.words.lines >= 3, JSON.stringify(m.words));
      check(`B2 ${W}: Aα and Emoji at least 34 px, inside the words box, beside the typed lines (never over them)`, m.tools.length === 2 && m.tools.every(t => t.h >= 33.5 && t.inBox && t.beside), JSON.stringify(m.tools));
      check(`B2 ${W}: the font picker about 44 px, its text 15 px`, m.font.h >= 42 && m.font.h <= 46 && m.font.fs >= 14.5 && m.font.fs <= 15.5, JSON.stringify(m.font));
      check(`B3 ${W}: nothing overflows the panel or the page`, m.inside && !m.over && !m.pageOver, JSON.stringify({ inside: m.inside, over: m.over, pageOver: m.pageOver, panelW: m.panelW }));
    }
    const tallest = Math.max(...Object.values(heights).filter(Boolean));
    console.log(`   column heights: ${JSON.stringify(heights)}`);
    check('B4 the column is no taller than it was with the Position row' + (MAX_COL ? ` (main: ${MAX_COL - 2} px)` : ' (main not measured here)'), !MAX_COL || tallest <= MAX_COL, JSON.stringify(heights));
    // B5 · live typing still lands on the thumbnail
    await page.setViewportSize({ width: 1440, height: 900 }); await R.openEngraveDetail(w, 'both', { face: 'front' }); await R.settle(page, 600);
    const live = await page.evaluate(q => new Promise(res => {
      const ta = document.querySelector(q + ' .pvSetup .engpWords'); ta.value = ta.value + 'Q'; ta.dispatchEvent(new Event('input', { bubbles: true }));
      requestAnimationFrame(() => requestAnimationFrame(() => res((document.querySelector(q + ' .pvSetup .engFace[data-face="front"] .engpvText') || {}).textContent || '')));
    }), Q);
    check('B5 a typed letter is on the thumbnail within two frames', /Q/i.test(live), live);
    check('B5 no page errors', errors.length === 0, errors.join(' | ').slice(0, 400));
  } catch (e) { console.error(e); check('ran', false, String(e && e.stack || e)); }
  finally { await w.close(); }
  const bad = RESULTS.filter(r => !r.ok);
  console.log(bad.length ? `engrave-panel-size: ${bad.length} failed` : 'engrave-panel-size: ok');
  process.exit(bad.length ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
