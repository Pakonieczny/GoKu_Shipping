// ROTBAR (rotbar-1013): THE WORK AREA'S BOTTOM ROW: ONE DEGREE BOX THAT TURNS THE WHOLE CHARM + ITS ENGRAVING, AND A BIGGER ROW (Paul, 13 Oct 2026):
//   "1. Remove the "+90", "-90" quick buttons. 2. In their place add a similar manual degree input and readout. This will control the rotation of the entire charm + engraving. Sometimes the system makes mistakes in how
//    the rotational placement of the charm is decided. The user needs the ability to quickly and in real-time update the rotation of the entire charm including the engraving text contained within the charm.
//    3. Enlarge the height of the entire bottom row of buttons and options by 33%. also enlarge each component in the row concurrently. Make them really easy to click and simple to interact with with clear feedback."
//
// What this proves (offline only: the REAL page over the repo's fake site with the real drawings, tests/charm-nest/_engraveRealWorld.cjs; made-up words only; nothing live, no Etsy, no paid call, no live write):
//   A  the +90 / -90 buttons and the old text-angle box are gone from the row; ONE degree box (with two step buttons) is there and starts at 0 = the placement the system chose
//   B  typing a degree turns the WHOLE piece in the work area as you type (no Enter): the drawn charm (outline) and the engraving turn together by that angle about the charm's centre, the engraving keeps its size and its
//      place ON the charm (checked on the geometry the page draws and on the canvas, not on the number)
//   C  Enter / leaving the box settles the value (any integer is normalised to -179..180); Up / Down step 1 degree, Shift 15; the step buttons and a held button; garbage is ignored without losing the value
//   D  Cancel puts the system's placement back exactly (the same view object, the same fit, the same canvas); Save asks "Why change it?" like any override and keeps the turn; Approve works after a turn
//   E  what the turn changes downstream: the written laser file and the saved record read the turned orientation, and the engraving sits on the SAME metal of the charm as before the turn (the sheet views' engraveOn)
//   F  a later fit (new words) keeps the person's orientation; a turn nobody asked for never happens
//   G  the row: measured heights at 1024 / 1280 / 1356 / 1440 / 1920 (was 31.94 px, now 42.5 px = exactly 1.33 x; every control exactly 1.33 x its old size; 11 px type is 14.63 px), fits without sideways overflow, hover / focus ring / pressed / disabled / applied styles
//   H  a turn reads, writes and sends nothing (no request, no cloud call, no paid look)
//   PW_DIR=/opt/node22/lib/node_modules/playwright/node_modules node tests/charm-nest/engrave-turn.cjs        (TURN_SHOTS=<dir> saves screenshots)
// A pass proves only what the page's own code does on the offline fake site; not a live order, not the live deploy.
'use strict';
const fs = require('fs'), path = require('path');
let R; try { R = require('./_engraveRealWorld.cjs'); } catch (e) { console.log('engrave-turn: tests/charm-nest/_engraveRealWorld.cjs is not here, skipped'); process.exit(0); }
const SHOT_DIR = process.env.TURN_SHOTS || '';
const SLACK = 0.36;       // pt: the exact turn may slide the words up to 2 raster pixels (0.12 mm) so the strict ink check still passes at every angle (turnTo)
const OLD_BTN_PX = 25.94;   // the buttons as they were on clean main
const OLD_ROW_PX = 31.94;   // the row as it was on clean main (1280x720 and 1440x900, with and without the spacing control): 5 px padding + 1 px rule + 25.94 px buttons

(async () => {
  const w = await R.world({ viewport: { width: 1440, height: 900 }, shots: SHOT_DIR || undefined });
  if (!w) { console.log('engrave-turn: playwright-core not found, skipped'); return; }
  let fails = 0; const check = (ok, msg, more) => { if (!ok) fails++; console.log((ok ? '  ok   ' : '  FAIL ') + msg + (more !== undefined ? '  [' + more + ']' : '')); };
  const shot = async name => { if (SHOT_DIR) { try { await R.shot(w, name); } catch (e) { console.log('  (no screenshot ' + name + ': ' + e.message + ')'); } } };
  const page = w.page;
  const requests = []; let watching = false;
  const POLL = /"op":"(getOrderPieces|cancelList)"/;
  page.on('request', r => { if (watching && !POLL.test(r.postData() || '')) requests.push(r.method() + ' ' + r.url().replace(/^https?:\/\/[^/]+/, '') + ' ' + (r.postData() || '').slice(0, 80)); });
  try {
    const made = await R.build(w, ['back', 'both']);
    const backJob = made.back.jobs.find(j => j.face === 'back'), twoJob = made.both.jobs.find(j => j.face === 'back');
    check(backJob && backJob.state === 'review' && backJob.fitted, 'the world has a REAL one-line back job (' + (backJob && backJob.text) + ') with a view, a mask and a fitted engraving');
    check(twoJob && twoJob.state === 'review' && twoJob.fitted, 'and a REAL two-line job (' + (twoJob && String(twoJob.text).replace(/\n/g, ' / ')) + '), which also shows the line-spacing control');
    if (!backJob || !twoJob) throw new Error('no real jobs to test with');
    const JK = backJob.key, TK = twoJob.key;

    /* helpers that live in the page */
    await page.evaluate(() => {
      const G = CharmNestGeom, r4 = v => Math.round(v * 1e4) / 1e4;
      window.__cv = () => document.querySelector('#egQueue .backHost canvas');
      window.__box = () => document.querySelector('#egQueue .placeView > .ctl input[data-a="turn"]');
      // the polygons of the drawn charm (the view's own members, flattened) and the engraving's points, in the view's frame
      window.__shape = key => { const j = Engrave.items().get(key), v = j.view; const polys = G.flatten(v.outline, 24); const c = G.polyCentroid(polys[0]);
        const pts = []; for (const g of j.fit.glyphs) for (const k of g.cmds) if (k.type !== 'Z') pts.push([k.x, k.y]);
        const gx = pts.reduce((a, p) => a + p[0], 0) / pts.length, gy = pts.reduce((a, p) => a + p[1], 0) / pts.length;
        return { up: v.upAngle, angleDeg: v.angleDeg, cx: v.cx, cy: v.cy, outline: polys[0].map(p => [p[0], p[1]]), area: Math.abs(c.area), centroid: [c.x, c.y], glyphPts: pts, glyphMid: [gx, gy], centre: j.fit.centre.slice(), angle: j.fit.angle || 0, size: j.fit.size, lines: j.lines.slice(), nudged: !!j.nudged, turn: Engrave.turnOf(j), holes: v.members.length, maskWH: [j.mask.w, j.mask.h], capMm: j.fit.capMm }; };
      // where the engraving sits ON THE CHARM (the charm's own front frame): the page's own engraveOn of the saved/pending record shape, the way the sheet views draw it (charm-nest-backs.js)
      window.__onCharm = (key, font) => { const j = Engrave.items().get(key), charm = Pool.charmOf(j.copies[0]);
        const back = { poolId: j.copies[0], upAngle: j.view.upAngle, centre: j.fit.centre, angle: j.fit.angle || 0, sizePt: j.fit.size, capMm: j.fit.capMm, lines: j.lines, lineGap: j.lineGap == null ? null : j.lineGap, approvedAt: 1, ...(j.face === 'front' ? { face: 'front' } : {}) };
        const e = CharmNestBacks.engraveOn(back, charm, { font: Engrave.fontForWeight(j.fit.weight, j) });
        const pts = []; for (const g of (e.glyphs || [])) for (const k of g.cmds) if (k.type !== 'Z') pts.push([k.x, k.y]);
        return { centre: e.centre.map(r4), n: pts.length, mid: [pts.reduce((a, p) => a + p[0], 0) / pts.length, pts.reduce((a, p) => a + p[1], 0) / pts.length].map(r4), first: pts.slice(0, 6).map(p => p.map(r4)) }; };
      // the canvas's pixels and where the engraving is on it relative to the charm, in canvas pixels (bc._map.tx is what the page draws with)
      window.__canvas = key => { const j = Engrave.items().get(key), bc = __cv(), m = bc._map; const o = G.flatten(j.view.outline, 24)[0]; const c = G.polyCentroid(o);
        const pts = []; for (const g of j.fit.glyphs) for (const k of g.cmds) if (k.type !== 'Z') pts.push(m.tx(k.x, k.y));
        const gm = [pts.reduce((a, p) => a + p[0], 0) / pts.length, pts.reduce((a, p) => a + p[1], 0) / pts.length], cm = m.tx(c.x, c.y);
        return { w: bc.width, h: bc.height, url: bc.toDataURL(), rel: [gm[0] - cm[0], gm[1] - cm[1]], k: m.k, dark: (() => { const d = bc.getContext('2d').getImageData(0, 0, bc.width, bc.height).data; let n = 0; for (let i = 0; i < d.length; i += 4) if (d[i] < 60 && d[i + 1] < 60 && d[i + 2] < 60) n++; return n; })() }; };
      window.__sig = key => { const j = Engrave.items().get(key); return JSON.stringify([j.fit.centre, j.fit.angle, j.fit.size, j.lines, j.wantSize, !!j.nudged, j.turnUp == null ? null : j.turnUp, j.upSystem == null ? null : j.upSystem, j.view.upAngle, j.view.angleDeg, j.mask.w, j.mask.h, G.flatten(j.view.outline, 24)[0].length, j.fit.glyphs.map(g => g.cmds.map(c => [c.type, c.x, c.y].join(',')).join(';')).join('|')]); };
      window.__rot = (p, cx, cy, deg) => { const a = deg * Math.PI / 180, c = Math.cos(a), s = Math.sin(a); return [cx + (p[0] - cx) * c - (p[1] - cy) * s, cy + (p[0] - cx) * s + (p[1] - cy) * c]; };
      // the laser file as the approval writes it (CharmNestPDF.buildBackFile with the view's own angle, the cut members and the glyphs relative to the view's centre) and read back: the middle of the ENGRAVE layer and of the cut outline,
      // in the file's own page (its size follows the turned charm)
      window.__file = async key => {
        const job = Engrave.items().get(key), view = job.view, fit = job.fit, charm = Pool.charmOf(job.copies[0]), src = sourceOf(charm.sourceId), PTm = 72 / 25.4;
        const rel = fit.glyphs.map(g => ({ cmds: g.cmds.map(c => { const o = { type: c.type }; if (c.type !== 'Z') { o.x = c.x - view.cx; o.y = c.y - view.cy; } if (c.type === 'C' || c.type === 'Q') { o.x1 = c.x1 - view.cx; o.y1 = c.y1 - view.cy; } if (c.type === 'C') { o.x2 = c.x2 - view.cx; o.y2 = c.y2 - view.cy; } return o; }) }));
        const built = await CharmNestPDF.buildBackFile({ charm, mirrored: !!charm.mirrored, parsed: src.parsed, cutMembers: view.cutMembers, cx: view.cx, cy: view.cy, angleDeg: view.angleDeg, padPt: 5 * PTm, glyphs: rel, view: 'asSeenFromBack', title: job.key, meta: { poolId: job.copies[0] } });
        const parsed = await CharmNestPDF.parseSource(built.bytes, 'x'), by = {};
        for (const p of CharmNestExport.leaves(parsed)) { const pts = p.subpaths.flatMap(sp => sp.filter(o => o[0] !== 'h').map(o => o[o.length - 1])); (by[p.layer] = by[p.layer] || []).push(...pts); }
        const mid = pts => [pts.reduce((a, p) => a + p[0], 0) / pts.length, pts.reduce((a, p) => a + p[1], 0) / pts.length];
        const eng = by['ENGRAVE'] || [], cut = Object.entries(by).filter(([l]) => /CUT/i.test(l)).flatMap(([, v]) => v);
        const e = mid(eng), c = mid(cut); return { nEng: eng.length, nCut: cut.length, vec: [e[0] - c[0], e[1] - c[1]], w: built.wPt, h: built.hPt };
      };
      // what Approve hands the file writer (a spy that calls through)
      window.__built = []; { const b0 = CharmNestPDF.buildBackFile; CharmNestPDF.buildBackFile = function (a) { try { const first = a.glyphs && a.glyphs[0] && a.glyphs[0].cmds.find(c => c.type !== 'Z'); __built.push({ angleDeg: a.angleDeg, cx: a.cx, cy: a.cy, up: a.meta && a.meta.upAngle, angle: a.meta && a.meta.angle, sizePt: a.meta && a.meta.sizePt, first: first && [first.x, first.y], n: a.glyphs.length, poolId: a.meta && a.meta.poolId, text: a.meta && a.meta.text }); } catch (_) {} return b0.apply(this, arguments); }; }
      window.__raf = () => new Promise(r => requestAnimationFrame(() => requestAnimationFrame(() => setTimeout(r, 30))));
      // everything that could leave the page
      window.__sink = { req: 0, calls: [] };
      const POLL = /"op":"(getOrderPieces|cancelList)"/, isPoll = a => { try { return POLL.test(String((a[1] && a[1].body) || (a[0] && a[0].body) || '')); } catch (_) { return false; } };
      const f0 = window.fetch; window.fetch = function () { if (!isPoll(arguments)) __sink.req++; return f0.apply(this, arguments); };
      const x0 = XMLHttpRequest.prototype.open; XMLHttpRequest.prototype.open = function () { __sink.req++; return x0.apply(this, arguments); };
      if (window.DesignLink && DesignLink.call) { const c0 = DesignLink.call; DesignLink.call = function (n) { __sink.calls.push(String(n)); return c0.apply(this, arguments); }; }
      if (window.CN && CN.api) { const a0 = CN.api; CN.api = function (n, body) { if (!POLL.test(JSON.stringify(body || {}))) __sink.calls.push('api:' + String(n) + ':' + String(body && body.op || '')); return a0.apply(this, arguments); }; }
    });
    const near = (a, b, e) => Math.abs(a - b) <= e;
    const shape = key => page.evaluate(k => __shape(k), key);
    const hausdorff = (A, B) => { const d = (P, Q) => Math.max(...P.map(p => Math.min(...Q.map(q => Math.hypot(p[0] - q[0], p[1] - q[1]))))); return Math.max(d(A, B), d(B, A)); };
    const rotated = (pts, cx, cy, deg) => page.evaluate(({ pts, cx, cy, deg }) => pts.map(p => __rot(p, cx, cy, deg)), { pts, cx, cy, deg });
    const box = () => page.$('#egQueue .placeView > .ctl input[data-a="turn"]');
    const val = () => page.evaluate(() => __box() && __box().value);
    const typeTurn = async text => { const b = await box(); await b.click({ clickCount: 3 }); await page.keyboard.press('Backspace'); if (text) await page.keyboard.type(text, { delay: 15 }); };

    /* ═══ A · the row: the quick buttons are gone, one degree box ═══ */
    await R.openEngraveDetail(w, 'back', { face: 'back' });
    const row = await page.evaluate(() => {
      const ctl = document.querySelector('#egQueue .placeView > .ctl'); const t = ctl.textContent.replace(/\s+/g, ' ');
      return { exists: !!ctl, turnLeft: !!ctl.querySelector('[data-a="turnLeft"]'), turnRight: !!ctl.querySelector('[data-a="turnRight"]'), plus90: /\+\s?90/.test(t), minus90: /[−-]\s?90/.test(t), oldAngle: !!ctl.querySelector('input[data-a="angle"]'),
        oldGroup: !!ctl.querySelector('.quarterTurns'), boxes: ctl.querySelectorAll('input[data-a="turn"]').length, steps: ctl.querySelectorAll('[data-a="turnStep"]').length, numberInputs: ctl.querySelectorAll('input[type=number]').length,
        deg: !!ctl.querySelector('.egTurnDeg') && getComputedStyle(ctl.querySelector('.egTurnDeg')).display !== 'none', val: ctl.querySelector('input[data-a="turn"]') ? ctl.querySelector('input[data-a="turn"]').value : null, controls: [...ctl.children].map(c => c.className || c.tagName).join(' | ') };
    });
    check(row.exists && !row.turnLeft && !row.turnRight && !row.plus90 && !row.minus90 && !row.oldGroup, 'the "+90" and "-90" quick buttons are gone from the bottom row', row.controls);
    check(row.boxes === 1 && row.numberInputs === 1 && !row.oldAngle && row.steps === 2 && row.deg, 'in their place: ONE degree box with its degree sign (and two small step buttons); the old text-angle box is not left beside it as a second degree readout');
    check(row.val === '0', 'the box starts at 0: the placement the system chose', row.val);
    await shot('before-after-A-row-1440');
    if (!row.boxes) throw new Error('this build has no degree box in the bottom row: the checks below all need it');

    /* ═══ G · the row: sizes, fit, styles ═══ */
    const sizes = {};
    for (const [W, H] of [[1024, 768], [1040, 768], [1280, 720], [1356, 768], [1440, 900], [1920, 1080]]) {
      await page.setViewportSize({ width: W, height: H });
      for (const nm of ['back', 'both']) {
        await R.openEngraveDetail(w, nm, { face: 'back' });
        const m = await page.evaluate(() => {
          const ctl = document.querySelector('#egQueue .placeView > .ctl'); if (!ctl) return null; const r = ctl.getBoundingClientRect();
          const items = [...ctl.querySelectorAll('button,select,input,.spacingControl')].filter(c => c.getBoundingClientRect().width > 0);
          const hs = items.map(c => [c.dataset.a || c.className || c.tagName, Math.round(c.getBoundingClientRect().height * 10) / 10]);
          const kids = [...ctl.children].filter(c => c.getBoundingClientRect().width > 0), rights = kids.map(c => c.getBoundingClientRect().right), host = document.querySelector('#egQueue .backHost').getBoundingClientRect();
          const lines = { size: r.height > 62 ? 2 : 1 };   // (one line is 42.5 px tall, two are about 82)
          const hOf = sel => { const e = ctl.querySelector(sel); return e && e.getBoundingClientRect().width > 0 ? Math.round(e.getBoundingClientRect().height * 100) / 100 : null; };
          const cats = { centre: hOf('[data-a="centre"]'), skip: hOf('[data-a="skip"]'), turn: hOf('.egTurn'), step: hOf('[data-a="turnStep"]'), select: hOf('select[data-a="linecount"]'), spacing: hOf('.spacingControl'), slider: hOf('.spacingControl input'), icon: hOf('.spacingIcon') };
          const fonts = [...ctl.querySelectorAll('.btn,select,.lineControl,[data-cap]')].map(c => parseFloat(getComputedStyle(c).fontSize));
          return { cats, maxFont: Math.max(...fonts), h: Math.round(r.height * 100) / 100, w: Math.round(r.width), over: ctl.scrollWidth > ctl.clientWidth + 1, beyond: Math.max(...rights) > r.right + 1, minControl: Math.min(...hs.map(x => x[1])), hs, rows: lines.size, minFont: Math.min(...fonts), overlapsWork: r.top < host.bottom - 1, pageOver: document.documentElement.scrollWidth > innerWidth + 1, turnBox: !!ctl.querySelector('input[data-a="turn"]') };
        });
        sizes[nm + W] = m;
        check(m && !m.over && !m.beyond && !m.overlapsWork && !m.pageOver, nm + ' at ' + W + 'x' + H + ': the row fits (no sideways overflow, nothing past the work area\'s edge, the work area is not covered)', JSON.stringify({ h: m && m.h, w: m && m.w, rows: m && m.rows }));
        const oldH = { centre: OLD_BTN_PX, skip: OLD_BTN_PX, turn: OLD_BTN_PX, step: OLD_BTN_PX, select: 21, spacing: 26, slider: 16, icon: 18 };   // (clean main, measured: the +90 / -90 buttons it replaces were buttons)
        const offs = m ? Object.entries(m.cats).filter(([, v]) => v != null).map(([k, v]) => [k, v, +(oldH[k] * 1.33).toFixed(2)]).filter(([, v, e]) => Math.abs(v - e) > 0.6) : [['none']];
        check(m && offs.length === 0 && Math.abs(m.minFont - 14.63) < 0.05 && Math.abs(m.maxFont - 14.63) < 0.05, nm + ' at ' + W + ': every control is exactly 1.33 x its old size (buttons 25.94 -> 34.5, select 21 -> 27.9, spacing 26 -> 34.5, its slider 16 -> 21.3, its icon 18 -> 24; type 11 -> 14.63 px)', JSON.stringify(m && m.cats) + ' type ' + (m && m.minFont) + ' ' + (m && m.maxFont) + (offs.length ? ' OFF: ' + JSON.stringify(offs) : ''));
      }
    }
    for (const k of ['back1280', 'back1356', 'back1440', 'back1920', 'both1280', 'both1356', 'both1440', 'both1920']) { const m = sizes[k]; check(m && m.rows === 1 && Math.abs(m.h - OLD_ROW_PX * 1.33) <= 0.6, k + ': the row is exactly 1.33 x the old height, on one line', 'was ' + OLD_ROW_PX + ' px, now ' + (m && m.h) + ' px = x' + (m && (m.h / OLD_ROW_PX).toFixed(3))); }
    console.log('  row heights (px), was ' + OLD_ROW_PX + ': ' + Object.entries(sizes).map(([k, v]) => k + '=' + (v && v.h) + (v && v.rows > 1 ? '(wraps to ' + v.rows + ' lines)' : '')).join('  '));
    await page.setViewportSize({ width: 1440, height: 900 }); await R.openEngraveDetail(w, 'both', { face: 'back' });
    await shot('after-row-1440x900-both');
    await page.setViewportSize({ width: 1280, height: 720 }); await R.openEngraveDetail(w, 'back', { face: 'back' });
    await shot('after-row-1280x720');
    await page.setViewportSize({ width: 1024, height: 768 }); await R.openEngraveDetail(w, 'back', { face: 'back' });
    await shot('after-row-1024x768');
    await page.setViewportSize({ width: 1440, height: 900 }); await R.openEngraveDetail(w, 'back', { face: 'back' });

    /* ═══ B · typing turns the whole piece, live ═══ */
    const sigStart = await page.evaluate(k => (window.__v00 = Engrave.items().get(k).view, __sig(k)), JK);
    const s0 = await shape(JK), c0 = await page.evaluate(k => __canvas(k), JK), onCharm0 = await page.evaluate(k => __onCharm(k, null), JK).catch(() => null);
    check(s0.turn === 0 && s0.nudged === false, 'before: turn 0, nobody has moved the words', 'up ' + s0.up + ' angleDeg ' + s0.angleDeg + ' text angle ' + s0.angle);
    watching = true; requests.length = 0;
    const sink0 = await page.evaluate(() => ({ req: __sink.req, calls: __sink.calls.length }));
    const b = await box(); await b.click({ clickCount: 3 }); await page.keyboard.press('Backspace');
    await page.keyboard.type('3', { delay: 10 }); await page.evaluate(() => __raf());
    let s = await shape(JK);
    check(s.turn === 3 && near(s.up, s0.up - 3, 1e-6), 'one typed digit: the piece is turned 3 degrees in the work area with no Enter pressed', 'turn ' + s.turn + ' up ' + s.up);
    await page.keyboard.type('7', { delay: 10 }); await page.evaluate(() => __raf());
    const s37 = await shape(JK), c37 = await page.evaluate(k => __canvas(k), JK);
    check(s37.turn === 37 && near(s37.up, s0.up - 37, 1e-6) && near(s37.angleDeg, s0.angleDeg + 37, 1e-6), '"37" typed: turn 37 (counter-clockwise), the view turned 37 degrees more', 'angleDeg ' + s0.angleDeg.toFixed(3) + ' -> ' + s37.angleDeg.toFixed(3));
    check(await val() === '37', 'the box reads what was typed', await val());
    // the charm: every point of the drawn outline is where a 37 degree turn about the charm's centre puts it (independent of backView: our own rotation of the old outline)
    const expectOutline = await rotated(s0.outline, s0.cx, s0.cy, 37);
    const hd = hausdorff(expectOutline, s37.outline);
    check(hd < 0.05, 'the drawn charm outline is the old outline turned 37 degrees about its centre', 'max distance ' + hd.toFixed(4) + ' pt over ' + s37.outline.length + ' points');
    check(near(s37.area, s0.area, s0.area * 0.002) && s37.holes === s0.holes, 'same metal: the same area and the same cut-outs and hole', s0.area.toFixed(2) + ' vs ' + s37.area.toFixed(2) + ', ' + s37.holes + ' parts');
    // the engraving: turned with it, same size, same place on the charm
    const expectCentre = await rotated([s0.centre], s0.cx, s0.cy, 37);
    check(Math.hypot(s37.centre[0] - expectCentre[0][0], s37.centre[1] - expectCentre[0][1]) <= SLACK, 'the engraving\'s centre moved with the charm (turned 37 degrees about the charm\'s centre, to within ' + SLACK + ' pt = 0.12 mm)', s0.centre.map(v => v.toFixed(2)) + ' -> ' + s37.centre.map(v => v.toFixed(2)) + ', expected ' + expectCentre[0].map(v => v.toFixed(2)));
    const dA = ((s37.angle - s0.angle - 37) % 360 + 540) % 360 - 180;
    check(near(dA, 0, 1e-3), 'the words are turned by the same 37 degrees (their angle on the charm is unchanged)', s0.angle + ' -> ' + s37.angle);
    check(near(s37.size, s0.size, 0.02) && s37.lines.join('|') === s0.lines.join('|') && near(s37.capMm, s0.capMm, 0.02), 'the engraving keeps its size and its lines', s0.size.toFixed(3) + ' pt -> ' + s37.size.toFixed(3) + ' pt, ' + s37.lines.join(' / '));
    const expectGlyphs = await rotated(s0.glyphPts, s0.cx, s0.cy, 37);
    const gd = Math.max(...expectGlyphs.map((p, i) => Math.hypot(p[0] - s37.glyphPts[i][0], p[1] - s37.glyphPts[i][1])));
    check(s37.glyphPts.length === expectGlyphs.length && gd <= SLACK, 'every letter point of the engraving is the old one turned 37 degrees with the charm (one body, to within ' + SLACK + ' pt = 0.12 mm)', 'worst point off by ' + gd.toFixed(4) + ' pt of ' + expectGlyphs.length);
    // the canvas Paul looks at
    check(c37.url !== c0.url && c37.dark > 50, 'the work area\'s canvas was redrawn and shows ink', 'dark px ' + c0.dark + ' -> ' + c37.dark);
    const relLen0 = Math.hypot(...c0.rel), relLen = Math.hypot(...c37.rel), ang0 = Math.atan2(c0.rel[1], c0.rel[0]), ang1 = Math.atan2(c37.rel[1], c37.rel[0]);
    const dAng = ((((ang0 - ang1) * 180 / Math.PI) - 37) % 360 + 540) % 360 - 180;   // (the canvas y points down: a counter-clockwise turn on screen is the opposite sense of atan2 here)
    const r0 = [c0.rel[0] / c0.k, c0.rel[1] / c0.k], r1 = [c37.rel[0] / c37.k, c37.rel[1] / c37.k], ca = Math.cos(-37 * Math.PI / 180), sa = Math.sin(-37 * Math.PI / 180);   // (in points; the canvas y points down)
    const relExp = [r0[0] * ca - r0[1] * sa, r0[0] * sa + r0[1] * ca];
    const relOff = Math.hypot(r1[0] - relExp[0], r1[1] - relExp[1]);
    check(relOff <= SLACK && near(relLen / c37.k, relLen0 / c0.k, SLACK), 'ON THE CANVAS the engraving sits where it did relative to the charm, turned 37 degrees (the work area draws the two as one body, to within ' + SLACK + ' pt)', 'offset ' + (relLen0 / c0.k).toFixed(3) + ' -> ' + (relLen / c37.k).toFixed(3) + ' pt, off the exact turn by ' + relOff.toFixed(3) + ' pt, angle error ' + dAng.toFixed(2) + ' deg');
    const sinkNow = await page.evaluate(() => ({ req: __sink.req, calls: __sink.calls.slice() }));
    check(sinkNow.req === sink0.req && sinkNow.calls.length === sink0.calls && requests.length === 0, 'H: a turn reads, writes and sends nothing (no request, no cloud call, no paid look)', JSON.stringify(sinkNow.calls) + ' ' + requests.join(' ; '));
    watching = false;
    await shot('after-turn-37-both-panels');
    // the thumbnails (left column) follow
    const thumbs = await page.evaluate(() => { const t = document.querySelector('#egQueue .engFace .engThumb'); return t ? t.innerHTML.length : 0; });
    check(thumbs > 0, 'the piece\'s thumbnail is still drawn after the turn', 'html ' + thumbs);

    /* ═══ C · Enter / blur settle it; arrows; step buttons; garbage ═══ */
    await typeTurn('370'); await page.evaluate(() => __raf());
    check((await shape(JK)).turn === 10, 'typing 370 turns the piece 10 degrees (any integer is accepted and wrapped)', 'turn ' + (await shape(JK)).turn);
    await page.keyboard.press('Enter'); await page.evaluate(() => __raf());
    check(await val() === '10', 'Enter settles it and the box shows the wrapped value, 10', await val());
    await typeTurn('-190'); await page.keyboard.press('Enter'); await page.evaluate(() => __raf());
    check(await val() === '170' && (await shape(JK)).turn === 170, '-190 settles as 170 (the box always shows -179 to 180)', await val());
    await typeTurn('180'); await page.keyboard.press('Tab'); await page.evaluate(() => __raf());
    check(await val() === '180' && (await shape(JK)).turn === 180, 'leaving the box (blur) commits 180', await val());
    await (await box()).focus(); await page.keyboard.press('ArrowUp'); await page.evaluate(() => __raf());
    check((await shape(JK)).turn === -179 && await val() === '-179', 'Up arrow steps +1 degree (180 wraps to -179)', await val());
    await page.keyboard.press('ArrowDown'); await page.keyboard.press('ArrowDown'); await page.evaluate(() => __raf());
    check((await shape(JK)).turn === 179 && await val() === '179', 'Down arrow steps -1 degree each press', await val());
    await page.keyboard.down('Shift'); await page.keyboard.press('ArrowDown'); await page.keyboard.up('Shift'); await page.evaluate(() => __raf());
    check((await shape(JK)).turn === 164, 'Shift + Down steps 15 degrees', 'turn ' + (await shape(JK)).turn);
    await typeTurn('0'); await page.keyboard.press('Enter'); await page.evaluate(() => __raf());
    const back0 = await page.evaluate(k => ({ turn: Engrave.turnOf(Engrave.items().get(k)), sig: __sig(k) }), JK);
    check(back0.turn === 0, 'typing 0 is the system\'s own placement again', 'turn ' + back0.turn);
    // garbage never loses the value
    await typeTurn('25'); await page.keyboard.press('Enter'); await page.evaluate(() => __raf());
    const before = await page.evaluate(k => __sig(k), JK);
    await (await box()).focus(); await page.keyboard.type('abc', { delay: 5 }); await page.evaluate(() => __raf());
    const afterAbc = await page.evaluate(k => __sig(k), JK);
    check(afterAbc === before && (await val()) === '25', 'letters typed in the box are ignored: the piece and the value stay at 25', await val());
    await page.evaluate(() => { const b = __box(); b.focus(); b.value = ''; b.dispatchEvent(new Event('input', { bubbles: true })); });
    await page.evaluate(() => __raf());
    check((await page.evaluate(k => __sig(k), JK)) === before, 'an emptied box changes nothing', await val());
    await page.keyboard.press('Tab'); await page.evaluate(() => __raf());
    check(await val() === '25' && (await page.evaluate(k => __sig(k), JK)) === before, 'leaving an empty box puts the value back, 25', await val());
    await page.evaluate(() => { const b = __box(); b.focus(); b.value = '12.6'; b.dispatchEvent(new Event('input', { bubbles: true })); b.dispatchEvent(new Event('change', { bubbles: true })); });
    await page.evaluate(() => __raf());
    check((await shape(JK)).turn === 13 && await val() === '13', 'a fraction is rounded to a whole degree (12.6 gives 13)', await val());
    await page.evaluate(() => { const b = __box(); b.focus(); b.value = '1e2'; b.dispatchEvent(new Event('input', { bubbles: true })); b.dispatchEvent(new Event('change', { bubbles: true })); });
    await page.evaluate(() => __raf());
    check((await shape(JK)).turn === 100, 'a number in exponent form is read as a number (1e2 = 100)', await val());
    // the step buttons
    await typeTurn('0'); await page.keyboard.press('Enter'); await page.evaluate(() => __raf());
    const stepBtns = await page.$$('#egQueue .placeView > .ctl [data-a="turnStep"]');
    await stepBtns[1].click(); await page.evaluate(() => __raf());
    check((await shape(JK)).turn === 1, 'the right step button turns the piece 1 degree counter-clockwise', 'turn ' + (await shape(JK)).turn);
    await stepBtns[0].click(); await stepBtns[0].click(); await page.evaluate(() => __raf());
    check((await shape(JK)).turn === -1, 'the left step button turns it 1 degree clockwise', 'turn ' + (await shape(JK)).turn);
    const bb = await stepBtns[1].boundingBox(); await page.mouse.move(bb.x + bb.width / 2, bb.y + bb.height / 2); await page.mouse.down(); await page.waitForTimeout(900); await page.mouse.up(); await page.evaluate(() => __raf());
    const held = (await shape(JK)).turn;
    check(held >= 5, 'a held step button keeps turning', 'turn after 0.9 s held: ' + held);
    // keyboard on a step button
    await stepBtns[1].focus(); const t1 = (await shape(JK)).turn; await page.keyboard.press('Enter'); await page.evaluate(() => __raf());
    check((await shape(JK)).turn === t1 + 1, 'a step button works from the keyboard (Enter)', 'turn ' + t1 + ' -> ' + (await shape(JK)).turn);
    // the wheel over the focused box
    await (await box()).focus(); const t2 = (await shape(JK)).turn; const bx = await (await box()).boundingBox(); await page.mouse.move(bx.x + bx.width / 2, bx.y + bx.height / 2); await page.mouse.wheel(0, -100); await page.waitForTimeout(400);
    check((await shape(JK)).turn === t2 + 1, 'the wheel over the focused box steps 1 degree', 'turn ' + t2 + ' -> ' + (await shape(JK)).turn);

    /* ═══ E0 · every angle holds, and the laser file: the cut outline and the engraving turn as one body ═══ */
    {
      await page.evaluate(([k]) => Engrave.turnTo(Engrave.items().get(k), 0), [JK]);
      const sweep = await page.evaluate(([k]) => { const j = Engrave.items().get(k), P = CharmNestEngravePosition, snap = P.snapshot(j), out = [];
        const angles = []; for (let t = -179; t <= 180; t += 7) angles.push(t); angles.push(1, 2, 45, 89, 91, 135, 179, 180);
        const flat = () => j.fit.glyphs.flatMap(g => g.cmds.filter(c => c.type !== 'Z').map(c => [c.x, c.y]));
        for (const T of angles) { P.restore(j, snap);   // (each angle starts from the same placement: Cancel's own restore)
          const v0 = j.view, c0 = [v0.cx, v0.cy], pts0 = flat(), size0 = j.fit.size;
          const r = Engrave.turnTo(j, T); const v = j.view, pts = flat();
          // (the turn is about the charm's middle, which the new view keeps: the same cx, cy for the same charm)
          const exp = pts0.map(p => __rot(p, c0[0], c0[1], T)); let worst = 0; for (let i = 0; i < pts.length; i++) worst = Math.max(worst, Math.hypot(pts[i][0] - exp[i][0], pts[i][1] - exp[i][1]));
          out.push({ T, ok: !!(r && r.ok), n: pts.length === pts0.length, worst, dsz: j.fit.size - size0, turn: Engrave.turnOf(j), cx: Math.hypot(v.cx - c0[0], v.cy - c0[1]) }); }
        P.restore(j, snap); return out; }, [JK]);
      const exact = sweep.filter(x => x.worst < 0.01), slid = sweep.filter(x => x.worst >= 0.01 && Math.abs(x.dsz) < 0.001), shrunk = sweep.filter(x => Math.abs(x.dsz) >= 0.001);
      const bad = sweep.filter(x => !x.ok || !x.n || x.turn !== x.T || x.cx > 1e-6);
      check(sweep.length >= 50 && bad.length === 0, 'a sweep of ' + sweep.length + ' angles from -179 to 180 (each from the system\'s placement): every turn succeeds and lands on the typed angle', bad.slice(0, 3).map(x => JSON.stringify(x)).join(' '));
      check(slid.every(x => x.worst <= SLACK), 'a turn that slides the words does so by at most ' + SLACK + ' pt (0.12 mm), never more', 'worst slide ' + Math.max(0, ...slid.map(x => x.worst)).toFixed(3) + ' pt');
      check(shrunk.every(x => x.dsz < 0 && x.dsz > -0.2), 'where even that does not pass the strict ink check the words are made a step smaller, never larger and never by more than 0.2 pt', shrunk.map(x => x.T + ':' + x.dsz.toFixed(3)).join(' ') || 'none');
      console.log('  sweep of ' + sweep.length + ' angles: ' + exact.length + ' exactly rigid, ' + slid.length + ' slid up to ' + Math.max(0, ...slid.map(x => x.worst)).toFixed(3) + ' pt, ' + shrunk.length + ' needed a smaller size (' + shrunk.map(x => x.T + ':' + x.dsz.toFixed(3)).join(' ') + ')');
    }
    {
      const vecs = {}; for (const T of [0, 37, -37, 90, 180]) { await page.evaluate(([k, t]) => Engrave.turnTo(Engrave.items().get(k), t), [JK, T]); vecs[T] = await page.evaluate(k => __file(k), JK); }
      const ang = v => Math.atan2(v.vec[1], v.vec[0]) * 180 / Math.PI, len = v => Math.hypot(v.vec[0], v.vec[1]), wrap = d => ((d % 360) + 540) % 360 - 180;
      const rows = Object.entries(vecs).map(([T, v]) => T + ': engraving-to-charm ' + len(v).toFixed(2) + ' pt at ' + ang(v).toFixed(1) + ' deg, page ' + v.w.toFixed(1) + 'x' + v.h.toFixed(1));
      check(Object.values(vecs).every(v => v.nEng > 100 && v.nCut > 100), 'the laser file (written the way the approval writes it) holds the engraving and the cut outline at every turn', rows.join(' | '));
      const base = vecs[0];
      check(Object.entries(vecs).every(([T, v]) => Math.abs(len(v) - len(base)) < 0.45), 'in the file the engraving keeps its distance to the charm at every turn (one rigid body)', Object.values(vecs).map(v => len(v).toFixed(2)).join(' '));
      check(Object.entries(vecs).every(([T, v]) => Math.abs(wrap(ang(v) - ang(base) - (+T))) < 1.8), 'and the direction from the charm to its engraving turns by exactly the typed angle in the file (0, 37, -37, 90, 180)', Object.entries(vecs).map(([T, v]) => T + '->' + wrap(ang(v) - ang(base)).toFixed(1)).join(' '));
      check(Math.abs(vecs[90].w - base.h) < 0.5 && Math.abs(vecs[90].h - base.w) < 0.5, 'the file\'s page turns with it (90 degrees swaps its width and height)', base.w.toFixed(1) + 'x' + base.h.toFixed(1) + ' -> ' + vecs[90].w.toFixed(1) + 'x' + vecs[90].h.toFixed(1));
      await page.evaluate(([k]) => Engrave.turnTo(Engrave.items().get(k), 0), [JK]);
    }

    /* ═══ D · Cancel restores the system's placement exactly; Save keeps the turn and asks why; Approve works ═══ */
    await R.openEngraveDetail(w, 'back', { face: 'back' });
    // everything staged so far (all of section C) goes back with ONE Cancel, to the system's own placement, exactly
    const cancelWas = await page.evaluate(() => document.querySelector('#egQueue [data-engp="cancel"]').disabled);
    if (!cancelWas) { await page.click('#egQueue [data-engp="cancel"]'); await page.evaluate(() => __raf()); await page.waitForTimeout(150); }
    const afterC = await page.evaluate(k => ({ sig: __sig(k), same: Engrave.items().get(k).view === window.__v00, turn: Engrave.turnOf(Engrave.items().get(k)) }), JK);
    check(afterC.sig === sigStart && afterC.same, 'one Cancel after dozens of turns puts the system\'s own placement back, exactly (same view object, same engraving point for point)', 'cancel was ' + (cancelWas ? 'OFF' : 'on') + ', turn ' + afterC.turn + ', same view ' + afterC.same + (afterC.sig === sigStart ? '' : ', signature differs: ' + afterC.sig.slice(0, 150) + ' vs ' + sigStart.slice(0, 150)));
    const base = await page.evaluate(k => ({ sig: __sig(k), view: (window.__v0 = Engrave.items().get(k).view, true), url: __canvas(k).url, saveOff: document.querySelector('#egQueue [data-engp="save"]').disabled, cancelOff: document.querySelector('#egQueue [data-engp="cancel"]').disabled }), JK);
    check(base.saveOff && base.cancelOff, 'before any change Save and Cancel are off', 'save off ' + base.saveOff + ', cancel off ' + base.cancelOff);
    await typeTurn('90'); await page.evaluate(() => __raf());
    const staged = await page.evaluate(() => ({ saveOff: document.querySelector('#egQueue [data-engp="save"]').disabled, cancelOff: document.querySelector('#egQueue [data-engp="cancel"]').disabled, approve: [...document.querySelectorAll('#egQueue [data-a="approve"]')].map(b => b.disabled) }));
    check(!staged.saveOff && !staged.cancelOff, 'after a turn the panel\'s Save and Cancel come on (a turn is a staged position change, like the Position buttons)', JSON.stringify(staged));
    check(staged.approve.length === 1 && staged.approve[0] === false, 'Approve is not blocked by a turn (as for a Position move; only staged words, font or mode block it)', JSON.stringify(staged.approve));
    await shot('after-turn-90');
    await page.click('#egQueue [data-engp="cancel"]'); await page.evaluate(() => __raf()); await page.waitForTimeout(150);
    const cancelled = await page.evaluate(k => ({ sig: __sig(k), same: Engrave.items().get(k).view === window.__v0, url: __canvas(k).url, val: __box().value, turn: Engrave.turnOf(Engrave.items().get(k)) }), JK);
    check(cancelled.sig === base.sig && cancelled.same, 'Cancel puts the system\'s placement back exactly: the same view, the same engraving, point for point', 'turn ' + cancelled.turn);
    check(cancelled.url === base.url, 'and the work area draws the very same picture again (byte for byte)');
    check(cancelled.val === '0', 'the box reads 0 again', cancelled.val);
    // Save: the reason prompt, then the turn stays
    await typeTurn('90'); await page.keyboard.press('Enter'); await page.evaluate(() => __raf());
    const turned90 = await page.evaluate(k => ({ sig: __sig(k), url: __canvas(k).url }), JK);
    await page.click('#egQueue [data-engp="save"]'); await page.waitForSelector('#egQueue .engpWhy', { timeout: 5000 }).catch(() => {});
    const asked = await page.evaluate(() => !!document.querySelector('#egQueue .engpWhy'));
    check(asked, 'Save of a turn asks "Why change it?" like any override of the system\'s choice (the existing reason path)');
    await page.click('#egQueue .engpChip[data-code="asked"]'); await page.waitForTimeout(500); await page.evaluate(() => __raf());
    const saved = await page.evaluate(k => ({ sig: __sig(k), turn: Engrave.turnOf(Engrave.items().get(k)), why: document.querySelector('#egQueue .engpWhyLine') ? document.querySelector('#egQueue .engpWhyLine').textContent : '', saveOff: document.querySelector('#egQueue [data-engp="save"]').disabled, val: __box() && __box().value }), JK);
    check(saved.sig === turned90.sig && saved.turn === 90, 'after Save the piece is still turned 90 and nothing else moved', 'turn ' + saved.turn);
    check(saved.saveOff && /Customer asked/.test(saved.why), 'Save is off again and the line says the reason was recorded', saved.why);
    // a new turn after the Save has its own Cancel back to the saved one
    await typeTurn('120'); await page.keyboard.press('Enter'); await page.evaluate(() => __raf());
    await page.click('#egQueue [data-engp="cancel"]'); await page.evaluate(() => __raf()); await page.waitForTimeout(100);
    check((await page.evaluate(k => __sig(k), JK)) === turned90.sig && await val() === '90', 'Cancel after a Save goes back to the saved turn (90), not the system\'s', await val());

    /* ═══ F · a later fit keeps the person's orientation ═══ */
    const preUp = (await shape(JK)).up;
    await page.fill('#egQueue .engpWords', 'Name Two'); await page.waitForTimeout(200);
    check((await shape(JK)).up === preUp, 'staging new words does not turn the piece back (the staged words are drawn on the turned piece)', 'up ' + (await shape(JK)).up);
    await page.click('#egQueue [data-engp="save"]'); await page.waitForSelector('#egQueue .engpWhy', { timeout: 5000 }).catch(() => {});
    if (await page.$('#egQueue .engpChip[data-code="typo"]')) await page.click('#egQueue .engpChip[data-code="typo"]');
    await page.waitForFunction(k => { const j = Engrave.items().get(k); return j && j.state === 'review' && j.fit && j.text === 'Name Two'; }, JK, { timeout: 30000 }).catch(() => {});
    await page.waitForTimeout(600); await page.evaluate(() => __raf());
    const refit = await page.evaluate(k => { const j = Engrave.items().get(k); return { text: j.text, up: j.view.upAngle, turn: Engrave.turnOf(j), turnUp: j.turnUp, ok: !!(j.fit && j.fit.ok !== false), state: j.state }; }, JK);
    check(refit.text === 'Name Two' && refit.state === 'review' && near(refit.up, preUp, 1e-6) && refit.turn === 90, 'new words are fitted afresh ON the turned piece (the orientation is the person\'s, kept by every later fit)', JSON.stringify(refit));

    /* ═══ E · Approve after a turn: the file and the record read the turned orientation, the engraving sits on the same metal ═══ */
    await R.openEngraveDetail(w, 'both', { face: 'back' });
    const T0 = await shape(TK), onCharm0b = await page.evaluate(k => __onCharm(k, 1), TK).catch(() => null);
    await typeTurn('-65'); await page.keyboard.press('Enter'); await page.evaluate(() => __raf());
    const T1 = await shape(TK);
    check(T1.turn === -65 && T1.lines.length >= 1, 'the two-line job turns too (-65)', 'lines ' + T1.lines.join(' / ') + ', turn ' + T1.turn);
    await shot('after-turn-both-lines');
    // the engraving on the charm's own frame (what the sheet views draw): the same place before and after
    const eng = async () => page.evaluate(k => { const j = Engrave.items().get(k); const charm = Pool.charmOf(j.copies[0]);
      const back = { poolId: j.copies[0], upAngle: j.view.upAngle, centre: j.fit.centre, angle: j.fit.angle || 0, sizePt: j.fit.size, capMm: j.fit.capMm, lines: j.lines, lineGap: j.lineGap == null ? null : j.lineGap, approvedAt: 1, face: j.face === 'front' ? 'front' : undefined };
      const e = CharmNestBacks.engraveOn(back, charm, { font: Engrave.fontForWeight(j.fit.weight, j) });
      const pts = []; for (const g of (e.glyphs || [])) for (const c of g.cmds) if (c.type !== 'Z') pts.push([c.x, c.y]);
      return { centre: e.centre, mid: [pts.reduce((a, p) => a + p[0], 0) / pts.length, pts.reduce((a, p) => a + p[1], 0) / pts.length], n: pts.length }; }, TK);
    const onNow = await eng();
    // the same job at turn 0 (typed 0) gives the reference
    await typeTurn('0'); await page.keyboard.press('Enter'); await page.evaluate(() => __raf());
    const onRef = await eng();
    check(onRef.n > 0 && near(onNow.mid[0], onRef.mid[0], 0.05) && near(onNow.mid[1], onRef.mid[1], 0.05) && near(onNow.centre[0], onRef.centre[0], 0.05) && near(onNow.centre[1], onRef.centre[1], 0.05),
      'the engraving sits on the SAME metal of the charm after the turn (charm-nest-backs.js engraveOn, the sheet views\' own mapping, gives the same place at -65 as at 0)', 'at 0: ' + onRef.mid.map(v => v.toFixed(3)) + '  at -65: ' + onNow.mid.map(v => v.toFixed(3)));
    await typeTurn('-65'); await page.keyboard.press('Enter'); await page.evaluate(() => __raf());
    // Approve with the real pressed button (the fake site's in-memory Firestore and Storage; nothing live)
    const turnedView = await page.evaluate(k => { const j = Engrave.items().get(k); const f = j.fit.glyphs[0].cmds.find(c => c.type !== 'Z'); return { up: j.view.upAngle, angleDeg: j.view.angleDeg, centre: j.fit.centre.slice(), angle: j.fit.angle || 0, lines: j.lines.slice(), first: [f.x, f.y] }; }, TK);
    const appr = await page.$('#egQueue [data-a="approve"]');
    check(appr && !(await appr.isDisabled()), 'Approve is enabled on the turned piece');
    await appr.click();
    await page.waitForFunction(k => { const j = Engrave.items().get(k); return j && j.state === 'approved'; }, TK, { timeout: 40000 }).catch(() => {});
    await page.waitForTimeout(2500);
    const done = await page.evaluate(async k => { const j = Engrave.items().get(k); return { state: j.state, approvedBy: j.approvedBy, built: __built.filter(b => b.poolId === j.copies[0]).pop() || null }; }, TK);
    check(done.state === 'approved', 'Approve works after a turn (the real pressed button)', done.state);
    const bt = done.built;
    check(!!bt && near(bt.angleDeg, turnedView.angleDeg, 1e-6) && near(bt.up, turnedView.up, 1e-6), 'the file writer is handed the TURNED orientation (page angle ' + (bt && bt.angleDeg.toFixed(3)) + ' = the work area\'s ' + turnedView.angleDeg.toFixed(3) + '; upAngle ' + (bt && bt.up.toFixed(3)) + ', the system\'s ' + (turnedView.up + 65).toFixed(3) + ' minus the 65 turn)', JSON.stringify(bt));
    check(!!bt && near(bt.angle, turnedView.angle, 1e-3) && near(bt.first[0] + bt.cx, turnedView.first[0], 0.01) && near(bt.first[1] + bt.cy, turnedView.first[1], 0.01), 'and the engraving as the work area showed it: its angle and its first letter point (relative to the charm\'s centre) are the ones on screen');
    // NODECIDED (Paul, 16 Oct 2026): an approved piece is not locked: it turns like any other, the FIRST turn puts it back to "needs Approve" (its seal stays), Approve again adds a second seal
    const seals0 = await page.evaluate(k => CNEngravingSeals.list(Engrave.items().get(k)).length, TK);
    const turned = await page.evaluate(async k => { const j = Engrave.items().get(k), was = { state: j.state, up: j.view.upAngle }; const r = await Engrave.turnTo(j, 10); return { ok: !!(r && r.ok), was: was.state, state: j.state, up: j.view.upAngle, was_up: was.up, seals: CNEngravingSeals.list(j).length, reason: j.reason || null }; }, TK);
    check(turned.was === 'approved' && turned.ok && turned.state === 'review' && turned.seals === seals0 && turned.up !== turned.was_up, 'an approved piece can be turned: the call is accepted, the piece is back to needs-Approve and its seal is kept', JSON.stringify(turned));
    await R.openEngraveDetail(w, 'both', { face: 'back' });
    const back2 = await page.evaluate(() => { const c = document.querySelector('#egQueue .rvItem[data-kind=placement]'); return { state: c && c.dataset.state, approve: !!(c && c.querySelector('[data-a="approve"]:not([disabled])')), tick: !!(c && c.querySelector('.egApproved[data-a="approved"]')) }; });
    check(back2.state === 'review' && back2.approve && !back2.tick, 'and its card asks for the Approve again (the tick is gone)', JSON.stringify(back2));
    const appr2 = await page.$('#egQueue [data-a="approve"]');
    await appr2.click();
    await page.waitForFunction(k => { const j = Engrave.items().get(k); return j && j.state === 'approved'; }, TK, { timeout: 40000 }).catch(() => {});
    await page.waitForTimeout(2500);
    const again = await page.evaluate(k => { const j = Engrave.items().get(k); return { state: j.state, seals: CNEngravingSeals.list(j).length }; }, TK);
    check(again.state === 'approved' && again.seals === seals0 + 1, 'Approve again: approved with a SECOND brand new seal, the first one stays', JSON.stringify(again));

    // the feedback styles
    const fb = async () => {
      const st = {};
      const cen = await page.$('#egQueue .placeView > .ctl [data-a="centre"]'), sel = await page.$('#egQueue .placeView > .ctl select'), step = (await page.$$('#egQueue .placeView > .ctl [data-a="turnStep"]'))[1], skip = await page.$('#egQueue .placeView > .ctl [data-a="skip"]');
      const css = (el, p) => el.evaluate((n, p) => getComputedStyle(n)[p], p);
      st.restBg = await css(cen, 'backgroundColor');
      let bx = await cen.boundingBox(); await page.mouse.move(bx.x + bx.width / 2, bx.y + bx.height / 2); await page.waitForTimeout(250);
      st.hoverBg = await css(cen, 'backgroundColor'); st.hoverBorder = await css(cen, 'borderColor');
      await page.mouse.down(); await page.waitForTimeout(120);
      st.pressBg = await css(cen, 'backgroundColor'); st.pressTf = await css(cen, 'transform'); st.pressShadow = await css(cen, 'boxShadow');
      await page.mouse.move(0, 0); await page.mouse.up(); await page.waitForTimeout(200);
      // keyboard focus ring: tab onto the select and the step button
      await sel.focus(); await page.keyboard.press('Shift+Tab'); await page.keyboard.press('Tab'); await page.waitForTimeout(100);
      st.selOutline = await css(sel, 'outlineStyle') + ' ' + await css(sel, 'outlineWidth');
      await step.focus(); await page.keyboard.press('Shift+Tab'); await page.keyboard.press('Tab'); await page.waitForTimeout(100);
      st.stepOutline = await css(step, 'outlineStyle') + ' ' + await css(step, 'outlineWidth');
      await cen.focus(); await page.keyboard.press('Shift+Tab'); await page.keyboard.press('Tab'); await page.waitForTimeout(100);
      st.cenOutline = await css(cen, 'outlineStyle') + ' ' + await css(cen, 'outlineWidth');
      st.boxRing = await page.evaluate(() => { const bx = __box(); bx.focus(); return getComputedStyle(bx.closest('.egTurn')).boxShadow; });
      st.disabledOpacity = await page.evaluate(async () => { const b = document.querySelector('#egQueue .placeView > .ctl [data-a="centre"]'); b.disabled = true; await new Promise(r => setTimeout(r, 300)); const o = getComputedStyle(b).opacity; b.disabled = false; return o; });
      st.stepDisabled = await page.evaluate(async () => { const b = document.querySelector('#egQueue .placeView > .ctl [data-a="turnStep"]'); b.disabled = true; await new Promise(r => setTimeout(r, 300)); const o = getComputedStyle(b).opacity; b.disabled = false; return o; });
      st.skipH = Math.round((await skip.boundingBox()).height);
      return st;
    };
    const f = await fb();
    check(f.hoverBg !== f.restBg, 'hover: a button changes colour under the pointer', f.restBg + ' -> ' + f.hoverBg);
    check(f.pressBg !== f.hoverBg && (f.pressTf !== 'none' || /inset/.test(f.pressShadow)), 'pressed: a button looks pressed (darker, nudged down, inset shadow)', f.pressBg + ' / ' + f.pressTf + ' / ' + f.pressShadow);
    check(/solid 3px/.test(f.selOutline) && /solid 3px/.test(f.stepOutline) && /solid 3px/.test(f.cenOutline), 'keyboard focus: a 3 px ring on the buttons, the select and the step buttons (focus-visible)', f.cenOutline + ' | ' + f.selOutline + ' | ' + f.stepOutline);
    check(/rgba?\(169, 130, 63/.test(f.boxRing), 'the degree box shows a focus glow while it is being typed in', f.boxRing);
    check(parseFloat(f.disabledOpacity) <= 0.5 && parseFloat(f.stepDisabled) <= 0.5, 'disabled: dimmed with a "not allowed" cursor', f.disabledOpacity + ' / ' + f.stepDisabled);
    // "applied" feedback after a commit
    await (await box()).click(); await page.keyboard.type('5'); await page.keyboard.press('Enter'); await page.waitForTimeout(80);
    check(await page.evaluate(() => __box().closest('.egTurn').classList.contains('applied')), 'applied: the box glows green for a moment after a value is settled');
    await page.waitForTimeout(900);
    check(!(await page.evaluate(() => __box().closest('.egTurn').classList.contains('applied'))), 'and the glow goes again');

    check(w.errors.length === 0, 'no page errors', JSON.stringify(w.errors).slice(0, 300));
  } catch (e) { fails++; console.log('  FAIL threw: ' + (e && e.stack || e)); }
  finally { await w.close(); }
  console.log(fails ? 'engrave-turn: ' + fails + ' check(s) FAILED' : 'engrave-turn: all checks passed');
  process.exit(fails ? 1 : 0);
})();
