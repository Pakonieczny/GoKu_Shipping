// ENGTHUMB (engrave-ui-1012 round 2, Paul's points 3 and 4, second half): "in each box I would see the current engraving and one would be front facing and one would be back facing so that I would see
// the charm front and back and the front is gonna have to show all of the engraving/cutouts for each given charm". The two boxes of a card are real pictures of the CHARM seen from each side, in the order
// window's card AND in the Engrave tab's detail. REAL charms: tests/charm-nest/_engraveRealWorld.cjs (the real .ai drawings, the real intake, pool, classifier and fit; offline; nothing live, no paid call).
//   A  the order window's card, real charms: both boxes hold a CANVAS of the charm (outline, cut-outs), the front also the design's engraved art (red / blue strokes), the back none; the words are
//      real text in the chosen font; the mini tag shows its hoop on the OPPOSITE side on the back (the charm turned over) and each box agrees with the job's own view; three discs, three pictures
//   B  the words land where the cut puts them: the text's centre on the thumbnail is the centre of the job's fitted glyphs (checked against the charm's own flattened bounds measured on the
//      canvas's pixels), its size the fit's size, its turn the fit's turn (CSS rotate(-angle), the way the work area shows it)
//   C  the same job without its geometry: a piece with no job yet is drawn from the pool's charm turned as the fit turns it (same frame as the fitted view); a WRITTEN piece (it let go of its view and
//      keeps writtenFit) lands the words on the same spot as before
//   D  the Engrave tab's detail: typing, a font pick, a mode change and a position nudge change the thumbnails in the SAME TICK, the charm is not drawn again (same canvas element), no request is made
//   E  with no geometry at all the order window's own Vector design is the picture (turned over for the back), with a small labelled wait while it loads; the front and back of a charm differ
//   PW_DIR=/opt/node22/lib/node_modules/playwright/node_modules node tests/charm-nest/engrave-thumbs.cjs      (THUMB_SHOTS=<dir> also writes screenshots; one world per run)
'use strict';
const assert = require('assert/strict'), fs = require('fs'), path = require('path');
const R = require('./_engraveRealWorld.cjs');
let n = 0;
const ok = (msg, cond) => { assert.ok(cond, msg); n++; };
const near = (msg, a, b, tol) => { assert.ok(Math.abs(a - b) <= tol, `${msg}: ${a} vs ${b} (tolerance ${tol})`); n++; };

/** installed in the page: what a thumbnail's canvas and words really are */
function install() {
  const alpha = cv => { const c = cv.getContext('2d'); return { d: c.getImageData(0, 0, cv.width, cv.height).data, w: cv.width, h: cv.height }; };
  window.__T = {
    /** ink statistics of a canvas: opaque pixels, red art, blue art, black line pixels, the opaque bounds, and the centroid (x, y as a fraction of width / height) of the enclosed transparent holes */
    pix(cv) {
      const { d, w, h } = alpha(cv); let a = 0, red = 0, blue = 0, dark = 0, x0 = w, y0 = h, x1 = -1, y1 = -1;
      for (let y = 0; y < h; y++) for (let x = 0; x < w; x++) {
        const i = (y * w + x) * 4, A = d[i + 3]; if (A < 16) continue; a++; if (x < x0) x0 = x; if (x > x1) x1 = x; if (y < y0) y0 = y; if (y > y1) y1 = y;
        const r = d[i], g = d[i + 1], b = d[i + 2];
        if (r > 170 && g < 120 && b < 120) red++; if (b > 170 && r < 100 && g < 140) blue++; if (r + g + b < 150) dark++;
      }
      // enclosed transparent pixels: flood the transparent area from the border; what is left is a hole of the charm
      const seen = new Uint8Array(w * h), st = [];
      const tr = (x, y) => d[(y * w + x) * 4 + 3] < 16;
      for (let x = 0; x < w; x++) for (const y of [0, h - 1]) if (tr(x, y) && !seen[y * w + x]) { seen[y * w + x] = 1; st.push([x, y]); }
      for (let y = 0; y < h; y++) for (const x of [0, w - 1]) if (tr(x, y) && !seen[y * w + x]) { seen[y * w + x] = 1; st.push([x, y]); }
      while (st.length) { const [x, y] = st.pop(); for (const [dx, dy] of [[1, 0], [-1, 0], [0, 1], [0, -1]]) { const X = x + dx, Y = y + dy; if (X < 0 || Y < 0 || X >= w || Y >= h || seen[Y * w + X] || !tr(X, Y)) continue; seen[Y * w + X] = 1; st.push([X, Y]); } }
      let hn = 0, hx = 0, hy = 0; for (let y = 0; y < h; y++) for (let x = 0; x < w; x++) if (tr(x, y) && !seen[y * w + x]) { hn++; hx += x; hy += y; }
      return { w, h, a, red, blue, dark, box: [x0, y0, x1, y1], holes: hn, holeX: hn ? hx / hn / w : null, holeY: hn ? hy / hn / h : null };
    },
    /** one face box of a card, read from the page */
    face(scope, face) {
      const f = (scope || document).querySelector(`.engFace[data-face="${face}"]`); if (!f) return null;
      const th = f.querySelector('.engThumb'), cv = th && th.querySelector('.engpvDraw canvas'), t = th && th.querySelector('.engpvText'), pv = th && th.querySelector('.engpv');
      return { has: true, canvas: !!cv, px: cv ? __T.pix(cv) : null, text: t ? t.textContent : null, family: t ? getComputedStyle(t).fontFamily : '', vis: t ? getComputedStyle(t).visibility : '', style: t ? t.getAttribute('style') : '', pic: pv ? pv.dataset.pic : '', flip: pv ? pv.getAttribute('data-flip') : null, state: pv ? pv.dataset.state : '', box: th ? th.getBoundingClientRect().toJSON() : null };
    },
    /** where a job's fitted letters really are, on the canvas of its face box, measured independently: the charm's flattened bounds (true geometry) against the opaque bounds on the canvas */
    landing(thumb, job) {
      const G = CharmNestGeom, v = job.view, cv = thumb.querySelector('.engpvDraw canvas'), t = thumb.querySelector('.engpvText'); if (!v || !cv || !t || !job.fit) return null;
      const px = __T.pix(cv); let tb = [Infinity, Infinity, -Infinity, -Infinity];
      for (const m of v.members) for (const poly of G.flatten(m, 16)) for (const p of poly) { tb = [Math.min(tb[0], p[0]), Math.min(tb[1], p[1]), Math.max(tb[2], p[0]), Math.max(tb[3], p[1])]; }
      const sx = (px.box[2] - px.box[0] + 1) / (tb[2] - tb[0]), sy = (px.box[3] - px.box[1] + 1) / (tb[3] - tb[1]);
      let gb = [Infinity, Infinity, -Infinity, -Infinity]; for (const c of job.fit.cmds) { if (c.type === 'Z') continue; gb = [Math.min(gb[0], c.x), Math.min(gb[1], c.y), Math.max(gb[2], c.x), Math.max(gb[3], c.y)]; }
      const gcx = (gb[0] + gb[2]) / 2, gcy = (gb[1] + gb[3]) / 2;
      const want = [(gcx - tb[0]) * sx + px.box[0], (tb[3] - gcy) * sy + px.box[1]];
      const cr = cv.getBoundingClientRect(), tr = t.getBoundingClientRect(), k = cv.width / cr.width;
      const got = [((tr.left + tr.right) / 2 - cr.left) * k, ((tr.top + tr.bottom) / 2 - cr.top) * k];
      const m = new DOMMatrix(getComputedStyle(t).transform), turn = Math.atan2(m.b, m.a) * 180 / Math.PI;
      return { want, got, scaleRatio: sx / sy, W: cv.width, H: cv.height, textW: tr.width * k, glyphW: (gb[2] - gb[0]) * sx, turn, angle: job.fit.angle || 0, size: job.fit.size, em: parseFloat(getComputedStyle(t).fontSize) * k };
    }
  };
}

(async () => {
  const shots = process.env.THUMB_SHOTS || '';
  const w = await R.world({ viewport: { width: 1440, height: 900 }, shots: shots || undefined, scale: shots ? 2 : 1 });
  if (!w) { console.log('engrave-thumbs: no playwright-core here, not run'); return; }
  const page = w.page; const requests = [];
  try {
    const made = await R.build(w, ['both', 'tag', 'discs', 'back']);
    await page.evaluate(install);
    // (the world's own fit passes can end before the last job is fitted on a busy machine: finish them here)
    await page.evaluate(async () => { for (let i = 0; i < 160; i++) { const js = [...Engrave.items().values()]; if (js.length && !js.some(j => ['classify', 'ready', 'fitting'].includes(j.state))) break; try { await Engrave.fitAll(); } catch (_) { /* again */ } await new Promise(r => setTimeout(r, 250)); } });
    for (const k of ['both', 'tag', 'discs', 'back']) made[k].jobs = await page.evaluate(key => Engrave.jobsOf(B.orders.byKey.get(key)).map(j => ({ key: j.key, state: j.state, reason: j.reason, hasView: !!j.view, fitted: !!j.fit && !!j.fit.cmds })), made[k].key);
    ok('the real world made jobs with real views and fits: ' + JSON.stringify(['both', 'tag', 'discs', 'back'].map(k => made[k].jobs.filter(j => !(j.hasView && j.fitted)))), ['both', 'tag', 'discs', 'back'].every(k => made[k].jobs.length && made[k].jobs.every(j => j.hasView && j.fitted)));
    const settleFonts = () => page.evaluate(async () => { const P = CharmNestEngravePreview; await Promise.all(['jost-ace', 'source-sans-3', 'playwrite-us-trad'].map(id => P.ensure(id))); await new Promise(r => setTimeout(r, 50)); [...document.querySelectorAll('.engThumb')].forEach(b => { if (b._engpv) CharmNestEngravePreview.update(b, {}); }); });
    const card = (sel, i = 0) => `[...document.querySelectorAll('#owEng .engCard')][${i}]`;

    /* ── A · the order window's card ── */
    await R.openOrderWindow(w, 'both'); await settleFonts();
    let f = await page.evaluate(() => ({ front: __T.face(document.querySelector('#owEng .engCard'), 'front'), back: __T.face(document.querySelector('#owEng .engCard'), 'back') }));
    ok('both: the front box is a canvas of the charm, not a bare square', f.front.canvas && f.front.px.a > 3000 && f.front.px.dark > 200);
    ok('both: the back box is a canvas of the charm too', f.back.canvas && f.back.px.a > 3000 && f.back.px.dark > 200);
    ok('the FRONT shows the design\'s engraved art (red strokes of the skull)', f.front.px.red > 100);
    ok('the BACK shows the metal only: no art', f.back.px.red === 0 && f.back.px.blue === 0);
    ok('the front has cut-outs (the hoop) drawn', f.front.px.holes > 30 && f.back.px.holes > 30);
    ok('the front words are real text in the chosen font (Ace, capitals)', f.front.text === 'W' && /CNEng jost-ace/.test(f.front.family) && f.front.vis === 'visible');
    ok('the back words are real text in the chosen font', f.back.text.replace(/\s+/g, ' ') === 'NAME ONE' && /CNEng jost-ace/.test(f.back.family));
    ok('the picture is the charm itself: no flip applied to a drawing that is already turned', f.back.flip === null && f.back.pic === 'frame');
    if (shots) { const clip = await page.evaluate(() => { const r = document.getElementById('owEng').getBoundingClientRect(); return { x: Math.max(0, r.x - 6), y: r.y - 6, width: Math.min(r.width + 12, 760), height: r.height + 12 }; }); await R.shot(w, 'THUMB-order-both', { clip }); }

    await R.openOrderWindow(w, 'tag'); await settleFonts();
    f = await page.evaluate(key => {
      const c = document.querySelector('#owEng .engCard'), out = { front: __T.face(c, 'front'), back: __T.face(c, 'back'), sides: {} };
      for (const j of Engrave.jobsOf(B.orders.byKey.get(key))) { const v = j.view, ob = v.outline.bbox, h = v.members.find(m => m !== v.outline); out.sides[j.face === 'front' ? 'front' : 'back'] = Math.sign(((h.bbox[0] + h.bbox[2]) / 2) - ((ob[0] + ob[2]) / 2)); }
      return out;
    }, made.tag.key);
    ok('tag: both boxes are charm pictures with their hoop', f.front.canvas && f.back.canvas && f.front.px.holes > 10 && f.back.px.holes > 10);
    ok('tag: the job\'s own views have the hoop on opposite sides (the charm turned over)', f.sides.front !== 0 && f.sides.front === -f.sides.back);
    ok('tag: each box agrees with its job\'s view (front hoop ' + f.sides.front + ', back ' + f.sides.back + ')', Math.sign(f.front.px.holeX - 0.5) === f.sides.front && Math.sign(f.back.px.holeX - 0.5) === f.sides.back);
    ok('tag: the words are the chosen ones in the chosen font', f.front.text === 'W' && f.back.text === '1. 27. 24' && /jost-ace/.test(f.back.family));
    if (shots) { const clip = await page.evaluate(() => { const r = document.getElementById('owEng').getBoundingClientRect(); return { x: Math.max(0, r.x - 6), y: r.y - 6, width: Math.min(r.width + 12, 760), height: r.height + 12 }; }); await R.shot(w, 'THUMB-order-tag', { clip }); }

    await R.openOrderWindow(w, 'discs'); await settleFonts();
    f = await page.evaluate(() => [...document.querySelectorAll('#owEng .engCard')].map(c => ({ piece: c.dataset.piece, back: __T.face(c, 'back') })));
    ok('discs: three cards, each with its own picture and its own letter', f.length === 3 && f.every(x => x.back.canvas && x.back.px.a > 1500) && f.map(x => x.back.text).join('') === 'ABC');
    if (shots) { const clip = await page.evaluate(() => { const r = document.getElementById('owEng').getBoundingClientRect(); return { x: Math.max(0, r.x - 6), y: r.y - 6, width: Math.min(r.width + 12, 1000), height: r.height + 12 }; }); await R.shot(w, 'THUMB-order-discs', { clip }); }

    await R.openOrderWindow(w, 'back'); await settleFonts();
    f = await page.evaluate(() => ({ back: __T.face(document.querySelector('#owEng .engCard'), 'back') }));
    ok('back only (comet): the back is the charm turned over with no art; the words in Stylish', f.back.canvas && f.back.px.blue === 0 && f.back.px.red === 0 && f.back.text === 'Name One' && /playwrite-us-trad/.test(f.back.family));
    if (shots) { const clip = await page.evaluate(() => { const r = document.getElementById('owEng').getBoundingClientRect(); return { x: Math.max(0, r.x - 6), y: r.y - 6, width: Math.min(r.width + 12, 760), height: r.height + 12 }; }); await R.shot(w, 'THUMB-order-back', { clip }); }

    /* ── B · the words land where the cut puts them ── */
    for (const name of ['both', 'tag', 'discs', 'back']) {
      await R.openOrderWindow(w, name); await settleFonts();
      const rows = await page.evaluate(key => { const out = []; for (const j of Engrave.jobsOf(B.orders.byKey.get(key))) { const card = [...document.querySelectorAll('#owEng .engCard')].find(c => c.dataset.piece === CharmNestEngraveFaces.pieceKeyOf(j.key)); const th = card && card.querySelector(`.engFace[data-face="${j.face === 'front' ? 'front' : 'back'}"] .engThumb`); const L = th && __T.landing(th, j); out.push({ key: j.key, face: j.face === 'front' ? 'front' : 'back', L }); } return out; }, made[name].key);
      ok(`${name}: every face job has a thumbnail to measure`, rows.length > 0 && rows.every(r => r.L));
      for (const r of rows) {
        const L = r.L, why = `${name} ${r.face} ${r.key}`;
        near(`${why}: the canvas keeps the charm's shape (x and y scale agree)`, L.scaleRatio, 1, 0.04);
        near(`${why}: the words are centred on the fitted letters (x)`, L.got[0], L.want[0], 0.07 * L.W);
        near(`${why}: the words are centred on the fitted letters (y)`, L.got[1], L.want[1], 0.07 * L.H);
        const turn = ((L.turn + L.angle + 540) % 360) - 180; near(`${why}: the words turn as the fit turns them (CSS rotate(-angle))`, turn, 0, 1);
        if (Math.abs(L.angle) < 1 || Math.abs(Math.abs(L.angle) - 180) < 1) ok(`${why}: the size is the fit's (text ${L.textW.toFixed(0)} px, letters ${L.glyphW.toFixed(0)} px)`, L.textW / L.glyphW > 0.7 && L.textW / L.glyphW < 1.45);
      }
    }

    /* ── C · no job yet, and a written piece ── */
    const C = await page.evaluate(key => {
      const P = CharmNestEngravePreview, row = B.orders.byKey.get(key), out = [];
      for (const j of Engrave.jobsOf(row)) {
        const face = j.face === 'front' ? 'front' : 'back', fj = P.frameFor({ job: j }), fr = P.frameFor({ row, face, slot: j.slot || null });
        const pos = P.positionOf(j), keep = Object.fromEntries(Object.entries(j.fit).filter(([k]) => !['glyphs', 'cmds', 'layout'].includes(k)));
        const clone = { key: j.key, face: j.face, row: j.row, copies: j.copies, slot: j.slot, state: 'written', view: null, fit: null, writtenFit: Object.assign(keep, { lines: j.lines, approvedAt: 1, upAngle: j.view.upAngle }) };
        const fw = P.frameFor({ job: clone }), pw = P.positionOf(clone, { row });
        out.push({ key: j.key, face, viewBox: fj && fj.box, charmBox: fr && fr.box, writtenBox: fw && fw.box, pos, posWritten: pw, art: fr ? fr.art.length : -1, kinds: [fj && fj.kind, fr && fr.kind, fw && fw.kind] });
      }
      return out;
    }, made.both.key);
    for (const c of C) {
      ok(`${c.face}: three sources, one frame kind each (view, charm, charm)`, c.kinds.join() === 'view,charm,charm');
      for (const [what, b] of [['no job yet', c.charmBox], ['written piece', c.writtenBox]]) for (let i = 0; i < 4; i++) near(`${c.face}, ${what}: the frame's box edge ${i} is the fitted view's`, b[i], c.viewBox[i], 0.06);
      for (const k of ['x', 'y', 'size', 'angle']) near(`${c.face}, written piece: the words' ${k} are where they were`, c.posWritten[k], c.pos[k], k === 'angle' ? 0.01 : 0.002);
      ok(`${c.face}: the front frame carries the design's art, the back none`, c.face === 'front' ? c.art > 0 : c.art === 0);
    }
    // a comet's front (no front job in this world: drawn from the pool's charm) carries its blue hatching and its engraving
    const comet = await page.evaluate(async key => {
      const P = CharmNestEngravePreview, row = B.orders.byKey.get(key), host = document.createElement('div'); host.style.cssText = 'width:104px;height:104px;position:fixed;left:0;top:0'; document.body.appendChild(host);
      P.render(host, { row, face: 'front', slot: 'D1', text: 'A' }); const cv = host.querySelector('.engpvDraw canvas'), px = cv ? __T.pix(cv) : null, back = document.createElement('div'); back.style.cssText = host.style.cssText; document.body.appendChild(back);
      P.render(back, { row, face: 'back', slot: 'D1', text: 'A' }); const cb = back.querySelector('.engpvDraw canvas'), pb = cb ? __T.pix(cb) : null; host.remove(); back.remove(); return { front: px, back: pb };
    }, made.discs.key);
    ok('a comet front drawn with no job: the cut line, the cut-outs and the blue hatching', comet.front && comet.front.blue > 80 && comet.front.dark > 100 && comet.front.holes > 10);
    ok('a comet back: the metal turned over, none of the front\'s hatching', comet.back && comet.back.blue === 0 && comet.back.a > 1500 && comet.front.blue > 0);

    /* ── D · the Engrave tab's detail: every change lands in the same tick ── */
    page.on('request', r => { if (!/\/vendor\/fonts\//.test(r.url())) { let op = ''; try { const b = JSON.parse(r.postData() || '{}'); op = b.op || b.name || ''; } catch (_) { /* a GET */ } requests.push(r.url().replace(/^https?:\/\/[^/]+/, '') + (op ? ' ' + op : '')); } });
    const jkey = await R.openEngraveDetail(w, 'both', { face: 'back' }); await settleFonts();
    ok('the detail opened on the back job', !!jkey);
    const thumbs = () => page.evaluate(() => { const g = f => { const t = document.querySelector(`.engpCard .engFace[data-face="${f}"] .engThumb`); return t ? { text: (t.querySelector('.engpvText') || {}).textContent, fam: getComputedStyle(t.querySelector('.engpvText') || document.body).fontFamily, style: (t.querySelector('.engpvText') || { getAttribute() { return ''; } }).getAttribute('style'), canvas: !!t.querySelector('.engpvDraw canvas'), state: (t.querySelector('.engpv') || {}).dataset && t.querySelector('.engpv').dataset.state } : null; }; return { front: g('front'), back: g('back') }; });
    let t0 = await thumbs();
    ok('the detail card shows the charm in both boxes', t0.front && t0.back && t0.front.canvas && t0.back.canvas);
    if (shots) { await R.shot(w, 'THUMB-detail-both'); }
    await page.waitForTimeout(800); requests.length = 0;   // (what the page itself still fetches after opening: not counted)
    const tick = await page.evaluate(async () => {
      const q = s => document.querySelector(s), out = {}, cv = f => q(`.engpCard .engFace[data-face="${f}"] .engThumb .engpvDraw canvas`), txt = f => { const t = q(`.engpCard .engFace[data-face="${f}"] .engThumb .engpvText`); return t ? t.textContent : null; }, fam = f => { const t = q(`.engpCard .engFace[data-face="${f}"] .engThumb .engpvText`); return t ? getComputedStyle(t).fontFamily : ''; };
      const c0 = { front: cv('front'), back: cv('back') };
      const ta = q('.engp .engpWords'), sel = q('.engp .engpFont'); out.hasPanel = !!ta && !!sel;
      // words: the shown face (back), same tick; five keystrokes, each read at once
      out.keys = []; for (const v of ['Q', 'Qq', 'Qq ', 'Qq Z', 'Qq Zz']) { ta.value = v; ta.dispatchEvent(new Event('input', { bubbles: true })); out.keys.push(txt('back')); }
      out.typedBack = txt('back'); out.frontKept = txt('front');
      // font: same tick (Stylish is Playwrite, Ace is Jost)
      const wasFam = fam('back'); sel.value = 'Stylish'; sel.dispatchEvent(new Event('change', { bubbles: true })); out.famAfter = fam('back'); out.famBefore = wasFam;
      out.sameCanvas = cv('front') === c0.front && cv('back') === c0.back;   // (typing and a font pick: the very same canvas elements)
      return out;
    });
    await page.waitForTimeout(400); const typedRequests = requests.splice(0).filter(u => !/ (getOrderPieces|cancelList)$|messagesFor=/.test(u));   // (the page's own background reads are not the thumbnails')
    ok('the panel is there', tick.hasPanel);
    ok('typing changes the shown face\'s box at every keystroke, in the same tick (capitals for Ace), and leaves the other face alone', tick.keys.join('|') === 'Q|QQ|QQ|QQ Z|QQ ZZ' && tick.typedBack === 'QQ ZZ' && tick.frontKept === 'W');
    ok('a font pick changes the family in the same tick', /jost-ace/.test(tick.famBefore) && /playwrite-us-trad/.test(tick.famAfter));
    ok('the charm is NOT drawn again by any of it: the same canvas elements', tick.sameCanvas);
    ok('typing and a font pick made no request (the charm is drawn from the page\'s own geometry; fonts are files of this site): ' + JSON.stringify(typedRequests), typedRequests.length === 0);
    // the other face is picked by its thumbnail: typing there lands in its own box and the first face keeps its staged words
    const other = await page.evaluate(() => {
      const q = s => document.querySelector(s), txt = f => { const t = q(`.engpCard .engFace[data-face="${f}"] .engThumb .engpvText`); return t ? t.textContent : null; };
      q('.engpCard .engFace[data-face="front"] .engThumb').click(); const ta2 = q('.engp .engpWords'); ta2.value = 'Zz'; ta2.dispatchEvent(new Event('input', { bubbles: true }));
      return { front: txt('front'), back: txt('back') };
    });
    ok('the other face: typing lands in its own box, the first keeps its staged words: ' + JSON.stringify(other), other.front === 'ZZ' && other.back === 'Qq Zz');   // (the back is staged in Stylish: no capitals now)
    // position: a move of the saved placement changes the box in the same tick
    const mv = await page.evaluate(async () => {
      document.querySelector('.engpCard .engFace[data-face="back"] .engThumb').click();
      const th = () => document.querySelector('.engpCard .engFace[data-face="back"] .engThumb .engpvText'), res = {};
      for (const act of ['left', 'down', 'right', 'up']) { const b = document.querySelector(`.engp [data-act="${act}"]`); if (!b || b.disabled) continue; const before = th().getAttribute('style'); b.click(); const after = th().getAttribute('style'); if (before !== after) { res.act = act; res.before = before; res.after = after; break; } }
      return res;
    });
    ok('a position nudge moves the words on the thumbnail in the same tick (' + (mv.act || 'none moved') + ')', !!mv.act && mv.before !== mv.after && /left:/.test(mv.after));
    // mode: leaving only the back leaves the front without words (ENGSMART removes its box)
    const md = await page.evaluate(() => { const b = document.querySelector('.engp [data-engp="mode"][data-mode="back"]'); if (b) b.click(); const t = document.querySelector('.engpCard .engFace[data-face="front"] .engThumb .engpvText'); return { clicked: !!b, frontText: t ? t.textContent : null, backText: (document.querySelector('.engpCard .engFace[data-face="back"] .engThumb .engpvText') || {}).textContent }; });
    ok('a mode change reaches the boxes in the same tick: the front has no words (or no box), the back keeps its own', md.clicked && (md.frontText === null || md.frontText === '') && !!md.backText);
    ok('typing, a font pick and a face pick made no request (the charm is drawn from the page\'s own geometry; fonts are files of this site): ' + JSON.stringify(typedRequests), typedRequests.length === 0);

    /* ── E · no geometry at all: the order window's own Vector design ── */
    const E = await page.evaluate(async key => {
      const P = CharmNestEngravePreview, row = B.orders.byKey.get(key), bare = Object.assign({}, row, { poolIds: [] }), mk = () => { const h = document.createElement('div'); h.style.cssText = 'width:104px;height:104px;position:fixed;left:0;top:0'; document.body.appendChild(h); return h; };
      const h = mk(); P.render(h, { row: bare, face: 'back', text: 'Hi' });
      const first = { pic: h.querySelector('.engpv').dataset.pic, wait: h.querySelector('.engpvWait') && !h.querySelector('.engpvWait').hidden ? h.querySelector('.engpvWait').textContent.trim() : '' };
      for (let i = 0; i < 100 && !(h.querySelector('.engpvDraw img') || (h.querySelector('.engpv') && h.querySelector('.engpv').dataset.pic === 'none')); i++) await new Promise(r => setTimeout(r, 100));
      const im = h.querySelector('.engpvDraw img'); await (im && im.decode ? im.decode().catch(() => {}) : null);
      const out = { first, pic: h.querySelector('.engpv').dataset.pic, img: !!im, nat: im ? [im.naturalWidth, im.naturalHeight] : null, flip: h.querySelector('.engpv').getAttribute('data-flip'), ar: h.querySelector('.engpvFrame').hasAttribute('data-ar'), text: h.querySelector('.engpvText').textContent, waitAfter: !h.querySelector('.engpvWait').hidden };
      h.remove();
      // nothing at all: no pool, no vector: still no error, the words alone
      const h2 = mk(); P.render(h2, { row: { key: 'x', spec: {}, line: {}, poolIds: [] }, face: 'front', text: 'Hi' }); out.none = { pic: h2.querySelector('.engpv').dataset.pic, text: h2.querySelector('.engpvText').textContent }; h2.remove();
      return out;
    }, made.back.key);
    ok('with no pool the line\'s Vector design is the picture (a small "Loading charm" wait first)', E.first.pic.startsWith('vec') && /Loading charm|Loading font/.test(E.first.wait) && E.img && E.nat[0] > 0 && E.pic === 'vec' && !E.waitAfter);
    ok('the back of it is the picture turned over, the words on top, the frame takes the picture\'s shape', E.flip === '1' && E.ar && E.text === 'Hi');
    ok('no geometry and no vector: the words alone, no error', E.none.pic === 'none' && E.none.text === 'Hi');

    ok('no page error', w.errors.length === 0);
    if (w.errors.length) console.log('  page errors:', w.errors.slice(0, 3));
  } finally { await w.close(); }
  console.log(`engrave-thumbs: ${n} checks passed`);
})().catch(e => { console.error(e); process.exit(1); });
