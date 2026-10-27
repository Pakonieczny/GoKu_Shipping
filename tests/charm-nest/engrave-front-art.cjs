// FRONTART (Paul, 27 Oct 2026: "In the Engraving Modal the User sometimes needs to be able to see the engraving on the front of a given charm").
// The work area of the Engrave card (renderBack, editable) now draws the design's own art (job.view.dropped: a calendar's day grid and month letters) as vectors at the work area's own scale:
// on a FRONT always, muted, over the grid and the tint and under the cut line and the words; on a BACK only while the quiet "Show front" switch beside the grid's label is on, fainter, seen through
// the charm. The choice holds for the browser tab (sessionStorage). Display only: job.fit, job.mask, job.view and the saved records are never changed, and the pictures written with a piece never draw it.
//   A  the real renderBack source (no browser) on a made-up calendar plate, drawn on a canvas that records every call: the art's paths are drawn as vectors through the work area's own transform at two
//      sizes; the same paths and frame as the Front box (charm-nest-engrave-preview.js frameFor); colours by role (red line, blue hatch, a white fill knocked out; a black stray on CUT and a keep-out not
//      drawn); layer order; the back's art turned over and drawn only with "Show front" on; the tab's choice (sessionStorage, a tab that cannot keep it); the written pictures unchanged; the job untouched
//   B  the real page over the repo's fake site (tests/charm-nest/_engraveRealWorld.cjs: real drawings, intake, pool, fit; offline, nothing written anywhere real, made-up names): the front job's work
//      area draws its art as vectors, crisp at any size; "Show front" on a back (and only there), remembered in the tab; dragging still works on a front with art; nothing changes in job.fit / job.mask
//      / job.view, and "Approve both faces" with the switch on saves records and pictures with no trace of it
//   PW_DIR=<playwright node_modules> CHROMIUM=<chrome> node tests/charm-nest/engrave-front-art.cjs      (FA_ONLY=A or B; SHOTS=<dir> for the screenshots)
'use strict';
const assert = require('node:assert/strict'), fs = require('node:fs'), vm = require('node:vm'), path = require('node:path');
const root = path.join(__dirname, '../..');
const RESULTS = [];
const check = (id, ok, evidence) => { RESULTS.push({ id, ok: !!ok }); console.log(`${ok ? 'PASS' : 'FAIL'}  ${id}  ${evidence}`); };
const ONLY = process.env.FA_ONLY || '';
const fnv = s => { let h = 0x811c9dc5; for (let i = 0; i < s.length; i++) { h ^= s.charCodeAt(i); h = Math.imul(h, 0x01000193) >>> 0; } return h.toString(16); };

/* ═══ A · the real renderBack, a recording canvas ═══ */
function partA() {
  const G = require(path.join(root, 'charm-nest-geom.js')), Fit = require(path.join(root, 'charm-nest-engrave-fit.js')), T = require(path.join(root, 'charm-nest-text.js'));
  const Preview = require(path.join(root, 'charm-nest-engrave-preview.js'));
  const ot = require(path.join(root, 'vendor/opentype-1.3.4.min.js'));
  const { CharmNestPDF: P } = require(path.join(root, 'netlify/functions/_charmNestPdf.js'));
  const PT = 72 / 25.4, MM = 25.4 / 72;
  const bytes = f => { const b = fs.readFileSync(path.join(root, f)); return b.buffer.slice(b.byteOffset, b.byteOffset + b.byteLength); };
  const emoji = ot.parse(bytes('vendor/fonts/NotoEmoji-Regular.ttf')), emojiMap = require(path.join(root, 'vendor/fonts/emoji-sequences.json'));
  const fonts = Object.fromEntries(['Regular', 'Semibold'].map(w => [w, T.withEmoji(ot.parse(bytes(`vendor/fonts/SourceSans3-${w}.otf`)), emoji, emojiMap, ot.Path)]));

  // a canvas whose 2d context records every call with the state it was made in (canvases that are drawn are recorded by their id)
  let ids = 0;
  const makeCanvas = () => {
    const cv = { id: ++ids, width: 0, height: 0, ops: [] }, state = { strokeStyle: '#000', fillStyle: '#000', globalAlpha: 1, globalCompositeOperation: 'source-over', lineWidth: 1 }, stack = [];
    const ctx = new Proxy({}, {
      get: (_, k) => {
        if (k === 'canvas') return cv;
        if (k === 'createImageData') return (w, h) => ({ data: new Uint8ClampedArray(w * h * 4), width: w, height: h });
        if (k === 'measureText') return t => ({ width: String(t).length * 6 });
        if (k === 'save') return () => { stack.push(Object.assign({}, state)); cv.ops.push(['save']); };
        if (k === 'restore') return () => { Object.assign(state, stack.pop() || {}); cv.ops.push(['restore']); };
        if (k in state) return state[k];
        return (...a) => { cv.ops.push([k, a.map(v => (v && Array.isArray(v.ops) ? { canvas: v.id, w: v.width, h: v.height } : v)), Object.assign({}, state)]); };
      },
      set: (_, k, v) => { state[k] = v; cv.ops.push(['set', k, v]); return true; }
    });
    cv.getContext = () => ctx; made.set(cv.id, cv); return cv;
  };
  const made = new Map();
  const W = { sessionStorage: (() => { const m = new Map(); return { getItem: k => (m.has(k) ? m.get(k) : null), setItem: (k, v) => m.set(k, String(v)), _m: m }; })() };
  const src = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8');
  const cut = (a, b) => { const i = src.indexOf(a), j = src.indexOf(b, i); assert(i > 0 && j > i, `bridge source markers: ${a}`); return src.slice(i, j); };
  const env = vm.createContext({ G, P, PT, MM, Math, Array, Infinity, document: { createElement: () => makeCanvas() }, console, window: W });
  vm.runInContext(cut('  function textBox(glyphs', '  function resize(job, size)') + cut('  /* ── 7.6 · back files', '  function renderFront(') +
    '\nthis.renderBack = renderBack; this.frontPeek = frontPeek; this.FRONT_ART_ALPHA = FRONT_ART_ALPHA; this.FRONT_INK = FRONT_INK;', env);

  // a made-up calendar plate, 110 x 80 pt, its hanging hole on the right (so the view is turned 90 degrees: the turn must be the view's own): a day grid of 14 red engraved squares, three blue
  // hatched "letters" (the first with a white window knocked out of it), a blue hatch line, a black stray line left open on CUT and a FRONT KEEP-OUT square (neither is art)
  const poly = (pts, open) => pts.map((p, i) => [i ? 'l' : 'm', p]).concat(open ? [] : [['h']]);
  const bb = pts => [Math.min(...pts.map(p => p[0])), Math.min(...pts.map(p => p[1])), Math.max(...pts.map(p => p[0])), Math.max(...pts.map(p => p[1]))];
  const rect = (x, y, w, h) => [[x, y], [x + w, y], [x + w, y + h], [x, y + h]];
  const ring = (cx, cy, r) => [...Array(32).keys()].map(i => [cx + r * Math.cos(i / 32 * 2 * Math.PI), cy + r * Math.sin(i / 32 * 2 * Math.PI)]);
  const seg = (pts, o) => Object.assign({ kind: 'path', closed: !o.open, subpaths: [poly(pts, o.open)], bbox: bb(pts), lwPt: 0.25, paintOp: o.fill ? 'f' : 'S' }, o);
  const cutP = pts => seg(pts, { layer: 'CUT', stroke: true, fill: false, strokeRGB: [0, 0, 0] });
  const red = pts => seg(pts, { layer: 'ENGRAVE', stroke: true, fill: false, strokeRGB: [1, 0.114, 0.145], lwPt: 0.283 });
  const blue = pts => seg(pts, { layer: 'HATCH', stroke: false, fill: true, fillRGB: [0, 0, 1] });
  const paper = pts => seg(pts, { layer: 'HATCH', stroke: false, fill: true, fillRGB: [1, 1, 1] });
  const outline = cutP(rect(0, 0, 110, 80)), hole = cutP(ring(104, 40, 2.5));
  const days = []; for (let r = 0; r < 2; r++) for (let c = 0; c < 7; c++) days.push(red(rect(8 + c * 10, 8 + r * 10, 8, 8)));
  const letters = [blue(rect(10, 60, 6, 12)), blue(rect(18, 60, 6, 12)), blue(rect(26, 60, 6, 12))], windowP = paper(rect(11.5, 63, 3, 5));
  const hatchLine = seg([[8, 50], [30, 50]], { open: true, layer: 'ENGRAVE', stroke: true, fill: false, strokeRGB: [0, 0, 0], hatchLine: true, hatchBlue: true });
  const stray = seg([[40, 77], [70, 77]], { open: true, layer: 'CUT', stroke: true, fill: false, strokeRGB: [0, 0, 0] });
  const keep = seg(rect(84, 8, 8, 8), { layer: 'FRONT KEEP-OUT', stroke: true, fill: false, strokeRGB: [0, 1, 0] });
  const ART = [...days, ...letters, windowP, hatchLine];
  const charm = { outline, members: [outline, hole, ...days, ...letters, windowP, hatchLine, stray, keep], bbox: outline.bbox };
  const prepare = face => {
    const res = Fit.calculate({ charm, lines: ['Anna'], lineMode: 'auto', viewOptions: G.viewOptionsFor(face === 'front' ? { face: 'front' } : {}), maskOptions: { marginMm: 0.8, keepOut: [] },
      opts: { minCapMm: 1.6, maxHeightFrac: 0.4, lineGap: 0.216, minStrokeMm: 0, minGapMm: 0, tryRotated: false } }, fonts, G);
    assert(res.fit, `the ${face} words fit: ${res.reason}`);
    return Object.assign({ view: res.view, mask: res.mask, fit: res.fit }, face === 'front' ? { face: 'front' } : {});
  };
  const front = prepare('front'), back = prepare('back');
  const jobHash = j => fnv(JSON.stringify({ fit: j.fit, mask: { w: j.mask.w, h: j.mask.h, res: j.mask.res, ox: j.mask.ox, oy: j.mask.oy, cx: j.mask.cx, cy: j.mask.cy, bits: fnv(Array.from(j.mask.bits).join('')) },
    view: { M: j.view.M, R: j.view.R, up: j.view.upAngle, face: j.view.face || null, members: j.view.members.map(m => m.subpaths), dropped: j.view.dropped.map(d => [d.subpaths, d.layer, d.strokeRGB, d.fillRGB]), keys: Object.keys(j.view) } }));
  const before = { front: jobHash(front), back: jobHash(back) };
  check('A0 the plate: the front view is turned (hole on the right) and keeps its art in view.dropped; both faces fit', Math.abs(front.view.angleDeg) > 45 && front.view.dropped.length === ART.length + 2 && back.view.dropped.length === ART.length + 2,
    `angle ${front.view.angleDeg.toFixed(1)}, dropped ${front.view.dropped.length}/${back.view.dropped.length}`);

  const opsOf = cv => cv.ops;
  const pathOps = cv => opsOf(cv).filter(o => ['moveTo', 'lineTo', 'bezierCurveTo', 'closePath'].includes(o[0])).map(o => [o[0], ...o[1]]);
  const expectedPath = (art, tx) => { const out = []; for (const s of art) for (const sub of s.subpaths) for (const op of sub) { if (op[0] === 'm') out.push(['moveTo', ...tx(op[1][0], op[1][1])]); else if (op[0] === 'l') out.push(['lineTo', ...tx(op[1][0], op[1][1])]); else if (op[0] === 'c') out.push(['bezierCurveTo', ...tx(...op[1]), ...tx(...op[2]), ...tx(...op[3])]); else if (op[0] === 'h') out.push(['closePath']); } return out; };
  /** the art layer laid on the main canvas: the drawImage of a canvas the size of the main canvas at alpha a (and the canvas drawn) */
  const layerOn = (cv, a) => { const i = opsOf(cv).findIndex(o => o[0] === 'drawImage' && o[1][0] && o[1][0].canvas && Math.abs(o[2].globalAlpha - a) < 1e-9); return i < 0 ? null : { i, op: opsOf(cv)[i], layer: made.get(opsOf(cv)[i][1][0].canvas) }; };

  // A1 · the FRONT: the art's own paths, through the work area's own transform, at two very different sizes
  const sizes = [[400, 400], [1400, 1100]], ks = [];
  for (const px of sizes) {
    const cv = env.renderBack(front, px, { grid: true, editable: true }), L = layerOn(cv, env.FRONT_ART_ALPHA.front);
    ks.push(cv._map.k);
    const got = L ? pathOps(L.layer) : [], want = expectedPath(cv._frontArt, cv._map.tx);
    check(`A1 front at ${px.join('x')}: the art is drawn as vectors through the work area's own transform (${want.length} path steps), on a layer the canvas's own size laid on 1:1 at alpha ${env.FRONT_ART_ALPHA.front}`,
      !!L && L.layer.width === cv.width && L.layer.height === cv.height && L.op[1].length === 3 && L.op[1][1] === 0 && L.op[1][2] === 0 && JSON.stringify(got) === JSON.stringify(want) && want.length === 92,
      L ? `layer ${L.layer.width}x${L.layer.height} on ${cv.width}x${cv.height}, ${got.length} steps drawn, ${want.length} expected, k ${cv._map.k.toFixed(3)} px/pt` : 'no art layer');
  }
  check('A2 the two sizes are drawn at their own scale (never one picture stretched): k differs', ks[1] > 2.5 * ks[0], ks.map(k => k.toFixed(3)).join(' vs '));

  // A3 · the very paths and frame of the Front box (charm-nest-engrave-preview.js frameFor: view.M, then view.R)
  {
    const cv = env.renderBack(front, [500, 500], { grid: true, editable: true }), F = Preview.frameFor({ job: front });
    const same = cv._frontArt.every(s => { const f = F.art.find(a => a.original === s.original); return f && JSON.stringify(f.subpaths) === JSON.stringify(s.subpaths) && JSON.stringify(f.bbox) === JSON.stringify(s.bbox); });
    const own = cv._frontArt.every(s => JSON.stringify(s.subpaths) === JSON.stringify(G.transformSeg(G.transformSeg(s.original, front.view.M), front.view.R).subpaths));
    check('A3 the front art is the Front box\'s own art, path for path, in its frame (view.M then view.R)', cv._frontArt.length === ART.length && same && own && cv._frontArt.every(s => ART.includes(s.original)),
      `${cv._frontArt.length} paths drawn of the box's ${F.art.length} (the box also shows the stray CUT line)`);
  }

  // A4 · colours by role, and what is not art is not drawn
  {
    const cv = env.renderBack(front, [600, 600], { grid: true, editable: true }), L = layerOn(cv, env.FRONT_ART_ALPHA.front).layer, I = env.FRONT_INK, ops = opsOf(L);
    const strokes = c => ops.filter(o => o[0] === 'stroke' && o[2].strokeStyle === c).length, fills = (c, comp) => ops.filter(o => o[0] === 'fill' && o[2].fillStyle === c && o[2].globalCompositeOperation === comp).length;
    const r = { redLines: strokes(I.line), blueFills: fills(I.hatch, 'source-over'), blueLines: strokes(I.hatch), knocked: ops.filter(o => o[0] === 'fill' && o[2].globalCompositeOperation === 'destination-out').length, paths: ops.filter(o => o[0] === 'beginPath').length,
      other: ops.filter(o => (o[0] === 'stroke' && ![I.line, I.hatch].includes(o[2].strokeStyle)) || (o[0] === 'fill' && o[2].globalCompositeOperation === 'source-over' && o[2].fillStyle !== I.hatch)).length };
    check('A4 by role: 14 red day squares, 3 blue hatched letters, 1 blue hatch line, the white window knocked out of the letter under it; the black stray on CUT and the keep-out are not drawn',
      r.redLines === 14 && r.blueFills === 3 && r.blueLines === 1 && r.knocked === 1 && r.paths === ART.length && r.other === 0 && !cv._frontArt.some(s => s.original === stray || s.original === keep), JSON.stringify(r));
  }

  // A5 · the order of the layers: the tint, then the art, then the cut line, then the words
  {
    const cv = env.renderBack(front, [500, 500], { grid: true, editable: true }), ops = opsOf(cv);
    const tint = ops.findIndex(o => o[0] === 'drawImage' && o[2].globalCompositeOperation === 'multiply'), art = layerOn(cv, env.FRONT_ART_ALPHA.front).i;
    const cutI = ops.findIndex(o => o[0] === 'stroke' && o[2].strokeStyle === '#000'), words = ops.findIndex((o, i) => i > cutI && o[0] === 'fill' && o[2].fillStyle === '#111');
    check('A5 over the grid and the tint, under the cut line and the words', tint >= 0 && tint < art && art < cutI && cutI < words, `tint ${tint} < art ${art} < cut line ${cutI} < words ${words}`);
  }

  // A6 · the pictures written with a piece (not editable: the approval .png, the look, the Approved card) are exactly what they were: the same calls as the same job with no art at all
  {
    const bare = j => Object.assign({}, j, { view: Object.assign({}, j.view, { dropped: [] }) });
    const norm = cv => JSON.stringify(opsOf(cv).map(o => o.map(v => (Array.isArray(v) ? v.map(x => (x && x.canvas ? 'canvas' : x)) : v))));
    const sameAll = [[front, { grid: false, hatch: false }, 500], [front, { grid: true }, 700], [back, { hatch: false }, 360], [front, { hatch: false, grid: false }, 900]]
      .map(([j, o, px]) => { const a = env.renderBack(j, px, o), b = env.renderBack(bare(j), px, o); return a._frontArt.length === 0 && norm(a) === norm(b) && a.width === b.width && a.height === b.height; });
    check('A6 the written pictures (not editable) draw no art and make exactly the calls they made before', sameAll.every(Boolean), JSON.stringify(sameAll));
  }

  // A7 · the BACK: the front's art turned over with the back (the view's own mirror), drawn only while "Show front" is on, fainter
  {
    const cv = env.renderBack(back, [500, 500], { grid: true, editable: true }), first = opsOf(cv).filter(o => o[0] === 'drawImage').pop();
    const turned = cv._frontArt.every(s => JSON.stringify(s.subpaths) === JSON.stringify(G.transformSeg(G.transformSeg(s.original, back.view.M), back.view.R).subpaths));
    const mirrored = JSON.stringify(cv._frontArt[0].subpaths) !== JSON.stringify(G.transformSeg(cv._frontArt[0].original, back.view.R).subpaths);
    const offAtFirst = !cv._showFront && !layerOn(cv, env.FRONT_ART_ALPHA.back) && !made.get(first[1][0].canvas).ops.some(o => o[0] === 'drawImage' && Math.abs(o[2].globalAlpha - env.FRONT_ART_ALPHA.back) < 1e-9);
    cv._showFront = true; cv._paint();
    const shown = opsOf(cv).filter(o => o[0] === 'drawImage').pop(), peek = made.get(shown[1][0].canvas), L = layerOn(peek, env.FRONT_ART_ALPHA.back);
    const peekOps = opsOf(peek), artAt = L ? L.i : -1, cutAt = peekOps.findIndex(o => o[0] === 'stroke' && o[2].strokeStyle === '#000');
    const coords = L && JSON.stringify(pathOps(L.layer)) === JSON.stringify(expectedPath(cv._frontArt, cv._map.tx));
    cv._showFront = false; cv._paint(); const off = opsOf(cv).filter(o => o[0] === 'drawImage').pop();
    check('A7 a back: the same art turned over with the back (view.M mirror, then view.R); off by default; "Show front" lays it on at ' + env.FRONT_ART_ALPHA.back + ' under the cut line; off again is the plain back',
      cv._frontArt.length === ART.length && turned && mirrored && offAtFirst && !!L && coords && artAt < cutAt && shown[1][0].canvas !== first[1][0].canvas && off[1][0].canvas === first[1][0].canvas,
      JSON.stringify({ art: cv._frontArt.length, turned, mirrored, offAtFirst, layer: !!L, coords, artAt, cutAt }));
  }

  // A8 · the tab's choice: sessionStorage, read by every new work area; a tab that cannot keep it holds it in memory
  {
    const store = W.sessionStorage;
    const a = env.frontPeek(), b = env.frontPeek(true), kept = store.getItem('cn.engraveShowFront'), cv = env.renderBack(back, [400, 400], { grid: true, editable: true });
    const onAtOnce = cv._showFront === true && !!layerOn(made.get(opsOf(cv).filter(o => o[0] === 'drawImage').pop()[1][0].canvas), env.FRONT_ART_ALPHA.back);
    const fr = env.renderBack(front, [400, 400], { grid: true, editable: true }).
      _showFront;   // (a front never "shows the front": its art is always there)
    env.frontPeek(false); const kept2 = store.getItem('cn.engraveShowFront');
    Object.defineProperty(W, 'sessionStorage', { configurable: true, get() { throw new Error('denied'); } });
    const c = env.frontPeek(true), d = env.frontPeek(), e = env.frontPeek(false), f = env.frontPeek();
    Object.defineProperty(W, 'sessionStorage', { configurable: true, value: store });
    check('A8 "Show front" is the tab\'s: off at first, kept in sessionStorage ("1"/"0"), read by the next work area; a tab whose storage throws keeps it in memory', a === false && b === true && kept === '1' && onAtOnce && fr === false && kept2 === '0' && c === true && d === true && e === false && f === false,
      JSON.stringify({ a, b, kept, onAtOnce, fr, kept2, c, d, e, f }));
  }

  // A9 · display only
  check('A9 job.fit, job.mask and job.view are exactly as they were after every drawing above', jobHash(front) === before.front && jobHash(back) === before.back, `${jobHash(front)} / ${jobHash(back)}`);
}

/* ═══ B · the real page ═══ */
async function partB() {
  const R = require('./_engraveRealWorld.cjs');
  const shots = process.env.SHOTS || '/tmp/engrave-front-art-shots';
  const w = await R.world({ shots });
  if (!w) { console.log('SKIP no playwright-core'); return; }
  const page = w.page, Q = '#egQueue .rvItem[data-kind=placement]';
  try {
    const made = await R.build(w, ['both', 'front', 'discTwin', 'tag']);
    // what is recorded on every real 2d context while window.__rec is on
    await page.evaluate(() => {
      const P2 = CanvasRenderingContext2D.prototype;
      for (const k of ['moveTo', 'lineTo', 'bezierCurveTo', 'closePath', 'stroke', 'fill', 'drawImage']) {
        const f = P2[k];
        P2[k] = function (...a) { if (window.__rec) window.__rec.push({ cv: this.canvas, k, a: k === 'drawImage' ? [a[0], ...a.slice(1)] : a, alpha: this.globalAlpha, comp: this.globalCompositeOperation, ss: String(this.strokeStyle), fs: String(this.fillStyle) }); return f.apply(this, a); };
      }
      window.__fa = {
        hash(j) {
          const h = s => { let x = 0x811c9dc5; for (let i = 0; i < s.length; i++) { x ^= s.charCodeAt(i); x = Math.imul(x, 0x01000193) >>> 0; } return x.toString(16); };
          return h(JSON.stringify({ fit: j.fit, mask: j.mask ? { w: j.mask.w, h: j.mask.h, res: j.mask.res, ox: j.mask.ox, oy: j.mask.oy, bits: h(Array.from(j.mask.bits).join('')) } : null,
            view: j.view ? { M: j.view.M, R: j.view.R, up: j.view.upAngle, face: j.view.face || null, members: j.view.members.map(m => m.subpaths), dropped: (j.view.dropped || []).map(d => d.subpaths), keys: Object.keys(j.view) } : null, state: j.state }));
        },
        jobs: rid => [...Engrave.items().values()].filter(j => (j.row && j.row.order && j.row.order.receiptId === rid) || (j.row && j.row.parentRow && j.row.parentRow.order.receiptId === rid)),
        /** the art layer of canvas cv drawn while recording (laid on cv itself, or on `on`: a back's picture with "Show front"): its path steps against the steps cv._frontArt makes through cv._map.tx */
        vector(cv, alpha, rec, on) {
          const lay = rec.find(r => r.cv === (on || cv) && r.k === 'drawImage' && Math.abs(r.alpha - alpha) < 1e-6 && r.a[0] instanceof HTMLCanvasElement && r.a[0].width === cv.width && r.a[0].height === cv.height && r.a.length === 3);
          if (!lay) return { layer: false };
          const L = lay.a[0], got = rec.filter(r => r.cv === L && ['moveTo', 'lineTo', 'bezierCurveTo', 'closePath'].includes(r.k)).map(r => [r.k, ...r.a]);
          const tx = cv._map.tx, want = [];
          for (const s of cv._frontArt) for (const sub of s.subpaths) for (const op of sub) { if (op[0] === 'm') want.push(['moveTo', ...tx(...op[1])]); else if (op[0] === 'l') want.push(['lineTo', ...tx(...op[1])]); else if (op[0] === 'c') want.push(['bezierCurveTo', ...tx(...op[1]), ...tx(...op[2]), ...tx(...op[3])]); else if (op[0] === 'h') want.push(['closePath']); }
          return { layer: true, w: L.width, h: L.height, steps: got.length, same: JSON.stringify(got) === JSON.stringify(want), k: cv._map.k, strokes: rec.filter(r => r.cv === L && r.k === 'stroke').map(r => r.ss), fills: rec.filter(r => r.cv === L && r.k === 'fill').map(r => r.fs) };
        },
        /** pixels of a canvas: reddish (the line art, muted or faint) and near-black (the words, the cut line) */
        pix(cv) {
          const d = cv.getContext('2d').getImageData(0, 0, cv.width, cv.height).data; let red = 0, dark = 0;
          for (let i = 0; i < d.length; i += 4) { const r = d[i], g = d[i + 1], b = d[i + 2]; if (r > 150 && r - g > 18 && r - b > 18) red++; if (r + g + b < 120) dark++; }
          return { red, dark };
        },
        card: () => document.querySelector('#egQueue .rvItem[data-kind=placement]'),
        cv: () => document.querySelector('#egQueue .rvItem[data-kind=placement] .backHost canvas'),
        toggle: () => document.querySelector('#egQueue .rvItem[data-kind=placement] .backHost .egFrontPeek')
      };
    });
    const rid = n => R.ORDERS[n].rid;
    const hashes = () => page.evaluate(rids => rids.map(r => __fa.jobs(r).map(j => [j.key, __fa.hash(j)])), ['both', 'discTwin', 'tag'].map(rid));
    const h0 = await hashes();
    const snap = await page.evaluate(r => __fa.jobs(r).map(j => ({ key: j.key, face: j.face === 'front' ? 'front' : 'back', size: j.fit && j.fit.size, centre: j.fit && j.fit.centre, angle: j.fit && j.fit.angle, up: j.view && j.view.upAngle, art: (j.view && j.view.dropped || []).length, copies: j.copies })), rid('both'));
    check('B0 the world: the cat\'s front and back are fitted and its design has line art on the front (view.dropped)', snap.length === 2 && snap.every(s => s.size > 0 && s.art > 0), JSON.stringify(snap.map(s => [s.face, +(s.size || 0).toFixed(2), s.art])));

    // B1 · the FRONT job's work area
    await R.openEngraveDetail(w, 'both', { face: 'front' }); await R.settle(page, 900);
    const f1 = await page.evaluate(() => {
      const cv = __fa.cv(), job = Engrave.items().get(__fa.card().dataset.key), F = CharmNestEngravePreview.frameFor({ job });
      const same = cv._frontArt.length > 0 && cv._frontArt.every(s => { const a = F.art.find(x => x.original === s.original); return a && JSON.stringify(a.subpaths) === JSON.stringify(s.subpaths); });
      window.__rec = []; __fa.card()._remountWork(); const rec = window.__rec; window.__rec = null;
      const cv2 = __fa.cv();
      return { face: job.face, art: cv2._frontArt.length, front: cv2._frontFace, same, toggle: !!__fa.toggle(), box: !!cv2._box, v: __fa.vector(cv2, 0.5, rec), pix: __fa.pix(cv2), size: [cv2.width, cv2.height] };
    });
    await R.shot(w, 'FA-front-work-area');
    check('B1 the front job: its work area draws the Front box\'s own art (the same paths in the same frame), with the words\' box, and no switch (a front always shows its art)',
      f1.face === 'front' && f1.front && f1.art > 0 && f1.same && !f1.toggle && f1.box, JSON.stringify({ art: f1.art, same: f1.same, toggle: f1.toggle, box: f1.box }));
    check('B2 …as vectors at the work area\'s own scale: every path step of the art goes through the canvas\'s own transform on a layer the canvas\'s size, laid on 1:1, in the red of a line engraving',
      f1.v.layer && f1.v.same && f1.v.steps > 20 && f1.v.w === f1.size[0] && f1.v.h === f1.size[1] && f1.v.strokes.length > 0 && f1.v.strokes.every(s => s === '#d63028'), JSON.stringify(f1.v));
    check('B3 the pixels: the muted red art is there, the words and cut line stay black', f1.pix.red > 300 && f1.pix.dark > 300, JSON.stringify(f1.pix));
    const sizes = await page.evaluate(() => {
      const job = Engrave.items().get(__fa.card().dataset.key), out = [];
      for (const px of [[320, 320], [1400, 1400]]) { window.__rec = []; const cv = Engrave.renderBack(job, px, { grid: true, editable: true }); const rec = window.__rec; window.__rec = null; out.push({ px, size: [cv.width, cv.height], k: cv._map.k, v: __fa.vector(cv, 0.5, rec) }); }
      return out;
    });
    check('B4 crisp at any size: drawn again at 320 and 1400 px, every time as vectors at that size\'s own scale (never one small picture stretched)',
      sizes.every(s => s.v.layer && s.v.same && s.v.w === s.size[0] && s.v.h === s.size[1]) && sizes[1].k > 3 * sizes[0].k, JSON.stringify(sizes.map(s => [s.size, +s.k.toFixed(2), s.v.steps, s.v.same])));

    // B5 · the BACK of the same piece: the quiet switch, off at first
    await page.evaluate(Q => { const b = document.querySelector(Q + ' .engCard .engFace[data-face="back"] .engThumb'); if (b) b.click(); }, Q); await R.settle(page, 900);
    const b1 = await page.evaluate(() => { const cv = __fa.cv(), t = __fa.toggle(), r = t && t.getBoundingClientRect(), c = cv.getBoundingClientRect(); const job = Engrave.items().get(__fa.card().dataset.key);
      return { face: job.face || 'back', art: cv._frontArt.length, front: cv._frontFace, show: cv._showFront, toggle: !!t, checked: t && t.querySelector('input').checked, text: t && t.textContent.trim(), pos: r && [Math.round(r.left - c.left), Math.round(r.top - c.top), Math.round(r.right - c.left)], cw: c.width, pix: __fa.pix(cv), kept: sessionStorage.getItem('cn.engraveShowFront') }; });
    check('B5 the back: a small "Show front" switch in the drawing\'s top-left corner beside the grid\'s label, off at first, and no art drawn', b1.face === 'back' && b1.art > 0 && !b1.front && b1.toggle && b1.checked === false && b1.show === false && b1.text === 'Show front' &&
      b1.pos[0] > 30 && b1.pos[1] >= 0 && b1.pos[1] < 30 && b1.pos[2] < b1.cw && b1.pix.red === 0 && b1.kept === null, JSON.stringify(b1));
    await page.click(Q + ' .backHost .egFrontPeek'); await R.settle(page, 300);
    const b2 = await page.evaluate(() => { const cv = __fa.cv(), t = __fa.toggle(); return { show: cv._showFront, checked: t.querySelector('input').checked, on: t.classList.contains('on'), pix: __fa.pix(cv), kept: sessionStorage.getItem('cn.engraveShowFront'), focus: document.activeElement && document.activeElement.matches && document.activeElement.matches('.egFrontPeek input') }; });
    await R.shot(w, 'FA-back-show-front');
    check('B6 a click: the front\'s art appears faintly through the back, the choice is kept for this tab, and the focus stays where it was (the card\'s keys keep working)', b2.show && b2.checked && b2.on && b2.pix.red > 100 && b2.pix.red < 1e9 && b2.kept === '1' && !b2.focus, JSON.stringify(b2));
    const b3 = await page.evaluate(() => { const cv = __fa.cv(); window.__rec = []; cv._paint(); const rec = window.__rec; window.__rec = null;
      const peek = rec.filter(r => r.cv === cv && r.k === 'drawImage').map(r => r.a[0])[0]; window.__rec = []; __fa.card()._remountWork(); const rec2 = window.__rec; window.__rec = null; const cv2 = __fa.cv();
      const peek2 = rec2.filter(r => r.cv === cv2 && r.k === 'drawImage').map(r => r.a[0]).pop();   // (the back with the front seen through it: the picture the work area lays down first)
      return { sameSize: !!peek && peek.width === cv.width, v: __fa.vector(cv2, 0.2, rec2, peek2), show: cv2._showFront, checked: __fa.toggle().querySelector('input').checked, art: cv2._frontArt.map(s => JSON.stringify(s.subpaths)).join('|') === cv2._frontArt.map(s => JSON.stringify(CharmNestGeom.transformSeg(CharmNestGeom.transformSeg(s.original, Engrave.items().get(__fa.card().dataset.key).view.M), Engrave.items().get(__fa.card().dataset.key).view.R).subpaths)).join('|') }; });
    check('B7 drawn again (a resize, a turn), the back keeps the switch on and draws the art as vectors, faint (0.2), turned over with the back (view.M, then view.R)', b3.show && b3.checked && b3.v.layer && b3.v.same && b3.art && b3.sameSize, JSON.stringify(b3));

    // B8 · another back with art: on at once (the tab remembers); a back with no art: no switch; off again
    await R.openEngraveDetail(w, 'discTwin', { face: 'back' }); await R.settle(page, 900);
    const d1 = await page.evaluate(() => { const cv = __fa.cv(), t = __fa.toggle(); return { art: cv._frontArt.length, toggle: !!t, checked: t && t.querySelector('input').checked, show: cv._showFront, pix: __fa.pix(cv) }; });
    await R.shot(w, 'FA-disc-back-show-front');
    await R.openEngraveDetail(w, 'tag', { face: 'back' }); await R.settle(page, 700);
    const t1 = await page.evaluate(() => { const cv = __fa.cv(); return { art: cv._frontArt.length, toggle: !!__fa.toggle(), text: cv.parentElement.textContent.trim() }; });
    await R.openEngraveDetail(w, 'discTwin', { face: 'back' }); await R.settle(page, 700);
    await page.click(Q + ' .backHost .egFrontPeek'); await R.settle(page, 300);
    const d2 = await page.evaluate(() => { const cv = __fa.cv(); return { show: cv._showFront, pix: __fa.pix(cv), kept: sessionStorage.getItem('cn.engraveShowFront') }; });
    check('B8 the next back with art opens with the switch on (the tab remembers) and its flower seen through; the mini tag (no art on its front) has no switch; off again: no art, kept "0"',
      d1.art > 10 && d1.toggle && d1.checked && d1.show && d1.pix.red > 50 && t1.art === 0 && !t1.toggle && t1.text === '' && !d2.show && d2.pix.red === 0 && d2.kept === '0', JSON.stringify({ d1, t1, d2 }));
    // (on again for the approval below: the switch on while approving)
    await R.openEngraveDetail(w, 'both', { face: 'back' }); await R.settle(page, 700);
    await page.click(Q + ' .backHost .egFrontPeek'); await R.settle(page, 300);

    // B9 · display only: every job's fit, mask and view exactly as built
    const h1 = await hashes();
    check('B9 nothing a person did with the pictures changed job.fit, job.mask or job.view (every job of the three orders)', JSON.stringify(h0) === JSON.stringify(h1), JSON.stringify(h1).slice(0, 200));

    // B10 · approval with the switch on: the saved records and pictures carry no trace of it. (The world's sheet is the page's own: the fake site is given its folder and its record, as a nested
    // sheet has them, so the back files are uploaded and the records written for real: Charm_Pool_Back, the .ai and the .png in Storage)
    const sheets = await page.evaluate(() => { const out = []; for (const s of allSheets()) if (s.charms && s.charms.length) { if (!s.folderPath) s.folderPath = 'charmnest/sheets/' + s.sheetId; out.push({ sheetId: s.sheetId, poolIds: [...new Set([...Engrave.items().values()].flatMap(j => j.copies || []))] }); } return out; });
    for (const s of sheets) w.srv.st.put('Charm_Nest_Sheets', s.sheetId, { sheetId: s.sheetId, poolIds: s.poolIds, backPool: [] });
    await page.evaluate(Q => { const b = document.querySelector(Q + ' .engCard .engFace[data-face="front"] .engThumb'); if (b) b.click(); }, Q); await R.settle(page, 700);
    const pressed = await page.evaluate(Q => { const b = document.querySelector(Q + ' [data-a="approveBoth"]:not([disabled]), ' + Q + ' [data-a="approveFaces"]:not([disabled])'); if (b) b.click(); return !!b; }, Q);
    const poolIds = snap[0].copies;
    const want = poolIds.flatMap(p => [p, p + '~front']);
    const t0 = Date.now(); while (Date.now() - t0 < 120000 && !want.every(id => w.srv.st.doc('Charm_Pool_Back', id))) await new Promise(r => setTimeout(r, 400));
    const recs = want.map(id => w.srv.st.doc('Charm_Pool_Back', id));
    const okNums = recs.every((r, i) => { if (!r) return false; const s = snap.find(x => x.face === (want[i].endsWith('~front') ? 'front' : 'back')); return r.sizePt === +s.size.toFixed(3) && JSON.stringify(r.centre) === JSON.stringify(s.centre.map(v => +v.toFixed(2))) && r.angle === s.angle && r.upAngle === s.up; });
    const trace = recs.filter(Boolean).some(r => /frontart|showfront|frontpeek|egfrontpeek|engraveshowfront/i.test(JSON.stringify(r)));
    check('B10 "Approve both faces" with "Show front" on: both records saved, their size, place, turn and up angle exactly the fit\'s, and nothing of the switch in them', pressed && recs.every(Boolean) && okNums && !trace,
      JSON.stringify({ pressed, saved: recs.map(Boolean), okNums, trace }));
    const pngs = recs.filter(Boolean).map(r => ({ face: r.face || (String(r.name || '').includes('_front_') ? 'front' : 'back'), path: r.outputs && r.outputs.png && r.outputs.png.path, poolId: r.poolId }));
    const pngFacts = [];
    for (const p of pngs) {
      const blob = p.path && w.srv.st.blobs.get(p.path);
      const b64 = blob ? Buffer.from(blob.buf).toString('base64') : null;
      pngFacts.push(await page.evaluate(async ({ b64, rid, front }) => {
        if (!b64) return { file: false };
        const img = new Image(); img.src = 'data:image/png;base64,' + b64; await img.decode();
        const c = document.createElement('canvas'); c.width = img.width; c.height = img.height; c.getContext('2d').drawImage(img, 0, 0);
        const j = __fa.jobs(rid).find(x => (x.face === 'front') === front);
        const fresh = Engrave.renderBack(j, 500, { grid: false, hatch: false });
        const a = c.getContext('2d').getImageData(0, 0, c.width, c.height).data, b = fresh.getContext('2d').getImageData(0, 0, fresh.width, fresh.height).data;
        let diff = 0; if (a.length === b.length) for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) diff++;
        return { file: true, w: c.width, h: c.height, fw: fresh.width, fh: fresh.height, pix: __fa.pix(c), diff, sameSize: a.length === b.length };
      }, { b64, rid: rid('both'), front: p.path ? /\/front\//.test(p.path) : false }));
    }
    check('B11 the saved .png of each face is the plain picture it always was: no art in it, pixel for pixel the written picture of the fit', pngFacts.length === 2 && pngFacts.every(f => f.file && f.sameSize && f.diff === 0 && f.pix.red === 0),
      JSON.stringify(pngFacts));

    // B12 · the words' box and dragging still work on a front with art (the cat's front-only job)
    await R.openEngraveDetail(w, 'front', { face: 'front' }); await R.settle(page, 900);
    const g0 = await page.evaluate(() => { const cv = __fa.cv(), job = Engrave.items().get(__fa.card().dataset.key), r = cv.getBoundingClientRect(), s = r.width / cv.width;
      return { art: cv._frontArt.length, box: !!cv._box, x: r.left + cv._box.centrePx[0] * s, y: r.top + cv._box.centrePx[1] * s, centre: job.fit.centre.slice(), key: job.key }; });
    await page.mouse.move(g0.x, g0.y); await page.mouse.down(); await page.mouse.move(g0.x + 6, g0.y + 3, { steps: 4 }); await page.mouse.up(); await R.settle(page, 500);
    const g1 = await page.evaluate(key => { const job = Engrave.items().get(key); return { centre: job.fit.centre.slice(), onMask: job.fit.glyphs.every(g => g.cmds.every(c => c.type === 'Z' || CharmNestGeom.at(job.mask, c.x, c.y) === 1)), box: !!(__fa.cv() && __fa.cv()._box), art: __fa.cv()._frontArt.length }; }, g0.key);
    check('B12 a front with hatched art (the cat\'s front-only job): the words\' box is drawn and a drag still moves the words, which stay on the usable area', g0.art > 0 && g0.box && g1.box && g1.art === g0.art && (g1.centre[0] !== g0.centre[0] || g1.centre[1] !== g0.centre[1]) && g1.onMask,
      JSON.stringify({ art: g0.art, before: g0.centre.map(v => +v.toFixed(2)), after: g1.centre.map(v => +v.toFixed(2)), onMask: g1.onMask }));
    check('B13 no page error', w.errors.length === 0, JSON.stringify(w.errors).slice(0, 300));
    console.log('screenshots in ' + shots);
  } finally { await w.close(); }
}

(async () => {
  if (ONLY !== 'B') partA();
  if (ONLY !== 'A') await partB();
  const bad = RESULTS.filter(r => !r.ok);
  console.log(`\n${RESULTS.length - bad.length}/${RESULTS.length} checks pass`);
  process.exit(bad.length ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
