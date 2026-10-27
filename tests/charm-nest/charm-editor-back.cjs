// CHARM EDIT · BACK (Paul, 27 Oct 2026: full control over every layer of a charm, front and back; what the person sees is what the laser does).
// A part a person adds to a piece's BACK in the charm editor (charm-nest-art-edit.js: the charm's artBack) is engraved by the back's own file (charm-nest-pdf.js buildBackFile, its ENGRAVE layer).
// The Engraving window draws it where the laser puts it, keeps the back's words off it, and an approved back is withdrawn and fitted again around it:
//   A  (node) charm-nest-geom.js engraveMask on a back, opts.avoid (the added parts): a filled part's whole inside is kept clear, two parts that overlap both (never an even-odd gap, as the keep-outs'
//      fill would open), an open line a band along itself, a frame round the body only its own line; a back given nothing is made exactly as before. buildBackFile writes the parts where the view
//      draws them (view.M, then view.R, then the page's T). The bridge's own renderBack (its source, a recording canvas, as engrave-front-art.cjs): the work area draws them as a front draws its art
//      (blue hatch and red line on a layer laid on at .5, under the cut line), the written pictures in the words' ink (#111); a back with nothing added makes exactly the calls it made before
//   B  the real sorter page over the fake site (_engraveRealWorld.cjs: real intake, pool, classifier and fit; offline, made-up names) with the calendar (_artCalendar.cjs) ordered front + back: the back
//      approved; "HI" typed in the charm editor on the back right where the back's words stand, and saved → the record and the piece's artBack; the back's work area draws it in blue where the file
//      puts it (the canvas's own pixels), the card's Back thumbnail draws it, the approved back is withdrawn in Engrave.artChanged's words and fitted again with its words clear of HI; approved
//      again, the written back file carries HI at the very place the screen shows (coordinates compared) and re-verifies, and its approval picture shows HI in the words' ink; the editor opens
//      again with HI an added, editable layer; the front is untouched; a calendar with no addition draws and masks its back as before
//   PW_DIR=<playwright node_modules> CHROMIUM=<chrome> node tests/charm-nest/charm-editor-back.cjs      (CB_ONLY=A or B; SHOTS=<dir> for the screenshots)
'use strict';
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path'), vm = require('node:vm'), os = require('node:os');
const ROOT = path.join(__dirname, '../..');
const RESULTS = [];
const check = (id, ok, evidence) => { RESULTS.push({ id, ok: !!ok }); console.log(`${ok ? 'PASS' : 'FAIL'}  ${id}  ${evidence || ''}`.trimEnd()); };
const ONLY = process.env.CB_ONLY || '';
const PT = 72 / 25.4, MM = 25.4 / 72;
const near = (a, b, tol) => !!a && !!b && a.length === b.length && a.every((v, i) => Math.abs(v - b[i]) <= tol);
const worst = (a, b) => (a && b ? Math.max(...a.map((v, i) => Math.abs(v - b[i]))) : Infinity);

async function nodeKit() {
  const hadWindow = 'window' in global, hadSelf = 'self' in global;
  global.window = global; global.self = global;
  global.PDFLib = global.PDFLib || require(path.join(ROOT, 'vendor/pdf-lib-1.17.1.min.js'));
  require(path.join(ROOT, 'charm-nest-pdf.js'));
  global.CharmNestGeom = global.CharmNestGeom || require(path.join(ROOT, 'charm-nest-geom.js'));
  const cal = await require('./_artCalendar.cjs')(ROOT);
  const kit = { P: global.CharmNestPDF, G: global.CharmNestGeom, L: global.PDFLib, AE: require(path.join(ROOT, 'charm-nest-art-edit.js')), cal };
  if (!hadWindow) delete global.window; if (!hadSelf) delete global.self;
  return kit;
}
/** The fill-only and the stroked paths a written back file engraves (not its cut reference), with their boxes in the page. */
async function engravedOf(P, bytes) {
  const p = await P.parseSource(bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes), 'back.ai');
  return p.segments.concat(p.nested || []).filter(s => s.kind === 'path' && (s.fill || s.stroke) && !/CUT OUTLINE/.test(s.layer || '')).map(s => ({ fill: !!s.fill, stroke: !!s.stroke, bbox: s.bbox }));
}

/* ═══ A · the mask, the file and the drawing, in node ═══ */
async function partA(kit) {
  const { AE, G, P, cal } = kit, cb = cal.charm;
  const view = G.backView(cb, { res: 6, upAngle: 75 }), cx = view.cx, cy = view.cy;   // (turned 15°: the turn must be the view's own)
  const toItem = subs => subs.map(sp => sp.map(o => (o[0] === 'h' ? ['h'] : [o[0]].concat(o.slice(1).map(p => [p[0], -p[1]])))));   // (the editor's frame: the view's, y down)
  const rect = (x, y, w, h) => [[['m', [x, y]], ['l', [x + w, y]], ['l', [x + w, y + h]], ['l', [x, y + h]], ['h']]];
  const ring = (x, y, r) => [[...Array(24).keys()].map(i => [i ? 'l' : 'm', [x + r * Math.cos(i / 24 * 2 * Math.PI), y + r * Math.sin(i / 24 * 2 * Math.PI)]]).concat([['h']])];
  // two hatched squares that overlap, a red open line, a small closed ring (a line), and a frame: the outline's own line drawn again 0.95 the size about its centre
  const sqA = rect(cx - 12, cy + 4, 8, 6), sqB = rect(cx - 8, cy + 6, 8, 6), ln = [[['m', [cx + 2, cy - 14]], ['l', [cx + 14, cy - 14]]]], rg = ring(cx + 10, cy + 10, 3);
  const frameSubs = view.outline.subpaths.map(sp => sp.map(o => (o[0] === 'h' ? ['h'] : [o[0]].concat(o.slice(1).map(p => [cx + 0.95 * (p[0] - cx), cy + 0.95 * (p[1] - cy)])))));
  const items = [{ id: 'sqA', kind: 'path', role: 'hatch', subpaths: toItem(sqA) }, { id: 'sqB', kind: 'path', role: 'hatch', subpaths: toItem(sqB) }, { id: 'ln', kind: 'path', role: 'line', lwPt: 0.3, subpaths: toItem(ln) }, { id: 'rg', kind: 'path', role: 'line', subpaths: toItem(rg) }];
  const rec = AE.encode({ added: items }, { charm: cb, view, face: 'back' }), c2 = AE.apply(cb, Object.assign({ at: 1 }, rec), 'back');
  const recF = AE.encode({ added: [{ id: 'fr', kind: 'path', role: 'line', subpaths: toItem(frameSubs) }] }, { charm: cb, view, face: 'back' }), cF = AE.apply(cb, Object.assign({ at: 2 }, recF), 'back');
  check('A0 the parts typed on a back are the piece\'s artBack (in its own points), the members its master\'s', (c2.artBack || []).length === 4 && c2.members === cb.members && c2.artBack.filter(s => s.hatchBlue && s.fill).length === 2 && (cF.artBack || []).length === 1,
    JSON.stringify({ n: (c2.artBack || []).length, roles: (c2.artBack || []).map(s => s.artRole) }));

  const mk = (o, avoid) => G.engraveMask(view, Object.assign({ marginMm: 0.8, keepOut: [] }, o || {}, avoid ? { avoid } : {}));
  const m0 = mk(), m1 = mk(null, c2.artBack), at = (m, x, y) => G.at(m, x, y);
  check('A1 a filled part keeps its whole inside clear (and the margin round it): the words may never go there; they could before', at(m0, cx - 10, cy + 5) === 1 && at(m1, cx - 10, cy + 5) === 0 && at(m1, cx - 10, cy + 4 - 1.5) === 0 && at(m1, cx - 10, cy + 4 - 3.5 * PT) === 1,
    JSON.stringify([at(m0, cx - 10, cy + 5), at(m1, cx - 10, cy + 5), at(m1, cx - 10, cy + 2.5), at(m1, cx - 10, cy + 4 - 3.5 * PT)]));
  // (with a margin small enough not to close it, the keep-outs' one even-odd fill opens the overlap of two parts; each part cleared on its own never does)
  const thin = { marginMm: 0.1 }, evenOdd = G.engraveMask(view, { marginMm: 0.1, keepOut: c2.artBack.slice(0, 2) }), own = mk(thin, c2.artBack.slice(0, 2));
  check('A2 two parts that overlap are both kept clear where they overlap (the keep-out layers\' even-odd fill would open a hole there)', at(evenOdd, cx - 6, cy + 8) === 1 && at(own, cx - 6, cy + 8) === 0 && at(own, cx - 10, cy + 5) === 0 && at(own, cx - 2, cy + 11) === 0,
    JSON.stringify([at(evenOdd, cx - 6, cy + 8), at(own, cx - 6, cy + 8)]));
  check('A3 an open line keeps a band along itself clear, the metal beside it stays free; a small closed ring keeps its inside clear', at(m0, cx + 8, cy - 14) === 1 && at(m1, cx + 8, cy - 14) === 0 && at(m1, cx + 8, cy - 14 - 4 * PT) === 1 && at(m1, cx + 10, cy + 10) === 0 && at(m0, cx + 10, cy + 10) === 1,
    JSON.stringify([at(m1, cx + 8, cy - 14), at(m1, cx + 8, cy - 14 - 4 * PT), at(m1, cx + 10, cy + 10)]));
  const mF = mk(null, cF.artBack), onLine = frameSubs[0][1][1];
  let lineTook = 0; for (let i = 0; i < m0.bits.length; i++) if (m0.bits[i] && !mF.bits[i]) lineTook++;
  check('A4 a frame drawn round the whole back keeps only its own line clear (a band along it is taken), the middle stays free for the words', at(mF, cx, cy) === 1 && at(m0, cx, cy) === 1 && at(mF, onLine[0], onLine[1]) === 0 && lineTook > 0 && lineTook < 0.5 * G.area(m0),
    JSON.stringify({ middle: at(mF, cx, cy), took: lineTook, of: G.area(m0) }));
  let gained = 0, far = 0; const boxes = c2.artBack.map(s => G.transformSeg(G.transformSeg(s, view.M), view.R).bbox), pad = 0.8 * PT + 0.5;
  for (let py = 0; py < m0.h; py++) for (let px = 0; px < m0.w; px++) { const i = py * m0.w + px, a = m0.bits[i], b = m1.bits[i]; if (b && !a) gained++; if (a && !b) { const x = m0.ox + (px + 0.5) / m0.res, y = m0.oy + (py + 0.5) / m0.res; if (!boxes.some(q => x > q[0] - pad && x < q[2] + pad && y > q[1] - pad && y < q[3] + pad)) far++; } }
  const same = o => { const a = G.engraveMask(view, Object.assign({ marginMm: 0.8, keepOut: [] }, o)); return a.w === m0.w && a.h === m0.h && a.bits.every((v, i) => v === m0.bits[i]); };
  check('A5 nothing else changes: no pixel is freed, none taken away farther than the margin from a part; a back given no parts (none, or an empty list) is the mask it always was', gained === 0 && far === 0 && same({}) && same({ avoid: [] }), JSON.stringify({ gained, far }));

  // the file: buildBackFile writes the parts where the view draws them, plus the page's own offset (frame.T)
  const built = await P.buildBackFile({ charm: c2, parsed: cal.parsed, cutMembers: view.cutMembers, cx, cy, angleDeg: view.angleDeg, padPt: 5 * PT, glyphs: [], artBack: c2.artBack });
  const written = await engravedOf(P, built.bytes), T = built.frame.T;
  const expect = c2.artBack.map(s => { const b = G.transformSeg(G.transformSeg(s, view.M), view.R).bbox; return { fill: !!s.fill, bbox: [b[0] + T[4], b[1] + T[5], b[2] + T[4], b[3] + T[5]] }; });
  const matched = expect.map(e => written.find(w => w.fill === e.fill && near(w.bbox, e.bbox, 0.01)));
  check('A6 buildBackFile writes every part at the view\'s place plus the page\'s offset, to 0.01 pt (fills filled, lines stroked)', written.length === 4 && matched.every(Boolean),
    JSON.stringify({ written: written.length, worst: Math.max(...expect.map((e, i) => (matched[i] ? worst(e.bbox, matched[i].bbox) : 99))) }));

  // the bridge's own renderBack, its source run with a canvas that records every call (engrave-front-art.cjs's way)
  let ids = 0; const made = new Map();
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
        return (...a) => { cv.ops.push([k, a.map(v => (v && Array.isArray(v.ops) ? { canvas: v.id } : v)), Object.assign({}, state)]); };
      },
      set: (_, k, v) => { state[k] = v; cv.ops.push(['set', k, v]); return true; }
    });
    cv.getContext = () => ctx; made.set(cv.id, cv); return cv;
  };
  const src = fs.readFileSync(path.join(ROOT, 'charm-nest-bridge.js'), 'utf8');
  const cut = (a, b) => { const i = src.indexOf(a), j = src.indexOf(b, i); assert(i > 0 && j > i, `bridge source markers: ${a}`); return src.slice(i, j); };
  const W = { sessionStorage: { getItem: () => null, setItem: () => {} } };
  const envOf = extra => { const env = vm.createContext(Object.assign({ G, P, PT, MM, Math, Array, Infinity, document: { createElement: () => makeCanvas() }, console, window: W }, extra || {})); vm.runInContext(cut('  function textBox(glyphs', '  function resize(job, size)') + cut('  /* ── 7.6 · back files', '  function renderFront(') + '\nthis.renderBack = renderBack; this.FRONT_ART_ALPHA = FRONT_ART_ALPHA; this.FRONT_INK = FRONT_INK;', env); return env; };
  const withArt = envOf({ charmFor: job => job.__charm }), plain = envOf();
  const job = { view, mask: m1, fit: null, __charm: c2 };
  const opsOf = cv => JSON.stringify(cv.ops), steps = cv => cv.ops.map(o => o[0]);
  const ed = withArt.renderBack(job, [400, 400], { grid: true, editable: true });
  const lays = ed.ops.filter(o => o[0] === 'drawImage' && o[2].globalAlpha === withArt.FRONT_ART_ALPHA.front).map(o => made.get(o[1][0].canvas));
  const lay = lays.find(c => c.ops.some(o => o[0] === 'fill')), fills = lay ? lay.ops.filter(o => o[0] === 'fill') : [], strokes = lay ? lay.ops.filter(o => o[0] === 'stroke') : [];
  const layAt = ed.ops.findIndex(o => o[0] === 'drawImage' && lay && o[1][0].canvas === lay.id), cutAt = ed.ops.findIndex((o, i) => i > layAt && o[0] === 'stroke' && o[2].strokeStyle === '#000');
  const first = lay && lay.ops.find(o => o[0] === 'moveTo'), want = ed._map.tx(...G.transformSeg(G.transformSeg(c2.artBack[0], view.M), view.R).subpaths[0][0][1]);
  check('A7 the work area (editable) draws the back\'s parts as a front draws its art: a layer laid on at .5, hatched parts in the hatch blue, lines in the line red, under the cut line, through the view\'s own frame',
    ed._backArt.length === 4 && !!lay && fills.length === 2 && fills.every(o => o[2].fillStyle === withArt.FRONT_INK.hatch) && strokes.length === 2 && strokes.every(o => o[2].strokeStyle === withArt.FRONT_INK.line) && layAt >= 0 && cutAt > layAt && near(first[1], want, 1e-6),
    JSON.stringify({ art: ed._backArt.length, fills: fills.length, strokes: strokes.length, layAt, cutAt, first: first && first[1], want }));
  const pic = withArt.renderBack(job, 500, { grid: false, hatch: false }), inked = pic.ops.filter(o => (o[0] === 'fill' && o[2].fillStyle === '#111') || (o[0] === 'stroke' && o[2].strokeStyle === '#111'));
  const inkAt = pic.ops.findIndex(o => o[0] === 'fill' && o[2].fillStyle === '#111'), pCut = pic.ops.findIndex((o, i) => i > inkAt && o[0] === 'stroke' && o[2].strokeStyle === '#000');
  check('A8 the pictures written with the piece (not editable: the approval .png, the Approved card) draw them in the words\' ink, #111, under the cut line; no muted layer', inked.length === 4 && inkAt >= 0 && pCut > inkAt && !pic.ops.some(o => o[0] === 'drawImage' && o[2].globalAlpha === 0.5),
    JSON.stringify({ inked: inked.length, inkAt, pCut }));
  const noArt = { view, mask: m0, fit: null, __charm: cb };
  const same2 = [[[400, 400], { grid: true, editable: true }], [500, { grid: false, hatch: false }], [360, { hatch: false }]].map(([px, o]) => { const a = withArt.renderBack(noArt, px, o), b = plain.renderBack(noArt, px, o); return a._backArt.length === 0 && opsOf(a).replace(/"canvas":\d+/g, '') === opsOf(b).replace(/"canvas":\d+/g, '') && steps(a).length === steps(b).length; });
  check('A9 a back with nothing added makes exactly the calls it made before, in the work area and in every written picture', same2.every(Boolean), JSON.stringify(same2));
}

/* ═══ B · the Engraving window ═══ */
const settleP = page => page.evaluate(() => new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r))));
async function typeField(page, f, value) { const el = page.locator(`.ce-root [data-f="${f}"]`); await el.click(); await page.keyboard.press('Control+a'); await page.keyboard.type(String(value)); await page.keyboard.press('Enter'); await settleP(page); }
const C = (page, fn, arg) => page.evaluate(fn, arg);
const clientOf = (page, x, y) => C(page, ([x, y]) => window.CharmEditor.current.clientOf(x, y), [x, y]);
const findText = (page, t) => C(page, t => { let f = null; const walk = l => l.forEach(i => { if (i.kind === 'text' && i.text === t) f = i; if (i.children) walk(i.children); }); walk(window.CharmEditor.current.items()); return f; }, t);

async function partB(kit) {
  const R = require('./_engraveRealWorld.cjs');
  const shots = process.env.SHOTS || path.join(os.tmpdir(), 'charm-editor-back-shots');
  const c = kit.cal.charm;
  R.DESIGNS.CALENDAR = { file: 'CALENDAR.ai', make: () => Buffer.from(kit.cal.bytes), entry: { sku: 'CALENDAR', widthPt: +(c.bbox[2] - c.bbox[0]).toFixed(3), heightPt: +(c.bbox[3] - c.bbox[1]).toFixed(3), areaPt2: 4200, members: c.members.length, holes: 0, engravable: true, engravableBy: 'index', upAngle: 90, upSource: 'drawn', charmHash: 'calhash01', backKeepOut: [] } };
  R.ORDERS.cal = Object.assign({}, R.ORDERS.both, { rid: '4290000141', sku: 'CALENDAR', title: 'Calendar Necklace, Personalized Month Calendar Charm', buyer: 'Name Eleven', personalization: ['front - W\nback - Name One'] });
  R.ORDERS.cal2 = Object.assign({}, R.ORDERS.back, { rid: '4290000142', sku: 'CALENDAR', listing: '1890000142', title: 'Calendar Necklace, Personalized Month Calendar Charm', buyer: 'Name Twelve', personalization: ['Name Two'] });
  R.EXPECT_JOBS.cal = 2; R.EXPECT_JOBS.cal2 = 1;
  const w = await R.world({ shots });
  if (!w) { console.log('SKIP no playwright-core'); return; }
  const page = w.page, st = w.srv.st, Q = '#egQueue .rvItem[data-kind=placement]';
  const artPuts = () => st.calls.filter(x => x.name === 'charmNestLibrary' && x.op === 'artPut').length;
  try {
    const made = await R.build(w, ['cal', 'cal2']);
    const PID = made.cal.poolIds[0], PID2 = made.cal2.poolIds[0];
    check('B0 the world: the calendar is pooled and its front and back are fitted (and another calendar order, back only)', made.cal.jobs.length === 2 && made.cal.jobs.every(j => j.fitted) && made.cal2.jobs.length === 1 && made.cal2.jobs[0].fitted, JSON.stringify(made.cal.jobs.map(j => [j.face, j.state])));
    await page.evaluate(() => {
      const fnv = d => { let h = 0x811c9dc5; for (let i = 0; i < d.length; i++) { h ^= d.charCodeAt(i); h = Math.imul(h, 0x01000193) >>> 0; } return h.toString(16); };
      window.__cb = {
        card: () => document.querySelector('#egQueue .rvItem[data-kind=placement]'),
        cv: () => document.querySelector('#egQueue .rvItem[data-kind=placement] .backHost canvas'),
        job: () => Engrave.items().get(document.querySelector('#egQueue .rvItem[data-kind=placement]').dataset.key),
        backJob: id => [...Engrave.items().values()].find(x => x.face !== 'front' && (x.copies || []).includes(id)),
        frontJob: id => [...Engrave.items().values()].find(x => x.face === 'front' && (x.copies || []).includes(id)),
        thumbCv: face => document.querySelector(`#egQueue .rvItem[data-kind=placement] .engCard .engFace[data-face="${face}"] .engThumb canvas`),
        thumb: face => { const t = __cb.thumbCv(face); return t ? fnv(t.toDataURL()) + ':' + t.width : ''; },
        /** the blue (hatch) pixels of a canvas inside a box of its pixels (all of it when none): { n, box } */
        blue(cv, box) {
          const g = cv.getContext('2d'), d = g.getImageData(0, 0, cv.width, cv.height).data; let n = 0, b = null;
          const [x0, y0, x1, y1] = box ? [Math.max(0, Math.floor(box[0])), Math.max(0, Math.floor(box[1])), Math.min(cv.width - 1, Math.ceil(box[2])), Math.min(cv.height - 1, Math.ceil(box[3]))] : [0, 0, cv.width - 1, cv.height - 1];
          for (let y = y0; y <= y1; y++) for (let x = x0; x <= x1; x++) { const i = (y * cv.width + x) * 4, r = d[i], gg = d[i + 1], bl = d[i + 2]; if (bl > 150 && bl - r > 45 && bl - gg > 30) { n++; b = b ? [Math.min(b[0], x), Math.min(b[1], y), Math.max(b[2], x), Math.max(b[3], y)] : [x, y, x, y]; } }
          return { n, box: b };
        },
        /** the hatch blue laid on at .5 (not the words' box, a deeper blue line) inside a box of a canvas's pixels */
        hatch(cv, box) { const g = cv.getContext('2d'), d = g.getImageData(0, 0, cv.width, cv.height).data; let n = 0; for (let y = Math.max(0, Math.floor(box[1])); y <= Math.min(cv.height - 1, Math.ceil(box[3])); y++) for (let x = Math.max(0, Math.floor(box[0])); x <= Math.min(cv.width - 1, Math.ceil(box[2])); x++) { const i = (y * cv.width + x) * 4, r = d[i], gg = d[i + 1], bl = d[i + 2]; if (bl > 150 && bl - r > 45 && bl - gg > 30 && r > 90) n++; } return n; },
        /** the near-black pixels of a canvas inside a box of its pixels */
        dark(cv, box) { const g = cv.getContext('2d'), d = g.getImageData(0, 0, cv.width, cv.height).data; let n = 0; for (let y = Math.max(0, Math.floor(box[1])); y <= Math.min(cv.height - 1, Math.ceil(box[3])); y++) for (let x = Math.max(0, Math.floor(box[0])); x <= Math.min(cv.width - 1, Math.ceil(box[2])); x++) { const i = (y * cv.width + x) * 4; if (d[i] < 60 && d[i + 1] < 60 && d[i + 2] < 60 && d[i + 3] > 200) n++; } return n; },
        /** a box of points (view frame, y up) as a box of the canvas's pixels */
        pxBox(cv, b) { const p = cv._map.tx(b[0], b[3]), q = cv._map.tx(b[2], b[1]); return [p[0], p[1], q[0], q[1]]; },
        /** the words' outlines as points along them (each curve sampled), in the view's frame */
        glyphPts: fit => { const out = []; for (const g of fit.glyphs || []) { let cur = null; for (const c of g.cmds) { if (c.type === 'Z') continue; if ((c.type === 'C' || c.type === 'Q') && cur) for (let t = 0.125; t < 1; t += 0.125) { const u = 1 - t; out.push(c.type === 'C' ? [u * u * u * cur[0] + 3 * u * u * t * c.x1 + 3 * u * t * t * c.x2 + t * t * t * c.x, u * u * u * cur[1] + 3 * u * u * t * c.y1 + 3 * u * t * t * c.y2 + t * t * t * c.y] : [u * u * cur[0] + 2 * u * t * c.x1 + t * t * c.x, u * u * cur[1] + 2 * u * t * c.y1 + t * t * c.y]); } out.push([c.x, c.y]); cur = [c.x, c.y]; } } return out; },
        union: bs => bs.reduce((a, b) => [Math.min(a[0], b[0]), Math.min(a[1], b[1]), Math.max(a[2], b[2]), Math.max(a[3], b[3])])
      };
    });
    // the sheets as the files are written to (the approval writes the back's file and picture into the sheet's folder)
    const sheets = await page.evaluate(() => { const out = []; for (const s of allSheets()) if (s.charms && s.charms.length) { if (!s.folderPath) s.folderPath = 'charmnest/sheets/' + s.sheetId; out.push({ sheetId: s.sheetId, poolIds: s.charms.map(x => x.poolId) }); } return out; });
    for (const s of sheets) st.put('Charm_Nest_Sheets', s.sheetId, { sheetId: s.sheetId, poolIds: s.poolIds, backPool: [] });

    /* ── the back approved first, as a person approves it ── */
    await R.openEngraveDetail(w, 'cal', { face: 'back' }); await R.settle(page, 700);
    await page.click(Q + ' [data-a="approve"]');
    for (let t0 = Date.now(); Date.now() - t0 < 120000 && !st.doc('Charm_Pool_Back', PID); ) await new Promise(r => setTimeout(r, 300));
    await R.openEngraveDetail(w, 'cal', { face: 'back' }); await R.settle(page, 900);
    const b0 = await page.evaluate(() => { const j = __cb.job(), cv = __cb.cv(); return { face: j.face || 'back', state: j.state, approvedAt: j.approvedAt, art: cv._backArt.length, blue: __cb.blue(cv._map.outline).n, fit: JSON.stringify([j.fit.size, j.fit.centre, j.fit.angle]), words: __cb.union(j.fit.glyphs.flatMap(g => g.cmds.filter(c => c.type !== 'Z').map(c => [c.x, c.y, c.x, c.y]))) }; });
    const thumb0 = await page.evaluate(() => ({ h: __cb.thumb('back'), blue: __cb.thumbCv('back') ? __cb.blue(__cb.thumbCv('back')).n : -1 }));
    await R.shot(w, 'CB-1-back-approved');
    check('B1 the calendar\'s back is approved (its record saved); its work area and its Back thumbnail have no blue art (the back shows only its cut line and words)', !!st.doc('Charm_Pool_Back', PID) && ['approved', 'written'].includes(b0.state) && b0.art === 0 && b0.blue === 0 && thumb0.blue === 0 && !!thumb0.h, JSON.stringify(Object.assign({}, b0, { words: undefined, thumb0 })));

    /* ── the editor opens on the back; "HI" is typed right where the back's words stand ── */
    await page.click(Q + ' .backHost .egCharmEdit');
    await page.waitForSelector('.ce-root .ce-items path', { timeout: 60000 }); await settleP(page); await settleP(page);
    const o0 = await page.evaluate(() => { const E = CharmEditor.current, its = E.items(); return { face: E.face(), n: its.length, roles: its.map(i => i.role), locked: its.filter(i => i.locked).length, frame: E.frame() }; });
    check('B2 Edit charm on the back opens the editor on the back: its cut line, locked (the back takes additions only)', o0.face === 'back' && o0.n === 1 && o0.roles[0] === 'cut' && o0.locked === 1, JSON.stringify(o0));
    const wb = b0.words, mid = [(wb[0] + wb[2]) / 2, -(wb[1] + wb[3]) / 2];   // (the words' middle in the editor's frame: the view's, y down)
    await page.keyboard.press('t');
    const at0 = await clientOf(page, mid[0] - 3, mid[1] + 2);
    await page.mouse.click(at0.x, at0.y); await settleP(page);
    await page.keyboard.type('HI'); await page.keyboard.press('Enter'); await settleP(page);
    await typeField(page, 'pt', '9');
    const hi0 = await findText(page, 'HI');
    const fr = o0.frame, wmm = parseFloat(await page.locator('.ce-root [data-f="w"]').inputValue()), hmm = parseFloat(await page.locator('.ce-root [data-f="h"]').inputValue());
    await typeField(page, 'x', (((mid[0] - fr.x0) / PT) - wmm / 2).toFixed(3));
    await typeField(page, 'y', (((mid[1] - fr.y0) / PT) - hmm / 2).toFixed(3));
    const hiBox = await C(page, id => CharmEditor.current.box(id), hi0.id);
    await R.shot(w, 'CB-2-editor-back-HI');
    check('B3 "HI" is typed on the back (9 pt, outlines from the page\'s own font) and centred on the back\'s words with X and Y', !!hi0 && hi0.sizePt === 9 && Math.abs((hiBox[0] + hiBox[2]) / 2 - mid[0]) < 0.05 && Math.abs((hiBox[1] + hiBox[3]) / 2 - mid[1]) < 0.05,
      JSON.stringify({ hiBox, mid }));
    const res = await page.evaluate(() => CharmEditor.current.result());
    check('B4 what the editor hands back: HI added on the back, nothing removed or changed, the front untouched', res.changes.back.added.length === 1 && res.changes.back.added[0].text === 'HI' && !res.changes.back.removed.length && !Object.keys(res.changes.back.changed).length && !res.changes.front.added.length && !res.changes.front.removed.length,
      JSON.stringify({ back: res.changes.back.added.map(a => a.text) }));

    /* ── Save ── */
    const puts0 = artPuts(), frontFit0 = await page.evaluate(id => { const f = __cb.frontJob(id); return JSON.stringify([f.state, f.fit.size, f.fit.centre, f.fit.angle]); }, PID);
    await page.click('.ce-root [data-act="save"]');
    for (let t0 = Date.now(); Date.now() - t0 < 60000 && !st.doc('Charm_Pool_Art', PID); ) await new Promise(r => setTimeout(r, 200));
    await page.waitForFunction(id => { const b = __cb.backJob(id); return b && b.view && b.fit && !['ready', 'fitting'].includes(b.state); }, PID, { timeout: 180000 }).catch(() => {});
    await R.openEngraveDetail(w, 'cal', { face: 'back' }); await R.settle(page, 1200);
    const rec = st.doc('Charm_Pool_Art', PID);
    check('B5 the cloud keeps it for this piece\'s BACK (Charm_Pool_Art <poolId>, no "~front"): who, HI added as blue-hatched outlines in the piece\'s own points; one save',
      !!rec && rec.face === 'back' && rec.by === 'Test Operator' && rec.added.length >= 1 && rec.added.every(a => a.role === 'hatch' && a.fill && a.fill.join() === '0,0,1' && /^M/.test(a.d)) && rec.added.some(a => a.text === 'HI') && !rec.removed.length && artPuts() === puts0 + 1 && !st.doc('Charm_Pool_Art', PID + '~front'),
      JSON.stringify(rec && { face: rec.face, by: rec.by, added: rec.added.map(a => [a.id, a.role, a.text, a.fill]) }));
    const after = await page.evaluate(([id, id2, ids]) => {
      const ch = Pool.charmOf(id), j = __cb.job(), cv = __cb.cv(), art = cv._backArt, ab = art.length ? __cb.union(art.map(s => s.bbox)) : null, px = ab ? __cb.pxBox(cv, ab) : null;
      const blue = __cb.blue(cv._map.outline, px ? [px[0] - 4, px[1] - 4, px[2] + 4, px[3] + 4] : null), all = __cb.blue(cv._map.outline);
      return { hash: ch.hash, artBack: (ch.artBack || []).map(s => s.artMemberId), ids, members: ch.members.length, other: Pool.charmOf(id2).artBack ? 1 : 0, face: j.face || 'back', state: j.state, approvedAt: j.approvedAt, fitted: !!j.fit,
        art: art.length, artBox: ab, px, blueIn: blue.n, blueAll: all.n, blueBox: blue.box, note: (__cb.card().querySelector('.backHost .egArtNote') || {}).textContent || '', row: (B.pool.rows.get(id) || {}).artEdit };
    }, [PID, PID2, rec ? rec.added.map(a => a.id) : []]);
    await R.shot(w, 'CB-3-back-work-area-HI');
    check('B6 the piece\'s charm carries HI on its back (artBack, in the record\'s ids; its members the master\'s), with a hash of its own; the other calendar has none',
      after.artBack.length === after.ids.length && after.ids.every(i => after.artBack.includes(i)) && after.members === c.members.length && /~ae[0-9a-z]+$/.test(after.hash) && after.other === 0 && after.row && after.row.back === rec.at, JSON.stringify(after));
    check('B7 the back\'s work area draws HI at once, in the hatch blue, exactly where the view puts it: the blue pixels\' box is its box on the canvas (±2 px), and there is no other blue',
      after.art === after.ids.length && after.blueIn > 30 && after.blueAll === after.blueIn && near(after.blueBox, after.px.map((v, i) => (i < 2 ? Math.round(v) : Math.round(v) - 1)), 2.5),
      JSON.stringify({ art: after.art, blueIn: after.blueIn, blueAll: after.blueAll, blueBox: after.blueBox, px: after.px && after.px.map(v => +v.toFixed(1)) }));
    // (with "Show front" on, the back is drawn over the front's art seen through it: HI is still there, over it)
    const peekOf = () => page.evaluate(() => { const cv = __cb.cv(), px = __cb.pxBox(cv, __cb.union(cv._backArt.map(s => s.bbox))); return { show: !!cv._showFront, n: __cb.hatch(cv, px) }; });
    const off0 = await peekOf();
    await page.click(Q + ' .backHost .egFrontPeek'); await R.settle(page, 300);
    const on1 = await peekOf();
    await R.shot(w, 'CB-3b-back-show-front');
    await page.click(Q + ' .backHost .egFrontPeek'); await R.settle(page, 300);
    check('B7b with "Show front" on, the back still draws HI (in the same blue, over the front\'s art seen through it)', !off0.show && on1.show && off0.n > 30 && Math.abs(on1.n - off0.n) <= 0.2 * off0.n, JSON.stringify({ off0, on1 }));
    const fb = st.doc('Charm_Pool_Back', PID);
    check('B8 the approved back is withdrawn in Engrave.artChanged\'s words: its record invalidated, the face fitted again and waiting for Approve, and the work area\'s note says so',
      fb && fb.invalidated === true && after.approvedAt == null && after.state === 'review' && after.fitted && after.note.includes("This charm's back changed since the words were approved — fit the words again") && /^Back artwork saved for this piece/.test(after.note),
      JSON.stringify({ invalidated: fb && fb.invalidated, state: after.state, note: after.note }));
    const clear = await page.evaluate(([id, oldWords]) => {
      const j = __cb.backJob(id), G = CharmNestGeom, ch = Pool.charmOf(id), art = ch.artBack.map(s => G.transformSeg(G.transformSeg(s, j.view.M), j.view.R));
      const polys = art.flatMap(s => G.flatten(s, 12)), pts = __cb.glyphPts(j.fit), inside = pts.filter(p => polys.some(pg => G.pointInPolys(p[0], p[1], [pg]))).length;
      let gap = Infinity; for (const p of pts) gap = Math.min(gap, G.distToPolys(p[0], p[1], polys));
      const artBox = __cb.union(art.map(s => s.bbox)), words = __cb.union(pts.map(p => [p[0], p[1], p[0], p[1]]));
      const overlapBefore = !(oldWords[2] < artBox[0] || oldWords[0] > artBox[2] || oldWords[3] < artBox[1] || oldWords[1] > artBox[3]);
      const maskOff = art.every(s => { const b = s.bbox; return G.at(j.mask, (b[0] + b[2]) / 2, (b[1] + b[3]) / 2) === 0; });
      return { inside, gapMm: gap * 25.4 / 72, marginMm: j.mask.marginMm, overlapBefore, maskOff, ink: G.verifyInk(j.fit.cmds, j.mask).ok, words, artBox };
    }, [PID, b0.words]);
    check(`B9 the back's words are fitted again clear of HI: none of their points inside it, ${clear.gapMm.toFixed(2)} mm from it at the closest (at least the engraving margin, ${clear.marginMm} mm), the mask has no room on it; HI stood where the words had been`,
      clear.inside === 0 && clear.marginMm > 0 && clear.gapMm >= clear.marginMm - 0.02 && clear.maskOff && clear.ink && clear.overlapBefore, JSON.stringify(clear));
    const thumb1 = await page.evaluate(id => { const t = __cb.thumbCv('back'), F = CharmNestEngravePreview.frameFor({ job: __cb.backJob(id), face: 'back' }); return { h: __cb.thumb('back'), blue: t ? __cb.blue(t).n : -1, frameArt: F ? F.art.length : -1 }; }, PID);
    check('B10 the card\'s Back thumbnail is drawn again with HI on it (the preview\'s frame of the back carries the parts, drawn in the hatch blue)', thumb1.h && thumb1.h !== thumb0.h && thumb1.blue > 10 && thumb1.frameArt === after.ids.length, JSON.stringify({ thumb0, thumb1 }));
    const fr1 = await page.evaluate(id => { const f = __cb.frontJob(id); return JSON.stringify([f.state, f.fit.size, f.fit.centre, f.fit.angle]); }, PID);
    check('B11 the front is untouched: its words and state as they were, no front record', fr1 === frontFit0 && !st.doc('Charm_Pool_Art', PID + '~front'), fr1 + ' vs ' + frontFit0);

    /* ── the approval picture, and the back approved again: the written file carries HI where the screen shows it ── */
    const pic = await page.evaluate(() => { const j = __cb.job(), cv = Engrave.renderBack(j, 500, { grid: false, hatch: false }), ab = __cb.union(cv._backArt.map(s => s.bbox)), px = __cb.pxBox(cv, ab); return { art: cv._backArt.length, dark: __cb.dark(cv, px), blue: __cb.blue(cv).n, area: (px[2] - px[0]) * (px[3] - px[1]) }; });
    await page.evaluate(() => { window.__backFiles = []; const P = CharmNestPDF, orig = P.__origBack || (P.__origBack = P.buildBackFile); P.buildBackFile = async spec => { const r = await orig(spec); window.__backFiles.push({ face: spec.face || 'back', artBack: (spec.artBack || []).length, T: r.frame.T, view: spec.view }); return r; }; });
    await R.openEngraveDetail(w, 'cal', { face: 'back' }); await R.settle(page, 700);
    const viewArt = await page.evaluate(() => __cb.cv()._backArt.map(s => ({ fill: !!s.fill, bbox: s.bbox })));
    const screenPx = await page.evaluate(() => { const cv = __cb.cv(); return cv._backArt.map(s => __cb.pxBox(cv, s.bbox)); });
    await page.click(Q + ' [data-a="approve"]');
    let fb2 = null;
    for (let t0 = Date.now(); Date.now() - t0 < 120000; ) { fb2 = st.doc('Charm_Pool_Back', PID); if (fb2 && !fb2.invalidated && fb2.outputs && fb2.outputs.ai && fb2.outputs.png && fb2.approvedAt !== (fb && fb.approvedAt)) break; await new Promise(r => setTimeout(r, 300)); }
    await R.settle(page, 600);
    const files = await page.evaluate(() => window.__backFiles);
    const aiBlob = fb2 && fb2.outputs && fb2.outputs.ai && st.blobs.get(fb2.outputs.ai.path), pngBlob = fb2 && fb2.outputs && fb2.outputs.png && st.blobs.get(fb2.outputs.png.path);
    const wrote = files.filter(f => f.face === 'back');
    const T = wrote.length ? wrote[wrote.length - 1].T : null, inFile = aiBlob ? await engravedOf(kit.P, new Uint8Array(aiBlob.buf)) : [];
    const pairs = viewArt.map(a => { const e = [a.bbox[0] + T[4], a.bbox[1] + T[5], a.bbox[2] + T[4], a.bbox[3] + T[5]]; const hit = inFile.find(f => f.fill === a.fill && near(f.bbox, e, 0.02)); return { view: a.bbox.map(v => +v.toFixed(3)), file: hit ? hit.bbox.map(v => +v.toFixed(3)) : null, off: hit ? +worst(e, hit.bbox).toFixed(4) : null }; });
    check('B12 approved again: the back\'s file is built with HI (artBack) at approval and when written, re-verifies, and is recorded (not invalidated)', !!fb2 && !fb2.invalidated && fb2.verified && fb2.verified.file && fb2.verified.file.ok === true && wrote.length >= 2 && wrote.every(f => f.artBack === viewArt.length) && !!aiBlob,
      JSON.stringify({ rec: !!fb2, inv: fb2 && fb2.invalidated, verified: fb2 && fb2.verified && fb2.verified.file, wrote }));
    check('B13 the written back file engraves every part of HI at the very place the work area draws it: its box in the file is its box on the screen plus the page\'s offset (to 0.02 pt), and on the canvas it is drawn at that box',
      pairs.length === viewArt.length && pairs.length > 0 && pairs.every(p => p.file && p.off <= 0.02) && screenPx.every(b => b[2] > b[0] && b[3] > b[1]),
      JSON.stringify({ T, pairs: pairs.slice(0, 3), n: pairs.length, inFile: inFile.length, screenPx: screenPx.slice(0, 2).map(b => b.map(v => +v.toFixed(1))) }));
    // (an added part may stand nearer the cut edge than the words' margin: it was checked on the metal when it was saved; the file's re-check is the words' own clearance and leaves it out)
    const edge = await page.evaluate(async id => {
      const j = __cb.backJob(id), live = Pool.charmOf(id); if (!j || !j.view || !j.fit) return { shelved: true };
      const b = live.outline.bbox, my = (b[1] + b[3]) / 2, x0 = b[0] + 0.4, sq = [[['m', [x0, my - 1]], ['l', [x0 + 2, my - 1]], ['l', [x0 + 2, my + 1]], ['l', [x0, my + 1]], ['h']]];
      const part = { kind: 'path', subpaths: sq, bbox: [x0, my - 1, x0 + 2, my + 1], closed: true, fill: true, stroke: false, fillRGB: [0, 0, 1], strokeRGB: [0, 0, 0], hatchBlue: true, paintOp: 'f', artRole: 'hatch', artAdded: true, artMemberId: 'edge1', synthetic: true };
      const withEdge = Object.assign({}, live, { artBack: live.artBack.concat([part]) }), view = j.view;
      const rel = j.fit.glyphs.map(g => ({ cmds: g.cmds.map(c => { const o = { type: c.type }; if (c.type !== 'Z') { o.x = c.x - view.cx; o.y = c.y - view.cy; } if (c.type === 'C' || c.type === 'Q') { o.x1 = c.x1 - view.cx; o.y1 = c.y1 - view.cy; } if (c.type === 'C') { o.x2 = c.x2 - view.cx; o.y2 = c.y2 - view.cy; } return o; }) }));
      const built = await CharmNestPDF.buildBackFile({ charm: withEdge, parsed: (sourceOf(live.sourceId) || {}).parsed, cutMembers: view.cutMembers, cx: view.cx, cy: view.cy, angleDeg: view.angleDeg, padPt: 5 * 72 / 25.4, glyphs: rel, artBack: withEdge.artBack, view: 'asSeenFromBack' });
      const known = await Engrave.verifyBackFile(built.bytes, Object.assign({}, j, { editCharm: withEdge })), unknown = await Engrave.verifyBackFile(built.bytes, Object.assign({}, j, { editCharm: Object.assign({}, live, { artBack: undefined }) }));
      return { known: { ok: known.ok, why: known.why }, unknown: { ok: unknown.ok, why: unknown.why }, gapMm: +((x0 - b[0]) * 25.4 / 72).toFixed(2) };
    }, PID);
    check('B13b a part added nearer the cut edge than the words\' margin does not fail the written file\'s re-check (the clearance checked there is the words\'): read as a word, it would', edge.known && edge.known.ok === true && edge.unknown.ok === false && /clearance/.test(edge.unknown.why || ''), JSON.stringify(edge));
    if (pngBlob) { try { fs.mkdirSync(shots, { recursive: true }); fs.writeFileSync(path.join(shots, 'CB-4b-approval-picture.png'), pngBlob.buf); } catch (_) { /* only for looking at */ } }
    let png = null;
    // (the picture of the back as written: the same drawing renderBack makes of the approved back, so HI's box on it is the one the back's own view gives; the card has moved on to the next face)
    if (pngBlob) png = await page.evaluate(async ([b64, id]) => { const im = new Image(); im.src = 'data:image/png;base64,' + b64; await im.decode(); const c = document.createElement('canvas'); c.width = im.naturalWidth; c.height = im.naturalHeight; c.getContext('2d').drawImage(im, 0, 0); const j = __cb.backJob(id); if (!j.view) return { w: c.width, h: c.height, shelved: true }; const v = Engrave.renderBack(j, 500, { grid: false, hatch: false }); const px = __cb.pxBox(v, __cb.union(v._backArt.map(s => s.bbox))); return { w: c.width, h: c.height, vw: v.width, vh: v.height, dark: __cb.dark(c, px), blue: __cb.blue(c).n, area: (px[2] - px[0]) * (px[3] - px[1]), same: c.width === v.width && c.height === v.height && c.toDataURL() === v.toDataURL() }; }, [pngBlob.buf.toString('base64'), PID]);
    check('B14 the approval picture (the .png written beside the file) shows HI in the words\' ink where the file engraves it (dark pixels inside its box, no blue): it stands for the file, which engraves HI',
      pic.art === viewArt.length && pic.dark > 0.15 * pic.area && pic.blue === 0 && !!png && png.w === png.vw && png.dark > 0.15 * png.area && png.blue === 0, JSON.stringify({ pic, png }));
    await R.shot(w, 'CB-4-back-approved-again');

    /* ── the editor opens again on the saved back: HI an added, editable layer ── */
    await R.openEngraveDetail(w, 'cal', { face: 'back' }); await R.settle(page, 900);
    await page.click(Q + ' .backHost .egCharmEdit');
    await page.waitForSelector('.ce-root .ce-items path', { timeout: 60000 }); await settleP(page);
    const re = await page.evaluate(() => { const its = CharmEditor.current.items(); return { face: CharmEditor.current.face(), n: its.length, added: its.filter(i => i.source === 'added').map(i => ({ name: i.name, locked: !!i.locked, role: i.role })), cut: its.filter(i => i.role === 'cut').map(i => !!i.locked) }; });
    await R.shot(w, 'CB-5-editor-reopened-back');
    check('B15 the editor opens again on the back with HI a layer of its own: added here, not locked (it can be moved, changed or deleted), named by its words; the cut line still locked',
      re.face === 'back' && re.added.length === after.ids.length && re.added.some(a => a.name === 'HI') && re.added.every(a => !a.locked && a.role === 'hatch') && re.cut.length === 1 && re.cut[0] === true, JSON.stringify(re));
    await page.click('.ce-root .ce-foot [data-act="cancel"]'); await settleP(page);
    if (await page.evaluate(() => !!document.querySelector('.ce-ask'))) { await page.click('.ce-ask [data-ask="yes"]'); await settleP(page); }

    /* ── a calendar with nothing added: its back drawn and masked as before ── */
    const plain = await page.evaluate(id => { const j = __cb.backJob(id), ch = Pool.charmOf(id), G = CharmNestGeom, cv = Engrave.renderBack(j, [600, 600], { grid: true, editable: true }), m = G.engraveMask(j.view, { marginMm: j.mask.marginMm, keepOut: ch.backKeepOut || [] }); return { art: cv._backArt.length, blue: __cb.blue(cv._map.outline).n, same: m.w === j.mask.w && m.h === j.mask.h && m.bits.every((v, i) => v === j.mask.bits[i]) }; }, PID2);
    check('B16 the other calendar (nothing added) draws no added art and its back\'s mask is exactly the one made without any', plain.art === 0 && plain.blue === 0 && plain.same, JSON.stringify(plain));
    check('B17 no page error', w.errors.length === 0, JSON.stringify(w.errors).slice(0, 400));
    console.log('screenshots in ' + shots);
  } finally { await w.close(); }
}

(async () => {
  const kit = await nodeKit();
  if (ONLY !== 'B') await partA(kit);
  if (ONLY !== 'A') await partB(kit);
  const bad = RESULTS.filter(r => !r.ok);
  console.log(`\n${RESULTS.length - bad.length}/${RESULTS.length} checks pass`);
  process.exit(bad.length ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
