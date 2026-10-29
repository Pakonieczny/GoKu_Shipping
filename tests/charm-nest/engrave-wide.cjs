// A wide piece in the Engrave editor uses the whole width of the work area when the Approved block (top right) does not touch it
// (Paul, 10 Oct 2026, order 4174565371, the "Stuart" bar: "not properly centred on the screen and right up top, which makes it look squished").
//   node tests/charm-nest/engrave-wide.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>; without a browser only part 1 runs)
// The Approved block sits at the top right of the work area. Its column (181 px, .pvMain padding-right) was held back for the work area's whole
// height, so a bar 4.5 times wider than tall was drawn in 381 px of a 564 px column (1500x1000) with 300 px of empty frame above and below it.
// Now the column is held back only where the block really is: a piece clearly wider than tall that, drawn at the full width and centred in the
// work area, sits clear below the block takes the whole width (the card wears .egWide); every other piece, the stacked card (under 920 px) and a
// piece that would touch the block keep exactly the strip they had. The block itself, the drawing, its scale and the saved geometry are untouched.
// STEADYGRID (Paul, 28 Oct 2026: "The grid should be static and only the charm inside the grid should rotate. The grid should also be always in the same place and also be the same full size"): the
// work area's grid is now decided by the card alone (mountBack): the piece turns inside it at one scale for every turn (renderBack steady), so the column is given back (.egWide) when the grid is
// larger so, whatever the piece's shape, and the grid starts under the card's top band (the Charm Editor button; the block when the column is taken). What stays: nothing of the drawing is ever
// under the block, the canvas is inside its box and centred in it, never stretched, the block itself untouched.
//  1. the steady frame (renderBack, the editor's own source): real engraving geometry, no browser: the canvas is the box, the same at every turn, the turned piece inside it;
//  2. the real card in real Chromium (the real page and CSS on the repo's fake site, the page's own fonts, fit and renderBack, nothing written anywhere):
//     bar, disc, long pendant laid flat, upright pendant x 1180x720, 1500x1000, 1800x1100, 900x600 x (Approved button, button and seals, no button):
//     the canvas never touches the block's button or seals, stays inside its box, is centred both ways, wide pieces fill the column, the rest keep
//     the strip, a resize and a growing block are followed.
const assert = require('node:assert/strict'), fs = require('node:fs'), vm = require('node:vm'), path = require('node:path');
const root = path.join(__dirname, '../..');
const src = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8'), css = fs.readFileSync(path.join(root, 'charm-nest-activity.css'), 'utf8'), html = fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8');

// ── 0 · what the files say ───────────────────────────────────────────────────────────────────────────────────────────────────────────────
{
  const base = css.match(/\.pvMain:has\(\.pvApproval:not\(:empty\)\)\{([^}]*)\}/), wide = css.match(/\.pvMain\.egWide:has\(\.pvApproval:not\(:empty\)\)\{([^}]*)\}/), blk = css.match(/\.pvApproval\{([^}]*)\}/);
  assert(base && /padding-right:calc\(156px \+ var\(--seal-base,50px\) \/ 2\)/.test(base[1]), 'the strip is held back as before');
  assert(wide && /^padding-right:0$/.test(wide[1].trim()), 'a wide piece gives the strip back, and only that');
  assert(css.indexOf(base[0]) < css.indexOf(wide[0]), '.egWide comes after the rule it overrides');
  assert(blk && /position:absolute;top:calc\(var\(--seal-base,50px\) \/ 2\);right:0;display:flex;flex-direction:column;align-items:flex-end;gap:8px;width:calc\(156px \+ var\(--seal-base,50px\) \/ 2\)/.test(blk[1]), 'the Approved block keeps its place, size and look');
  const cv = html.match(/\.rvItem\.full \.backHost canvas\{([^}]*)\}/);
  assert(cv && /align-self:\s*center/.test(cv[1]), 'the canvas is centred in the work area (ENGCENTER)');
  assert(/mountBack = \(\) => \{[\s\S]*classList\.toggle\("egWide", wide\)[\s\S]*steady: true/.test(src) && !/function takesStrip/.test(src), 'mountBack decides the column by the grid\'s own size, wears the class, and draws the steady grid');
  assert(/ro\.observe\(backHost\); const ap = card\.querySelector\("\.pvApproval"\); if \(ap\) ro\.observe\(ap\)/.test(src), 'and follows the block when it changes size');
  for (const [file, re] of [['charm-nest-bridge.js', /charm-nest-bridge\.js\?v=([^"]+)"/], ['charm-nest-activity.css', /charm-nest-activity\.css\?v=([^"]+)"/]]) {
    const m = html.match(re); assert(m && /-wd1(-|$)/.test(m[1]), `${file}: its ?v= token carries this change (-wd1)`);
  }
}

// ── 1 · the rule, on real engraving geometry ─────────────────────────────────────────────────────────────────────────────────────────────
const G = require(path.join(root, 'charm-nest-geom.js')), Fit = require(path.join(root, 'charm-nest-engrave-fit.js')), T = require(path.join(root, 'charm-nest-text.js'));
const ot = require(path.join(root, 'vendor/opentype-1.3.4.min.js'));
const { CharmNestPDF: P } = require(path.join(root, 'netlify/functions/_charmNestPdf.js'));
const PT = 72 / 25.4, MM = 25.4 / 72;
const bytes = f => { const b = fs.readFileSync(path.join(root, f)); return b.buffer.slice(b.byteOffset, b.byteOffset + b.byteLength); };
const emoji = ot.parse(bytes('vendor/fonts/NotoEmoji-Regular.ttf')), emojiMap = require(path.join(root, 'vendor/fonts/emoji-sequences.json'));
const fonts = Object.fromEntries(['Regular', 'Semibold'].map(w => [w, T.withEmoji(ot.parse(bytes(`vendor/fonts/SourceSans3-${w}.otf`)), emoji, emojiMap, ot.Path)]));
const cut = (a, b) => { const i = src.indexOf(a), j = src.indexOf(b, i); assert(i > 0 && j > i, `bridge source markers: ${a}`); return src.slice(i, j); };
const noop = () => {};
const fakeCanvas = () => { const ctx = new Proxy({}, { get: (_, k) => (k === 'createImageData' ? (w, h) => ({ data: new Uint8ClampedArray(w * h * 4) }) : noop), set: () => true }); return { width: 0, height: 0, getContext: () => ctx }; };
const env = vm.createContext({ G, P, PT, MM, Math, Array, Infinity, document: { createElement: () => fakeCanvas() }, console, window: {}, WeakMap });   // (window: the code between the markers sets the page's CharmEditDesign)
vm.runInContext(cut('  function textBox(glyphs', '  function resize(job, size)') + cut('  /* ── 7.6 · back files', '  function renderFront(') + '\nthis.renderBack = renderBack; this.BLOCK_AIR;', env);
const ring = (cx, cy, r) => [...Array(32).keys()].map(i => [cx + r * Math.cos(i / 32 * 2 * Math.PI), cy + r * Math.sin(i / 32 * 2 * Math.PI)]);
const rect = (x, y, w, h) => [[x, y], [x + w, y], [x + w, y + h], [x, y + h]];
const path_ = pts => { const xs = pts.map(p => p[0]), ys = pts.map(p => p[1]); return { kind: 'path', layer: 'CUT', closed: true, stroke: true, strokeRGB: [1, 0, 0], subpaths: [pts.map((p, i) => [i ? 'l' : 'm', p]).concat([['h']])], bbox: [Math.min(...xs), Math.min(...ys), Math.max(...xs), Math.max(...ys)] }; };
const charm = (outline, holes) => { const o = path_(outline); return { outline: o, members: [o, ...holes.map(path_)], bbox: o.bbox }; };
const designs = {   // TINY BAR_7427 is 91.9 x 17.2 pt with a hoop at each end
  bar: { c: charm(rect(0, 0, 91.9, 17.2), [ring(6.5, 8.6, 3), ring(85.4, 8.6, 3)]), text: 'Stuart', wide: true },
  'long pendant laid flat': { c: charm(rect(0, 0, 20, 50.4), [rect(8, 45, 4, 3)]), text: 'Joe', wide: true },
  disc: { c: charm(ring(30, 30, 24), [ring(30, 51, 2)]), text: 'Anna', wide: false },
  'upright pendant': { c: charm(rect(0, 0, 20, 50.4), [rect(8, 45, 4, 3)]), text: 'Joe', view: { editingBack: true, savedUp: 90 }, wide: false }
};
const prepare = d => {
  const res = Fit.calculate({ charm: d.c, lines: [d.text], lineMode: 'auto', viewOptions: G.viewOptionsFor(d.view || {}), maskOptions: { marginMm: .8, keepOut: [] },
    opts: { minCapMm: 1.6, maxHeightFrac: .4, lineGap: .216, minStrokeMm: 0, minGapMm: 0, tryRotated: false } }, fonts, G);
  assert(res.fit, `${d.text}: the words fit: ${res.reason}`);
  return { view: res.view, mask: res.mask, fit: res.fit };
};
const side = v => Math.round(Math.min(1400, Math.max(200, v - 4)));
// the work areas the real card gives (measured in Chromium on the real card, see part 2): [column width with the strip, work-area height], and how far down the block reaches
const STRIP = 181, AREAS = { '1180x720': [556.8, 499], '1500x1000': [564.3, 797.8], '1800x1100': [723.3, 897.8] }, BELOW = { 'button': 45, 'button and seals': 103 };
let rules = 0;
for (const [name, d] of Object.entries(designs)) {
  const job = prepare(d), face = job.view.face === 'front' ? { face: 'front' } : {};
  for (const [vp, [W, H]] of Object.entries(AREAS)) for (const px of [[side(W - STRIP), side(H)], [side(W), side(H - 120)]]) {
    const why = `${name} at ${vp}, box ${px.join('x')}`, bc = env.renderBack(job, px, { grid: true, editable: true, steady: true });
    assert(bc._steady && bc.width === px[0] && bc.height === px[1], `${why}: the canvas is the box itself (${bc.width}x${bc.height})`);
    const c = [(bc._map.bb[0] + bc._map.bb[2]) / 2, (bc._map.bb[1] + bc._map.bb[3]) / 2];
    assert(Math.abs(c[0] - job.view.cx) < 1e-6 && Math.abs(c[1] - job.view.cy) < 1e-6, `${why}: centred on the point the piece turns about`);
    for (const turn of [35, 90, 180]) {
      const view = G.backView(d.c, Object.assign({ res: 6, upAngle: job.view.upAngle - turn, holeOnly: true }, face)), b2 = env.renderBack({ view, mask: null, fit: null }, px, { grid: true, editable: true, steady: true });
      assert(b2.width === bc.width && b2.height === bc.height && Math.abs(b2._map.k - bc._map.k) < 1e-9 && JSON.stringify(b2._map.bb.map(v => v.toFixed(6))) === JSON.stringify(bc._map.bb.map(v => v.toFixed(6))), `${why}: turned ${turn}°, the same grid (size, scale, millimetres)`);
      for (const m of view.members) { const a = b2._map.tx(m.bbox[0], m.bbox[1]), b = b2._map.tx(m.bbox[2], m.bbox[3]); assert([a, b].every(q => q[0] >= -.5 && q[1] >= -.5 && q[0] <= b2.width + .5 && q[1] <= b2.height + .5), `${why}: turned ${turn}°, the piece is inside the grid`); }
    }
    rules++;
  }
}
console.log(`Engrave steady grid OK: ${rules} boxes on real geometry (the canvas is the box, one scale and the same millimetres at every turn, the turned piece inside)`);

// ── 2 · the real card in real Chromium ────────────────────────────────────────────────────────────────────────────────────────────────────
(async () => {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { try { ({ chromium } = require('/opt/node22/lib/node_modules/playwright/node_modules/playwright-core')); } catch (__) { console.log('  – no playwright-core: the browser checks were not run'); return; } }
  const { start } = require('./bridge-server.cjs');
  const srv = await start({ receipts: [] }), { sorterOrigin } = srv;
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium', args: ['--no-sandbox'] });
  const fails = [];
  const check = (ok, msg) => { if (!ok) fails.push(msg); };
  try {
    const ctx = await browser.newContext({ viewport: { width: 1500, height: 1000 } });
    const fbStub = "const nope = () => { throw new Error('firebase stub'); }; export const initializeApp = nope, getApp = nope, getStorage = nope, ref = nope, uploadBytesResumable = nope, getDownloadURL = nope, getAuth = nope, signInAnonymously = nope;";
    await ctx.route(/gstatic\.com\/firebasejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: /-compat\.js/.test(r.request().url()) ? '' : fbStub }));
    await ctx.route(/qrcodejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: fs.readFileSync(path.join(root, 'lib/qrcode.min.js')) }));
    await ctx.route(/fonts\.googleapis|fonts\.gstatic/, r => r.fulfill({ status: 200, contentType: 'text/css', body: '' }));
    await ctx.route(/^https:\/\/firebasestorage\.googleapis\.com\//, r => r.fulfill({ status: 404, body: 'none' }));
    await ctx.addInitScript(({ sorter }) => {
      if (location.origin === sorter) localStorage.setItem('cn.employee', 'Tester');
      window.confirm = () => true; window.prompt = () => 'Tester'; window.alert = () => {};
      if (window.Notification) { try { Object.defineProperty(window, 'Notification', { value: undefined }); } catch (_) {} }
    }, { sorter: sorterOrigin });
    const page = await ctx.newPage(), errors = [];
    page.on('pageerror', e => errors.push('pageerror: ' + e.message));
    await page.goto(`${sorterOrigin}/charm-nest-1.html`);
    await page.waitForFunction(() => window.CN && window.CN.S && window.Engrave && window.CharmNestEngraveFit);
    await page.evaluate(async () => { CN.S.settings.review = 'off'; await Engrave.loadFonts(); });

    // one real placement card for a piece: the page's own fit, the card drawn by Engrave.render() (mountBack, renderBack and the CSS all real)
    const card = (key, shape, state) => page.evaluate(async ({ key, shape, state }) => {
      const ring = (cx, cy, r) => [...Array(32).keys()].map(i => [cx + r * Math.cos(i / 32 * 2 * Math.PI), cy + r * Math.sin(i / 32 * 2 * Math.PI)]);
      const rect = (x, y, w, h) => [[x, y], [x + w, y], [x + w, y + h], [x, y + h]];
      const mem = pts => { const xs = pts.map(p => p[0]), ys = pts.map(p => p[1]); return { kind: 'path', layer: 'CUT', closed: true, stroke: true, strokeRGB: [1, 0, 0], subpaths: [pts.map((p, i) => [i ? 'l' : 'm', p]).concat([['h']])], bbox: [Math.min(...xs), Math.min(...ys), Math.max(...xs), Math.max(...ys)] }; };
      const make = (outline, holes) => { const o = mem(outline); return { outline: o, members: [o, ...holes.map(mem)], bbox: o.bbox }; };
      const SH = {
        bar: { c: make(rect(0, 0, 91.9, 17.2), [ring(6.5, 8.6, 3), ring(85.4, 8.6, 3)]), words: 'Stuart' },
        disc: { c: make(ring(30, 30, 24), [ring(30, 51, 2)]), words: 'Anna' },
        flat: { c: make(rect(0, 0, 20, 50.4), [rect(8, 45, 4, 3)]), words: 'Joe' },
        tall: { c: make(rect(0, 0, 20, 50.4), [rect(8, 45, 4, 3)]), words: 'Joe', view: { editingBack: true, savedUp: 90 } }
      }[shape];
      const res = CharmNestEngraveFit.calculate({ charm: SH.c, lines: [SH.words], lineMode: 'auto', viewOptions: CharmNestGeom.viewOptionsFor(SH.view || { entry: {} }), maskOptions: { marginMm: .8, keepOut: [] },
        opts: { minCapMm: 1.6, maxHeightFrac: .4, lineGap: .216, minStrokeMm: 0, minGapMm: 0, tryRotated: true } }, Engrave.fonts, CharmNestGeom);
      const row = { key, state: 'pooled', poolIds: ['p-' + key], order: { receiptId: '4174565371' }, line: { sku: shape, title: shape, variations: [], transactionId: '1' }, spec: { designSku: shape, personalization: [SH.words], form: '', size: '' }, engrave: { state: 'review' } };
      const job = { key, row, copies: ['p-' + key], state: 'review', lines: [SH.words], lineInput: [SH.words], text: SH.words, view: res.view, mask: res.mask, fit: state === 'none' ? null : res.fit, editCharm: SH.c, confidence: .62, source: 'personalization', questions: [], requests: {}, orientVersion: CharmNestGeom.ORIENT, materialVersion: 2, wantSize: res.fit && res.fit.size };
      if (state === 'seals') job.engravingSeals = [0, 1, 2].map(i => ({ id: 'engraveApproved:' + (1700000000000 + i * 3600000), how: 'engraveApproved', at: 1700000000000 + i * 3600000, by: 'Tester' + i }));
      Engrave.items().clear(); Engrave.items().set(key, job);
      CN.setMode('engrave'); const v = document.getElementById('engraveView'); v.dataset.egTab = ''; Engrave.render();
      return true;
    }, { key, shape, state });
    const settle = () => page.evaluate(async () => { await new Promise(r => setTimeout(r, 200)); await new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r))); await new Promise(r => setTimeout(r, 120)); });
    const measure = () => page.evaluate(() => {
      const c = document.querySelector('#engraveView .rvItem[data-kind=placement]'); if (!c) return null;
      const host = c.querySelector('.backHost'), cv = host.querySelector('canvas'), pm = c.querySelector('.pvMain'), ap = c.querySelector('.pvApproval');
      const R = el => { if (!el) return null; const b = el.getBoundingClientRect(); return { x: b.x, y: b.y, w: b.width, h: b.height, r: b.right, b: b.bottom }; };
      return { host: R(host), canvas: R(cv), block: R(ap), button: R(c.querySelector('.egApproveButton')), seals: R(c.querySelector('.pvApproval .sealRow')), pm: R(pm), padR: parseFloat(getComputedStyle(pm).paddingRight), wide: pm.classList.contains('egWide'),
        stacked: getComputedStyle(host).flexGrow === '0', padT: parseFloat(getComputedStyle(host).paddingTop), padB: parseFloat(getComputedStyle(host).paddingBottom), box: cv && cv._box ? [...cv._box.corners, cv._box.rotate] : null, bitmap: cv ? [cv.width, cv.height] : null };
    });
    const hit = (a, b) => !!(a && b && a.w > 0 && a.h > 0 && b.w > 0 && b.h > 0 && Math.min(a.r, b.r) - Math.max(a.x, b.x) > 0 && Math.min(a.b, b.b) - Math.max(a.y, b.y) > 0);

    const SHAPES = { bar: true, flat: true, disc: false, tall: false }, STATES = ['button', 'seals', 'none'], WINDOWS = [[1180, 720], [1500, 1000], [1800, 1100], [900, 600]];
    let n = 0, wideChecked = 0, keptChecked = 0; const widths = {}, same = {};
    for (const [shape, isWide] of Object.entries(SHAPES)) for (const state of STATES) for (const [w, h] of WINDOWS) {
      await page.setViewportSize({ width: w, height: h });
      await card(`k${++n}`, shape, state); await settle();
      const m = await measure(), why = `${shape}, ${state}, ${w}x${h}`;
      if (!m || !m.canvas) { check(false, `${why}: the card has its back drawn`); continue; }
      const { canvas: cv, host, block, pm } = m;
      // wherever the Approved block is, nothing of the drawing is under it
      check(!hit(cv, m.button) && !hit(cv, m.seals) && !hit(cv, block), `${why}: the canvas does not touch the block (${Math.round(cv.w)}x${Math.round(cv.h)} at ${Math.round(cv.x)},${Math.round(cv.y)}; block ${block && Math.round(block.w)}x${block && Math.round(block.h)} at ${block && Math.round(block.x)},${block && Math.round(block.y)})`);
      check(cv.x >= host.x - .5 && cv.r <= host.r + .5 && cv.y >= host.y - .5 && cv.b <= host.b + .5, `${why}: the canvas is inside its box`);
      check(Math.abs((cv.y - host.y - m.padT) - (host.b - m.padB - cv.b)) <= 1.5, `${why}: centred up and down under the card's top band (${(cv.y - host.y - m.padT).toFixed(1)} above, ${(host.b - m.padB - cv.b).toFixed(1)} below)`);
      check(Math.abs((cv.x - host.x) - (host.r - cv.r)) <= 3, `${why}: centred left and right (${(cv.x - host.x).toFixed(1)} left, ${(host.r - cv.r).toFixed(1)} right)`);
      if (m.box) check(m.box.every(p => p[0] >= -.5 && p[1] >= -.5 && p[0] <= m.bitmap[0] + .5 && p[1] <= m.bitmap[1] + .5), `${why}: the drawn text box is inside the canvas`);
      const blockOn = block && block.h > 0, strip = blockOn ? block.w : 0;
      check(Math.abs((cv.w - 2) / (cv.h - 2) - m.bitmap[0] / m.bitmap[1]) < .02, `${why}: the drawing keeps its shape (never stretched; the canvas's border is 2 px)`);
      // (STEADYGRID: the card decides, not the piece: every piece gets the same grid in the same card, window and block state)
      const at = `${state} ${w}x${h}`; if (!same[at]) same[at] = []; same[at].push({ shape, rect: [cv.x, cv.y, cv.w, cv.h].map(v => Math.round(v)), wide: m.wide });
      if (m.stacked) check(!m.wide, `${why}: the stacked card keeps its strip`);
      else if (m.wide) { check(m.padR === 0 && cv.w > pm.w - strip + 50 && (!blockOn || cv.y >= block.b - .5), `${why}: the column taken: wider than beside the strip (${Math.round(cv.w)} > ${Math.round(pm.w - strip)}), under the block`); wideChecked++; }
      else { if (blockOn) { check(Math.abs(m.padR - strip) < .5 && cv.w <= pm.w - strip, `${why}: beside the strip (${Math.round(cv.w)} <= ${Math.round(pm.w - strip)})`); keptChecked++; } else check(m.padR === 0, `${why}: no block, no strip`); }
      // the block did not move or change: top right, 181 wide
      if (blockOn) { check(Math.abs(block.r - pm.r) < .5 && Math.abs(block.w - 181) < .5, `${why}: the block is still 181 px wide at the right edge`); check(Math.abs((block.y - pm.y) - 25) < .5, `${why}: and 25 px from the top of its column`); }
    }
    for (const [at, list] of Object.entries(same)) check(list.every(x => JSON.stringify(x.rect) === JSON.stringify(list[0].rect) && x.wide === list[0].wide), `${at}: every piece has the same grid in the same card (${JSON.stringify(list)})`);
    check(wideChecked >= 6 && n >= 48, `enough cases ran (wide ${wideChecked}, kept ${keptChecked}, ${n} cards)`);

    // a window dragged bigger and smaller follows (the same pixels as a fresh card), and comes back
    await page.setViewportSize({ width: 1500, height: 1000 }); await card('live', 'bar', 'button'); await settle();
    const first = await measure(), seen = [];
    for (const [w, h] of [[1180, 720], [900, 600], [1800, 1100], [1500, 1000]]) {
      await page.setViewportSize({ width: w, height: h }); await settle();
      const m = await measure(); seen.push([w, h, Math.round(m.canvas.w), Math.round(m.canvas.h), m.wide]);
      check(!hit(m.canvas, m.button), `live ${w}x${h}: the canvas does not touch the button`);
      check(!m.stacked || !m.wide, `live ${w}x${h}: the stacked card keeps its strip (${m.wide})`);
    }
    const back = await measure();
    check(Math.abs(back.canvas.w - first.canvas.w) <= 2 && Math.abs(back.canvas.h - first.canvas.h) <= 2, `live: back at 1500x1000 the canvas is as it was (${Math.round(first.canvas.w)}x${Math.round(first.canvas.h)} -> ${Math.round(back.canvas.w)}x${Math.round(back.canvas.h)})`);

    // the block grows (more seals, a message): the piece is not left under it
    await page.setViewportSize({ width: 1180, height: 720 }); await card('grow', 'bar', 'button'); await settle();
    const before = await measure();
    await page.evaluate(() => { const ap = document.querySelector('#engraveView .pvApproval'); const d = document.createElement('div'); d.id = 'tall'; d.style.cssText = 'height:300px;width:100px'; ap.appendChild(d); });
    await settle();
    const grown = await measure();
    check(!hit(grown.canvas, grown.block), `growing block: the canvas does not touch the taller block (${Math.round(grown.canvas.w)}x${Math.round(grown.canvas.h)} at y ${Math.round(grown.canvas.y)}; block ends ${Math.round(grown.block.b)})`);
    await page.evaluate(() => document.getElementById('tall').remove()); await settle();
    const shrunk = await measure(); check(shrunk.wide === before.wide && Math.abs(shrunk.canvas.w - before.canvas.w) <= 2 && Math.abs(shrunk.canvas.y - before.canvas.y) <= 2, 'growing block: the grid as it was when the block is small again');
    check(errors.length === 0, 'no page errors: ' + errors.join(' | '));
    console.log(`Engrave wide card OK in Chromium: ${n} cards (4 pieces x 3 block states x 4 windows); wide ${wideChecked}, strip kept ${keptChecked}; live resize ${JSON.stringify(seen)}`);
  } finally { await browser.close(); srv.close(); }
  if (fails.length) { console.error(`\n${fails.length} failed:\n - ${fails.slice(0, 40).join('\n - ')}`); process.exit(1); }
})().catch(e => { console.error(e); process.exit(1); });
