// A wide piece in the Engrave editor uses the whole width of the work area when the Approved block (top right) does not touch it
// (Paul, 10 Oct 2026, order 4174565371, the "Stuart" bar: "not properly centred on the screen and right up top, which makes it look squished").
//   node tests/charm-nest/engrave-wide.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>; without a browser only part 1 runs)
// The Approved block sits at the top right of the work area. Its column (181 px, .pvMain padding-right) was held back for the work area's whole
// height, so a bar 4.5 times wider than tall was drawn in 381 px of a 564 px column (1500x1000) with 300 px of empty frame above and below it.
// Now the column is held back only where the block really is: a piece clearly wider than tall that, drawn at the full width and centred in the
// work area, sits clear below the block takes the whole width (the card wears .egWide); every other piece, the stacked card (under 920 px) and a
// piece that would touch the block keep exactly the strip they had. The block itself, the drawing, its scale and the saved geometry are untouched.
//  1. the rule (takesStrip, the editor's own source): real engraving geometry, no browser;
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
  assert(/mountBack = \(\) => \{[\s\S]*takesStrip\([\s\S]*classList\.toggle\("egWide"/.test(src), 'mountBack decides and wears the class');
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
const env = vm.createContext({ G, P, PT, MM, Math, Array, Infinity, document: { createElement: () => fakeCanvas() }, console });
vm.runInContext(cut('  function textBox(glyphs', '  function resize(job, size)') + cut('  /* ── 7.6 · back files', '  function renderFront(') + cut('  const WIDE_PIECE = ', '  /** The placement review card') + '\nthis.renderBack = renderBack; this.takesStrip = takesStrip; this.WIDE_PIECE = WIDE_PIECE; this.BLOCK_AIR = BLOCK_AIR;', env);
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
  const job = prepare(d);
  for (const [vp, [W, H]] of Object.entries(AREAS)) for (const [bn, below] of Object.entries(BELOW)) {
    const why = `${name} at ${vp}, block: ${bn}`;
    const bc = env.renderBack(job, [side(W - STRIP), side(H)], { grid: true, editable: true });                  // beside the strip: as it always was
    const to = [side(W), side(H)], yes = env.takesStrip(bc._sizePt, [bc.width, bc.height], to, H, below);
    // the bar always clears the block; the long pendant (about 1.7 : 1 with its text box) is taller, so at the smallest window with the block's seals under the button there is no room
    const want = d.wide && !(name === 'long pendant laid flat' && vp === '1180x720' && bn === 'button and seals');
    assert.equal(yes, want, `${why}: ${want ? 'a wide piece takes the strip' : d.wide ? 'no room below the block, the strip stays' : 'a round or upright piece keeps it'} (canvas beside the strip ${bc.width}x${bc.height})`);
    if (yes) {
      const w2 = env.renderBack(job, to, { grid: true, editable: true });
      assert(w2.width >= bc.width + 12 && w2.width <= W, `${why}: it grows (${bc.width} -> ${w2.width}) and stays inside the column (${W})`);
      assert((H - (w2.height + 2)) / 2 >= below + env.BLOCK_AIR, `${why}: centred in the work area it sits clear below the block (top ${((H - w2.height - 2) / 2).toFixed(1)} px, block ends ${below})`);
      assert(w2.width >= .95 * (W - 4), `${why}: it fills the column (${w2.width} of ${W})`);
    }
    // a block that comes down over where the piece sits gives no strip back
    assert.equal(env.takesStrip(bc._sizePt, [bc.width, bc.height], to, H, H / 2 + 20), false, `${why}: a block reaching past the middle keeps the strip`);
    rules++;
  }
  // a short work area (the clear gap is gone) and a window that would give nothing back
  const bc = env.renderBack(job, [side(300 - STRIP), side(150)], { grid: true, editable: true });
  assert.equal(env.takesStrip(bc._sizePt, [bc.width, bc.height], [side(300), side(150)], 150, 45), false, `${name}: no room below the block, no change`);
  assert.equal(env.takesStrip(null, [100, 50], [200, 200], 500, 40), false, 'no drawing size, no change');
}
console.log(`Engrave wide rule OK: ${rules} cases on real geometry (wide pieces take the strip when clear, round and upright ones never)`);

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
        stacked: getComputedStyle(host).flexGrow === '0', box: cv && cv._box ? [...cv._box.corners, cv._box.rotate] : null, bitmap: cv ? [cv.width, cv.height] : null };
    });
    const hit = (a, b) => !!(a && b && a.w > 0 && a.h > 0 && b.w > 0 && b.h > 0 && Math.min(a.r, b.r) - Math.max(a.x, b.x) > 0 && Math.min(a.b, b.b) - Math.max(a.y, b.y) > 0);

    const SHAPES = { bar: true, flat: true, disc: false, tall: false }, STATES = ['button', 'seals', 'none'], WINDOWS = [[1180, 720], [1500, 1000], [1800, 1100], [900, 600]];
    let n = 0, wideChecked = 0, keptChecked = 0; const widths = {};
    for (const [shape, isWide] of Object.entries(SHAPES)) for (const state of STATES) for (const [w, h] of WINDOWS) {
      await page.setViewportSize({ width: w, height: h });
      await card(`k${++n}`, shape, state); await settle();
      const m = await measure(), why = `${shape}, ${state}, ${w}x${h}`;
      if (!m || !m.canvas) { check(false, `${why}: the card has its back drawn`); continue; }
      const { canvas: cv, host, block, pm } = m;
      // wherever the Approved block is, nothing of the drawing is under it
      check(!hit(cv, m.button) && !hit(cv, m.seals) && !hit(cv, block), `${why}: the canvas does not touch the block (${Math.round(cv.w)}x${Math.round(cv.h)} at ${Math.round(cv.x)},${Math.round(cv.y)}; block ${block && Math.round(block.w)}x${block && Math.round(block.h)} at ${block && Math.round(block.x)},${block && Math.round(block.y)})`);
      check(cv.x >= host.x - .5 && cv.r <= host.r + .5 && cv.y >= host.y - .5 && cv.b <= host.b + .5, `${why}: the canvas is inside its box`);
      check(Math.abs((cv.y - host.y) - (host.b - cv.b)) <= 1.5, `${why}: centred up and down (${(cv.y - host.y).toFixed(1)} above, ${(host.b - cv.b).toFixed(1)} below)`);
      check(Math.abs((cv.x - host.x) - (host.r - cv.r)) <= 3, `${why}: centred left and right (${(cv.x - host.x).toFixed(1)} left, ${(host.r - cv.r).toFixed(1)} right)`);
      if (m.box) check(m.box.every(p => p[0] >= -.5 && p[1] >= -.5 && p[0] <= m.bitmap[0] + .5 && p[1] <= m.bitmap[1] + .5), `${why}: the drawn text box is inside the canvas`);
      const blockOn = block && block.h > 0, strip = blockOn ? block.w : 0;
      check(Math.abs((cv.w - 2) / (cv.h - 2) - m.bitmap[0] / m.bitmap[1]) < .02, `${why}: the drawing keeps its shape (never stretched; the canvas's border is 2 px)`);
      if (m.stacked) {                                   // under 920 px the card stacks: exactly as before, the strip stays
        check(!m.wide, `${why}: the stacked card is untouched`);
        if (blockOn) check(Math.abs(m.padR - strip) < .5, `${why}: and keeps its strip (${m.padR} / ${strip})`);
      } else if (isWide && blockOn && !(shape === 'flat' && state === 'seals' && w === 1180)) {   // a wide piece with the block at the top right: the whole column (the long pendant, taller, has no room under button AND seals at 1180x720)
        check(m.wide, `${why}: the wide piece takes the strip`);
        check(m.padR === 0 && cv.w >= .95 * (pm.w - 4), `${why}: and fills the column (${Math.round(cv.w)} of ${Math.round(pm.w)})`);
        check(cv.w > pm.w - strip + 100, `${why}: well beyond the width beside the strip (${Math.round(cv.w)} > ${Math.round(pm.w - strip)})`);
        (widths[`${shape} ${state} ${w}x${h}`] = Math.round(cv.w)); wideChecked++;
      } else {                                           // round and upright pieces, a piece with no room under the block, and a card without a block: as before
        check(!m.wide, `${why}: no strip given back`);
        if (blockOn) { check(Math.abs(m.padR - strip) < .5 && cv.w <= pm.w - strip, `${why}: still beside the strip (${Math.round(cv.w)} <= ${Math.round(pm.w - strip)})`); keptChecked++; }
        else check(m.padR === 0, `${why}: no block, no strip`);
      }
      // the block did not move or change: top right, 181 wide
      if (blockOn) { check(Math.abs(block.r - pm.r) < .5 && Math.abs(block.w - 181) < .5, `${why}: the block is still 181 px wide at the right edge`); check(Math.abs((block.y - pm.y) - 25) < .5, `${why}: and 25 px from the top of its column`); }
    }
    check(wideChecked >= 11 && keptChecked >= 12, `enough cases ran (wide ${wideChecked}, kept ${keptChecked})`);

    // a window dragged bigger and smaller follows (the same pixels as a fresh card), and comes back
    await page.setViewportSize({ width: 1500, height: 1000 }); await card('live', 'bar', 'button'); await settle();
    const first = await measure(), seen = [];
    for (const [w, h] of [[1180, 720], [900, 600], [1800, 1100], [1500, 1000]]) {
      await page.setViewportSize({ width: w, height: h }); await settle();
      const m = await measure(); seen.push([w, h, Math.round(m.canvas.w), Math.round(m.canvas.h), m.wide]);
      check(!hit(m.canvas, m.button), `live ${w}x${h}: the canvas does not touch the button`);
      check(m.wide === (w > 920), `live ${w}x${h}: wide on a side-by-side card, not on the stacked one (${m.wide})`);
    }
    const back = await measure();
    check(Math.abs(back.canvas.w - first.canvas.w) <= 2 && Math.abs(back.canvas.h - first.canvas.h) <= 2, `live: back at 1500x1000 the canvas is as it was (${Math.round(first.canvas.w)}x${Math.round(first.canvas.h)} -> ${Math.round(back.canvas.w)}x${Math.round(back.canvas.h)})`);

    // the block grows (more seals, a message): the piece is not left under it
    await page.setViewportSize({ width: 1180, height: 720 }); await card('grow', 'bar', 'button'); await settle();
    const before = await measure(); check(before.wide, 'growing block: starts wide');
    await page.evaluate(() => { const ap = document.querySelector('#engraveView .pvApproval'); const d = document.createElement('div'); d.id = 'tall'; d.style.cssText = 'height:300px;width:100px'; ap.appendChild(d); });
    await settle();
    const grown = await measure();
    check(!hit(grown.canvas, grown.block), `growing block: the canvas does not touch the taller block (${Math.round(grown.canvas.w)}x${Math.round(grown.canvas.h)} at y ${Math.round(grown.canvas.y)}; block ends ${Math.round(grown.block.b)})`);
    check(!grown.wide && grown.padR > 0, 'growing block: the strip is held back again');
    await page.evaluate(() => document.getElementById('tall').remove()); await settle();
    const shrunk = await measure(); check(shrunk.wide && Math.abs(shrunk.canvas.w - before.canvas.w) <= 2, 'growing block: and given back when the block is small again');
    check(errors.length === 0, 'no page errors: ' + errors.join(' | '));
    console.log(`Engrave wide card OK in Chromium: ${n} cards (4 pieces x 3 block states x 4 windows); wide ${wideChecked}, strip kept ${keptChecked}; the bar's canvas: ${Object.entries(widths).filter(([k]) => /^bar button/.test(k)).map(([k, v]) => `${k.split(' ').pop()} ${v}px`).join(', ')}; live resize ${JSON.stringify(seen)}`);
  } finally { await browser.close(); srv.close(); }
  if (fails.length) { console.error(`\n${fails.length} failed:\n - ${fails.slice(0, 40).join('\n - ')}`); process.exit(1); }
})().catch(e => { console.error(e); process.exit(1); });
