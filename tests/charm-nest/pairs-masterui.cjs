// Pairs, picture side (PAIRMASTERUI): a mismatched pair design is drawn as ONE picture of both bodies side by side at one scale, a Left
// chip under the left body and a Right chip under the right one; every other charm is drawn by exactly the old code.
//   node tests/charm-nest/pairs-masterui.cjs        (the browser part needs PW_DIR=<playwright node_modules> and CHROMIUM=<chrome>; skipped without)
const fs = require('fs'), path = require('path'), http = require('http'), assert = require('assert'), cp = require('child_process');
const root = path.join(__dirname, '../..');
const Pair = require('../../charm-nest-pair.js');
const PT = require('../../charm-nest-pair-thumb.js');
require('./_noNestedArrays.cjs');
let n = 0; const ok = (c, m) => { assert(c, m); n++; };

const rect = (x0, y0, x1, y1, extra) => Object.assign({ kind: 'path', closed: true, stroke: true, fill: false, paintOp: 'S', layer: 'CUT', strokeRGB: [0, 0, 0], lwPt: .25, bbox: [x0, y0, x1, y1], subpaths: [[['m', [x0, y0]], ['l', [x1, y0]], ['l', [x1, y1]], ['l', [x0, y1]], ['h']]] }, extra || {});
const eng = (x0, y0, x1, y1, rgb) => Object.assign(rect(x0, y0, x1, y1), { layer: 'ENGRAVE', strokeRGB: rgb || [1, 0, 0] });
// MITTENS 1 (left, 16 x 22 pt, red cuff) and MITTENS 2 (right, 24 x 30 pt, blue plate): different sizes so a wrong scale shows
const oL = rect(0, 4, 16, 26), eL = eng(3, 6, 13, 9, [1, 0, 0]), oR = rect(22, 0, 46, 30), eR = eng(26, 4, 42, 10, [0, 0, 1]);
const pairCharm = () => ({ id: 'p', name: 'MISMATCHED_7134', outline: oR, members: [oR, eR, oL, eL], bbox: [0, 0, 46, 30], strokePt: .25, centerPt: [23, 15] });
const o1 = rect(0, 0, 20, 26), e1 = eng(4, 2, 16, 6);
const oneCharm = () => ({ id: 's', name: 'ONE', outline: o1, members: [o1, e1], bbox: [0, 0, 20, 26], strokePt: .25, centerPt: [10, 13] });
const twinCharm = () => ({ id: 't', name: 'TWIN', outline: o1, members: [o1, e1, rect(24, 0, 44, 26), eng(28, 2, 40, 6)], bbox: [0, 0, 44, 26], strokePt: .25, centerPt: [22, 13] });

/* ── 1 · the plan: which charms get chips ── */
ok(PT.plan(oneCharm()) === null, 'a normal charm has no plan: its picture is drawn by the old code');
ok(PT.plan(twinCharm()) === null, 'a matching pair drawn as twin bodies has no plan');
ok(PT.plan(null) === null && PT.plan({}) === null && PT.plan({ bbox: [0, 0, 1, 1] }) === null, 'nothing to draw gives no plan');
const pl = PT.plan(pairCharm());
ok(pl && pl.bodies.length === 2, 'a mismatched pair has a plan of two bodies');
ok(pl.bodies[0].side === 'L' && pl.bodies[1].side === 'R' && pl.bodies[0].label === 'Left' && pl.bodies[1].label === 'Right', 'Left then Right');
ok(pl.bodies[0].bbox[0] === 0 && pl.bodies[1].bbox[0] === 22, 'in drawing order (left body first), whichever body is the charm outline');
ok(PT.plan(pairCharm()).bodies[0].short === 'L' && PT.plan(pairCharm()).bodies[1].short === 'R', 'the short words');
ok(PT.plan({ sku: 'X', pair: { v: 1, bodies: 2, mismatched: true }, bbox: [0, 0, 5, 5] }) === null, 'a record with no geometry cannot be drawn: no plan');

/* ── 2 · the layout: one scale, the band under the drawing, the chips under their own body ── */
const L168 = PT.layout(pl, pairCharm().bbox, { size: 168, padPt: 2 });
ok(L168.W <= 168 && L168.H <= 168, 'the whole picture, band included, stays inside the size asked for');
ok(Math.abs(L168.W - Math.round((46 + 4) * L168.s)) <= 1, 'one scale for the whole drawing (width = drawing width x s)');
ok(L168.H0 === Math.max(8, Math.round((30 + 4) * L168.s)) && L168.H === L168.H0 + L168.band && L168.y > L168.H0, 'the chips sit in a band under the drawing');
ok(L168.chips.map(c => c.text).join() === 'Left,Right', 'at 168 px the chips say Left and Right');
const bodyCx = i => ((pl.bodies[i].bbox[0] + pl.bodies[i].bbox[2]) / 2 - 0 + 2) * L168.s;
ok(Math.abs(L168.chips[0].x + L168.chips[0].w / 2 - bodyCx(0)) < 1 && Math.abs(L168.chips[1].x + L168.chips[1].w / 2 - bodyCx(1)) < 1, 'each chip is centred under its own body');
ok(L168.chips[0].x >= 0 && L168.chips[1].x + L168.chips[1].w <= L168.W, 'the chips stay inside the picture');
ok(PT.layout(pl, pairCharm().bbox, { size: 90, padPt: 2 }).chips.map(c => c.text).join() === 'L,R', 'a small picture says L and R');
const big = PT.layout(pl, pairCharm().bbox, { size: 1600, padPt: 2 });
ok(Math.abs(big.fontPx / big.W - L168.fontPx / L168.W) < .01, 'the chip is the same share of the picture at 1600 px as at 168 px');
// two narrow bodies far from their chips: Left and Right would touch, so the chips fall back to L and R
const nA = rect(0, 0, 6, 30), narrow = { outline: nA, members: [nA, eng(1, 2, 5, 5), rect(7, 0, 13, 30), eng(8, 2, 12, 20, [0, 0, 1])], bbox: [0, 0, 13, 30] };
const npl = PT.plan(narrow);
ok(npl && PT.layout(npl, narrow.bbox, { size: 168, padPt: 2 }).chips.map(c => c.text).join() === 'L,R', 'chips that would touch are shortened to L and R');
// a pair in a tall frame still fits the square: (two bodies one above the other is not side by side, but must not overflow either)
const tA = rect(0, 0, 10, 40), tallPair = { outline: tA, members: [tA, eng(2, 2, 8, 6), rect(14, 0, 24, 40), eng(16, 2, 22, 12, [0, 0, 1])], bbox: [0, 0, 24, 40] };
const tl = PT.layout(PT.plan(tallPair), tallPair.bbox, { size: 168, padPt: 2 });
ok(tl.H <= 168 && tl.W <= 168, 'a tall pair fits inside the size asked for, band included');

/* ── 3 · canvasFor with a recording canvas: one drawCharm call, one scale, the wash only when asked and only when it is safe ── */
const recorder = () => { const log = []; const ctx = new Proxy({}, { get: (t, k) => k === 'canvas' ? ctx._cv : (k in t ? t[k] : (...a) => { log.push([k, a]); }), set: (t, k, v) => { t[k] = v; log.push(['set:' + k, v]); return true; } }); return { log, make: (w, h) => { const cv = { width: w, height: h, getContext: () => ctx }; ctx._cv = cv; return cv; } }; };
let drew = []; const fakeP = { drawCharm: (ctx, c, tx, k) => drew.push({ c, k, p: tx(0, 0) }) };
let r = recorder(); drew = [];
const cv0 = PT.canvasFor(fakeP, pairCharm(), { size: 168, padPt: 2, bg: '#ece7dc', makeCanvas: r.make });
ok(cv0 && cv0.width === L168.W && cv0.height === L168.H, 'the canvas is the layout');
ok(drew.length === 1 && Math.abs(drew[0].k - L168.s) < 1e-9, 'the charm is drawn once, with the layout scale (both bodies at one scale)');
ok(!r.log.some(l => l[0] === 'set:globalAlpha' && l[1] === .72), 'no wash without a highlight');
ok(r.log.filter(l => l[0] === 'fill').length === 2, 'two chips');
r = recorder(); drew = [];
PT.canvasFor(fakeP, pairCharm(), { size: 168, padPt: 2, bg: '#fff', highlight: 'L', makeCanvas: r.make });
const wash = r.log.find(l => l[0] === 'fillRect' && l[1][1] === 0 && l[1][3] === L168.H0);
ok(wash && wash[1][0] > L168.W * .3, 'highlight L washes the RIGHT body (the rectangle starts over the right body)');
ok(r.log.some(l => l[0] === 'set:globalAlpha' && l[1] === .45), 'the washed side\'s chip is faded');
r = recorder();
PT.canvasFor(fakeP, pairCharm(), { size: 168, padPt: 2, bg: '#fff', highlight: 'R', makeCanvas: r.make });
const wash2 = r.log.find(l => l[0] === 'fillRect' && l[1][1] === 0 && l[1][3] === L168.H0);
ok(wash2 && wash2[1][0] < L168.W * .2, 'highlight R washes the LEFT body');
// bodies whose boxes overlap sideways: no wash (it would hide part of the body pointed at)
const lA = rect(0, 0, 20, 26), lap = { outline: lA, members: [lA, eng(2, 2, 10, 6), rect(16, 0, 36, 26), eng(20, 2, 32, 10, [0, 0, 1])], bbox: [0, 0, 36, 26] };
r = recorder(); const lp = PT.plan(lap);
if (lp) { PT.canvasFor(fakeP, lap, { size: 168, padPt: 2, bg: '#fff', highlight: 'L', makeCanvas: r.make }); ok(!r.log.some(l => l[0] === 'fillRect' && l[1][1] === 0 && l[1][3] > 0 && l[1][2] < L168.W && l[1][0] > 0), 'overlapping bodies: no wash'); }
ok(PT.canvasFor(fakeP, oneCharm(), { size: 168, padPt: 2, bg: '#fff', makeCanvas: r.make }) === null, 'a normal charm: canvasFor is null, so the maker runs its old code');
ok(PT.canvasFor(fakeP, pairCharm(), { size: 168, padPt: 2, bg: '#fff' }) === null && PT.canvasFor(null, pairCharm(), { size: 168, makeCanvas: r.make }) === null, 'no canvas maker or no drawer: null, never a throw');

// one ear alone: that body only, at the scale it has in the pair's own picture of this size, with its own single chip
r = recorder(); drew = [];
const cvB = PT.canvasFor(fakeP, pairCharm(), { size: 168, padPt: 2, bg: '#fff', body: 1, makeCanvas: r.make });
ok(cvB && drew.length === 1 && Math.abs(drew[0].k - L168.s) < 1e-9, 'one ear alone is drawn at the scale it has in the pair picture');
ok(drew[0].c.members.length === 2 && drew[0].c.members.includes(oR) && drew[0].c.members.includes(eR) && !drew[0].c.members.includes(oL), 'only the right body\'s own ink is drawn');
ok(cvB.width >= Math.round((24 + 4) * L168.s) && cvB.width < L168.W, 'its picture is as wide as that body (narrower than the pair), chip included');
ok(r.log.filter(l => l[0] === 'fill').length === 1, 'one chip');
const cvA = PT.canvasFor(fakeP, pairCharm(), { size: 168, padPt: 2, bg: '#fff', body: 0, makeCanvas: recorder().make });
ok(cvA.width < cvB.width, 'the smaller left body gives the narrower picture: true relative size is kept between the two ears');
ok(PT.canvasFor(fakeP, oneCharm(), { size: 168, padPt: 2, bg: '#fff', body: 0, makeCanvas: r.make }) === null, 'a normal charm ignores the body option too');
ok(PT.canvasFor(fakeP, pairCharm(), { size: 168, padPt: 2, bg: '#fff', body: 1, highlight: 'L', makeCanvas: recorder().make }) !== null, 'body with a highlight does not throw');
// the markup chip: the same look as the drawn one
const ch = PT.chipHtml('L'), cr = PT.chipHtml('R', { short: true }), c0 = PT.chipHtml(0), c1 = PT.chipHtml(1);
ok(ch.includes('>Left</span>') && ch.includes('data-side="L"') && ch.includes('#2a2724') && cr.includes('>R</span>') && c0 === ch && c1.includes('>Right</span>'), 'chipHtml: Left, Right, L, R, by side or body index');
ok(PT.chipHtml(null) === '' && PT.chipHtml('X') === '' && PT.chipHtml(2) === '', 'chipHtml says nothing for anything that is not an ear');

/* ── 4 · the stored PNG's picture (SVG): the same layout, the chips as vector rects and text ── */
const pic = PT.svgPicture(pl, { bbox: pairCharm().bbox, padPt: 2, size: 168, bg: '#ece7dc', inner: '<path d="M0 0L1 1"/>' });
ok(pic.layout.W === L168.W && pic.layout.H === L168.H, 'the SVG has the canvas layout');
ok(pic.svg.includes(`width="${L168.W}" height="${L168.H}"`) && pic.svg.includes('>Left</text>') && pic.svg.includes('>Right</text>') && pic.svg.includes('Source Sans 3'), 'the SVG carries both chips in the repo font');
ok((pic.svg.match(/<rect /g) || []).length === 3 && (pic.svg.match(/<g /g) || []).length === 1 && pic.svg.endsWith('</svg>'), 'the SVG is one background, one drawing group and two chips');
ok(new RegExp(`scale\\(${Math.round(L168.s * 100) / 100} -${Math.round(L168.s * 100) / 100}\\)`).test(pic.svg), 'the drawing group uses the layout scale');

/* ── 5 · the PNG maker in scripts/index-master.cjs: a normal design keeps today's picture ── */
const IM = require('../../scripts/index-master.cjs');
let Resvg = null;
for (const base of [root, '/tmp/catapply-deps']) {   // (the repo does not carry resvg; a copy another worker installed outside it is used only to look, never installed from here)
  try { const at = require.resolve('@resvg/resvg-js', { paths: [base] }); ({ Resvg } = require(at)); if (base !== root) { process.env.NODE_PATH = path.join(base, 'node_modules') + path.delimiter + (process.env.NODE_PATH || ''); require('module')._initPaths(); } break; } catch (_) {}
}
if (Resvg) {
  const Geom = require('../../charm-nest-geom.js');
  const png = c => IM.thumbnailPng(Geom, c, 168, null);
  const pngPair = png(pairCharm()), pngOne = png(oneCharm());
  ok(pngPair && pngOne, 'both PNGs are made');
  const dim = b => ({ w: b.readUInt32BE(16), h: b.readUInt32BE(20) });
  ok(dim(pngPair).w === L168.W && dim(pngPair).h === L168.H, 'the stored PNG of a pair is the pair layout: ' + JSON.stringify(dim(pngPair)));
  const dOne = dim(pngOne), sOne = 168 / Math.max(24, 30);
  ok(dOne.w === Math.round(24 * sOne) && dOne.h === Math.round(30 * sOne), 'the stored PNG of a normal design is exactly the old size');
  // the same normal design through the code as it was before this change gives the same bytes
  const old = cp.execFileSync('git', ['show', 'origin/main:scripts/index-master.cjs'], { cwd: root, encoding: 'utf8' });
  if (!/charm-nest-pair-thumb/.test(old)) {
    const tmp = path.join(root, 'scripts', '.index-master-before.cjs'); fs.writeFileSync(tmp, old.replace(/\nif \(require\.main === module\)[^\n]*/, '') + '\nmodule.exports.thumbnailPng = thumbnailPng;\n');
    try { const before = require(tmp).thumbnailPng; ok(before, 'the old maker could be loaded'); ok(Buffer.compare(before(Geom, oneCharm(), 168, null), pngOne) === 0, 'a normal design: byte-identical PNG to the code before this change'); } finally { fs.unlinkSync(tmp); }
  }
} else console.log('  – no @resvg/resvg-js here: the stored-PNG checks were not run');

/* ── 6 · wiring (source checks): every maker asks the component, every page loads it, the build ships it ── */
const read = f => fs.readFileSync(path.join(root, f), 'utf8');
ok(/const PT = root\.CharmNestPairThumb, pairCv = PT \? PT\.canvasFor\(\{ drawCharm \}, c, \{ size, padPt: 2, bg: "#ece7dc", makeCanvas \}\) : null;/.test(read('charm-nest-pdf.js')), 'pdf.js thumbnail asks the component first');
ok(/charm-nest-pair\.js\?v=[^']+','charm-nest-pair-thumb\.js\?v=/.test(read('charm-nest-compute-worker.js')), 'the compute worker imports the pair module and the component');
ok(/self\.CharmNestPairThumb[\s\S]{0,200}highlight:a\.opts&&a\.opts\.highlight/.test(read('charm-nest-compute-worker.js')), 'the worker front picture passes the highlight');
ok(/P\.frontPreview=\(c,size,opts\)=>preview\.run\('front',\{charm:charm\(c\),size,opts:/.test(read('charm-nest-background.js')), 'the page proxy passes opts to the worker');
const html = read('charm-nest-1.html'), at = s => html.indexOf(s);
ok(at('charm-nest-pair.js?v=') > 0 && at('charm-nest-pair-thumb.js?v=') > at('charm-nest-pair.js?v=') && at('charm-nest-pair-thumb.js?v=') < at('charm-nest-background.js?v='), 'the sorter page loads the pair module and the component before the picture makers run');
const bp = read('scripts/build-public.cjs');
ok(bp.includes('"charm-nest-pair-thumb.js"') && bp.includes('"charm-nest-pair.js"'), 'the public build ships both files');
const br = read('charm-nest-bridge.js');
ok(/window\.CharmNestPairThumb\?\.canvasFor\(P, charm, \{ size: px, padPt: 3 \* PT, bg: "#fff", highlight: opts && opts\.highlight/.test(br), 'renderFront asks the component first');
ok(br.includes('async function masterFront(entry,size,px,opts)') && br.includes('P.frontPreview(charm,px,opts)') && br.includes('async function masterPreview(entry,size,front,opts)'), 'Pool.masterFront and masterPreview take the highlight option');
ok(br.includes('e.pair && e.pair.mismatched ?') && br.includes('Left + Right pair'), 'the Master tab tile names a pair design');

/* ── 7 · the browser: the real pdf.js thumbnail and the real compute worker draw the picture ── */
(async () => {
  const pwDir = process.env.PW_DIR || (fs.existsSync('/opt/node22/lib/node_modules/playwright/node_modules') ? '/opt/node22/lib/node_modules/playwright/node_modules' : path.join(root, 'node_modules'));
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log(`pairs-masterui: ${n} checks passed (no playwright-core: the browser checks were not run)`); return; }
  const chromePath = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';
  if (!fs.existsSync(chromePath)) { console.log(`pairs-masterui: ${n} checks passed (no chrome: the browser checks were not run)`); return; }
  const oldPdf = cp.execFileSync('git', ['show', 'origin/main:charm-nest-pdf.js'], { cwd: root, encoding: 'utf8' });
  const types = { '.js': 'text/javascript', '.html': 'text/html', '.otf': 'font/otf', '.json': 'application/json' };
  const srv = http.createServer((req, res) => {
    const u = decodeURIComponent(req.url.split('?')[0]);
    if (u === '/old-pdf.js') { res.writeHead(200, { 'Content-Type': 'text/javascript' }); return res.end(oldPdf); }
    if (u === '/blank.html') { res.writeHead(200, { 'Content-Type': 'text/html' }); return res.end('<!doctype html><meta charset=utf-8><body></body>'); }
    const f = path.join(root, u); if (!f.startsWith(root) || !fs.existsSync(f) || fs.statSync(f).isDirectory()) { res.writeHead(404); return res.end(); }
    res.writeHead(200, { 'Content-Type': types[path.extname(f)] || 'application/octet-stream' }); fs.createReadStream(f).pipe(res);
  });
  await new Promise(r => srv.listen(0, '127.0.0.1', r)); const origin = 'http://127.0.0.1:' + srv.address().port;
  const browser = await chromium.launch({ executablePath: chromePath, args: ['--no-sandbox'] });
  try {
    const page = await browser.newPage(), errors = []; page.on('pageerror', e => errors.push(e.message));
    await page.goto(origin + '/blank.html');
    for (const s of ['vendor/pdf-lib-1.17.1.min.js', 'charm-nest-geom.js', 'charm-nest-pair.js', 'charm-nest-pdf.js', 'charm-nest-pair-thumb.js']) await page.addScriptTag({ url: origin + '/' + s });
    const fixtures = { pair: pairCharm(), one: oneCharm(), twin: twinCharm() };
    // pixel reading shared by the checks: ink columns above the band, chip columns in it
    await page.evaluate(() => {
      window.__px = async url => { const im = new Image(); im.src = url; await im.decode(); const cv = document.createElement('canvas'); cv.width = im.width; cv.height = im.height; const c = cv.getContext('2d'); c.drawImage(im, 0, 0); return { w: im.width, h: im.height, d: c.getImageData(0, 0, im.width, im.height).data }; };
      window.__clusters = (img, y0, y1, pred) => { const cols = []; for (let x = 0; x < img.w; x++) { let hit = 0; for (let y = y0; y < y1; y++) { const i = (y * img.w + x) * 4; if (pred(img.d[i], img.d[i + 1], img.d[i + 2], img.d[i + 3])) hit++; } cols.push(hit > 0); } const out = []; let s = -1; for (let x = 0; x <= img.w; x++) { if (x < img.w && cols[x]) { if (s < 0) s = x; } else if (s >= 0) { if (out.length && s - out[out.length - 1][1] < 3) out[out.length - 1][1] = x - 1; else out.push([s, x - 1]); s = -1; } } return out; };
    });
    const thumb = await page.evaluate(async fx => { const o = {}; for (const k of Object.keys(fx)) o[k] = await CharmNestPDF.thumbnail(fx[k], 168); return o; }, fixtures);
    const lay = PT.layout(pl, pairCharm().bbox, { size: 168, padPt: 2 });
    const img = await page.evaluate(async u => { const p = await window.__px(u); return { w: p.w, h: p.h }; }, thumb.pair);
    ok(img.w === lay.W && img.h === lay.H, `the real thumbnail is the pair layout (${img.w} x ${img.h})`);
    const probe = await page.evaluate(async ([u, band]) => {
      const p = await window.__px(u), bg = [0xec, 0xe7, 0xdc], far = (r, g, b) => Math.abs(r - bg[0]) + Math.abs(g - bg[1]) + Math.abs(b - bg[2]) > 90;
      const dark = (r, g, b) => r < 90 && g < 90 && b < 90;
      return { w: p.w, bodies: window.__clusters(p, 0, p.h - band, far), chips: window.__clusters(p, p.h - band, p.h, dark), bandInk: window.__clusters(p, p.h - band, p.h, far) };
    }, [thumb.pair, lay.band]);
    ok(probe.bodies.length === 2, 'the picture shows two bodies side by side: ' + JSON.stringify(probe.bodies));
    const wL = probe.bodies[0][1] - probe.bodies[0][0] + 1, wR = probe.bodies[1][1] - probe.bodies[1][0] + 1;
    ok(Math.abs(wR / wL - 24 / 16) < .12, `true relative scale: the right body is 1.5 times the left in width (${wR} / ${wL})`);
    ok(probe.chips.length === 2, 'two chips under the bodies: ' + JSON.stringify(probe.chips));
    const cc = i => (probe.chips[i][0] + probe.chips[i][1]) / 2, bc = i => (probe.bodies[i][0] + probe.bodies[i][1]) / 2;
    ok(Math.abs(cc(0) - bc(0)) <= 3 && Math.abs(cc(1) - bc(1)) <= 3, `each chip is centred under its body (${cc(0).toFixed(0)}/${bc(0).toFixed(0)}, ${cc(1).toFixed(0)}/${bc(1).toFixed(0)})`);
    const cwL = probe.chips[0][1] - probe.chips[0][0], cwR = probe.chips[1][1] - probe.chips[1][0];
    ok(cwR > cwL * 1.15, `the Right chip is wider than the Left chip (${cwR} / ${cwL}): two different words`);
    // a normal charm and a twin pair are exactly what the old code made
    const old = await browser.newPage(); await old.goto(origin + '/blank.html');
    for (const s of ['vendor/pdf-lib-1.17.1.min.js', 'charm-nest-geom.js']) await old.addScriptTag({ url: origin + '/' + s });
    await old.addScriptTag({ url: origin + '/old-pdf.js' });
    const before = await old.evaluate(async fx => { const o = {}; for (const k of ['one', 'twin', 'pair']) o[k] = await CharmNestPDF.thumbnail(fx[k], 168); return o; }, fixtures);
    ok(before.one === thumb.one, 'a normal charm: the thumbnail is byte-identical to the code before this change');
    ok(before.twin === thumb.twin, 'a matching pair (twin bodies): byte-identical to the code before this change');
    ok(before.pair !== thumb.pair, 'a mismatched pair: not the old picture (it has the chips)');
    // the real compute worker (front picture, white, 3 mm): chips, and the highlight washes the other body
    const workerPics = await page.evaluate(async fx => {
      const w = new Worker('charm-nest-compute-worker.js'); let id = 0;
      const run = (type, input) => new Promise((res, rej) => { const my = ++id; w.onmessage = ({ data }) => { if (data.id !== my || data.progress) return; data.error ? rej(new Error(data.error)) : res(data.result); }; w.postMessage({ id: my, type, input }); });
      const strip = c => Object.fromEntries(['id', 'name', 'bbox', 'outline', 'members', 'strokePt', 'centerPt'].filter(k => c[k] !== undefined).map(k => [k, c[k]]));
      const out = { pair: await run('front', { charm: strip(fx.pair), size: 220 }), hiL: await run('front', { charm: strip(fx.pair), size: 220, opts: { highlight: 'L' } }), hiR: await run('front', { charm: strip(fx.pair), size: 220, opts: { highlight: 'R' } }), one: await run('front', { charm: strip(fx.one), size: 220 }), oneHi: await run('front', { charm: strip(fx.one), size: 220, opts: { highlight: 'L' } }), thumb: await run('thumbnail', { charm: strip(fx.pair), size: 168 }) };
      w.terminate(); return out;
    }, fixtures);
    const inkOf = async (u, x0f, x1f) => page.evaluate(async ([u, x0f, x1f]) => { const p = await window.__px(u); let n = 0; for (let y = 0; y < p.h; y++) for (let x = Math.floor(p.w * x0f); x < Math.floor(p.w * x1f); x++) { const i = (y * p.w + x) * 4; if (p.d[i] < 120 && p.d[i + 1] < 120 && p.d[i + 2] < 120) n++; } return n; }, [u, x0f, x1f]);
    const wl = await inkOf(workerPics.pair, 0, .33), wr = await inkOf(workerPics.pair, .5, 1);
    const hl = await inkOf(workerPics.hiL, .5, 1), hr = await inkOf(workerPics.hiR, 0, .33);
    ok(wl > 40 && wr > 40, 'the worker front picture has ink on both sides');
    ok(hl < wr * .55 && hr < wl * .55, `highlight Left washes the right body out (${hl} of ${wr}), highlight Right washes the left (${hr} of ${wl})`);
    const bodyPics = await page.evaluate(async fx => {
      const w = new Worker('charm-nest-compute-worker.js'); let id = 0;
      const run = (type, input) => new Promise((res, rej) => { const my = ++id; w.onmessage = ({ data }) => { if (data.id !== my || data.progress) return; data.error ? rej(new Error(data.error)) : res(data.result); }; w.postMessage({ id: my, type, input }); });
      const strip = c => Object.fromEntries(['id', 'name', 'bbox', 'outline', 'members', 'strokePt', 'centerPt'].filter(k => c[k] !== undefined).map(k => [k, c[k]]));
      const o = { both: await run('front', { charm: strip(fx.pair), size: 220 }), b0: await run('front', { charm: strip(fx.pair), size: 220, opts: { body: 0 } }), b1: await run('front', { charm: strip(fx.pair), size: 220, opts: { body: 1 } }) };
      w.terminate(); return o;
    }, fixtures);
    const sizeOf = async u => page.evaluate(async u => { const p = await window.__px(u); return { w: p.w, h: p.h }; }, u);
    const colsOf = async (u, band) => page.evaluate(async ([u, band]) => { const p = await window.__px(u); return window.__clusters(p, 0, p.h - band, (r, g, b) => r < 140 && g < 140 && b < 140 || (r > 200 && g < 90) || (b > 200 && r < 90)); }, [u, band]);
    const bandOf = PT.layout(pl, pairCharm().bbox, { size: 220, padPt: 3 * 72 / 25.4 }).band;
    const cBoth = await colsOf(bodyPics.both, bandOf), c0 = await colsOf(bodyPics.b0, bandOf), c1b = await colsOf(bodyPics.b1, bandOf);
    ok(cBoth.length === 2 && c0.length === 1 && c1b.length === 1, 'each ear alone shows one body');
    const wid = c => c[0][1] - c[0][0] + 1;
    ok(Math.abs(wid(c0) - (cBoth[0][1] - cBoth[0][0] + 1)) <= 2 && Math.abs(wid(c1b) - (cBoth[1][1] - cBoth[1][0] + 1)) <= 2, 'an ear alone is drawn at its scale in the pair picture (' + wid(c0) + '/' + wid(c1b) + ' vs ' + (cBoth[0][1] - cBoth[0][0] + 1) + '/' + (cBoth[1][1] - cBoth[1][0] + 1) + ')');
    ok((await sizeOf(bodyPics.b0)).w < (await sizeOf(bodyPics.b1)).w, 'the left ear\'s picture is narrower than the right ear\'s');
    ok(workerPics.one === workerPics.oneHi, 'a normal charm ignores a highlight: the very same picture');
    ok(workerPics.thumb === thumb.pair, 'the worker thumbnail of the pair equals the page thumbnail');
    ok(!errors.length, 'no page errors: ' + errors.join(' | '));
  } finally { await browser.close(); srv.close(); }
  console.log(`pairs-masterui: ${n} checks passed`);
})().catch(e => { console.error(e); process.exit(1); });
