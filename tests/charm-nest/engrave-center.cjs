// The Engraving card's back preview puts the piece in the middle of its work area (Paul, 10 Oct 2026, order 4174565371: a wide
// bar "is not properly centred on the screen and it's right up top, which makes it look squished").
//   node tests/charm-nest/engrave-center.cjs
// Two things frame it, and this test holds both:
//  1. renderBack(job, [w, h]) draws the charm's outline, the text's box and the turning handle with 1 mm around them, in a canvas of
//     exactly that shape: the drawn box's centre is the canvas centre and the four margins are equal, for wide, tall, round and turned
//     pieces alike (real engraving geometry: the real back view, mask, fit and renderBack source, no browser);
//  2. the canvas then sits in the middle of the work area (.backHost) both ways. It used to hang from the top edge (align-self:flex-start),
//     so a wide piece left the lower half of the frame empty.
const assert = require('node:assert/strict'), fs = require('node:fs'), vm = require('node:vm'), path = require('node:path');
const root = path.join(__dirname, '../..');
const G = require(path.join(root, 'charm-nest-geom.js')), Fit = require(path.join(root, 'charm-nest-engrave-fit.js')), T = require(path.join(root, 'charm-nest-text.js'));
const ot = require(path.join(root, 'vendor/opentype-1.3.4.min.js'));
const { CharmNestPDF: P } = require(path.join(root, 'netlify/functions/_charmNestPdf.js'));
const PT = 72 / 25.4, MM = 25.4 / 72;

// the fonts the editor fits with
const bytes = f => { const b = fs.readFileSync(path.join(root, f)); return b.buffer.slice(b.byteOffset, b.byteOffset + b.byteLength); };
const emoji = ot.parse(bytes('vendor/fonts/NotoEmoji-Regular.ttf')), emojiMap = require(path.join(root, 'vendor/fonts/emoji-sequences.json'));
const fonts = Object.fromEntries(['Regular', 'Semibold'].map(w => [w, T.withEmoji(ot.parse(bytes(`vendor/fonts/SourceSans3-${w}.otf`)), emoji, emojiMap, ot.Path)]));

// renderBack as the editor ships it, on a canvas that records nothing
const src = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8');
const cut = (a, b) => { const i = src.indexOf(a), j = src.indexOf(b, i); assert(i > 0 && j > i, `bridge source markers: ${a}`); return src.slice(i, j); };
const noop = () => {};
const fakeCanvas = () => { const ctx = new Proxy({}, { get: (_, k) => (k === 'createImageData' ? (w, h) => ({ data: new Uint8ClampedArray(w * h * 4) }) : noop), set: () => true }); return { width: 0, height: 0, getContext: () => ctx }; };
const env = vm.createContext({ G, P, PT, MM, Math, Array, Infinity, document: { createElement: () => fakeCanvas() }, console });
vm.runInContext(cut('  function textBox(glyphs', '  function resize(job, size)') + cut('  /* ── 7.6 · back files', '  function renderFront(') + '\nthis.renderBack = renderBack;', env);

// charms: a polyline outline with round cut-outs, as the sorter hands them over
const ring = (cx, cy, r) => [...Array(32).keys()].map(i => [cx + r * Math.cos(i / 32 * 2 * Math.PI), cy + r * Math.sin(i / 32 * 2 * Math.PI)]);
const rect = (x, y, w, h) => [[x, y], [x + w, y], [x + w, y + h], [x, y + h]];
const poly = pts => pts.map((p, i) => [i ? 'l' : 'm', p]).concat([['h']]);
const path_ = pts => { const xs = pts.map(p => p[0]), ys = pts.map(p => p[1]); return { kind: 'path', layer: 'CUT', closed: true, stroke: true, strokeRGB: [1, 0, 0], subpaths: [poly(pts)], bbox: [Math.min(...xs), Math.min(...ys), Math.max(...xs), Math.max(...ys)] }; };
const charm = (outline, holes) => { const o = path_(outline); return { outline: o, members: [o, ...holes.map(path_)], bbox: o.bbox }; };
const designs = {
  'wide bar with a hoop at each end (the order)': { c: charm(rect(0, 0, 92, 17), [ring(6.5, 8.5, 3), ring(85.5, 8.5, 3)]), text: 'Stuart' },
  'round disc': { c: charm(ring(30, 30, 24), [ring(30, 51, 2)]), text: 'Anna' },
  'long pendant (the long-side rule lays it flat)': { c: charm(rect(0, 0, 22, 62), [rect(9, 55, 4, 4)]), text: 'Joe' },
  'tall pendant (saved upright)': { c: charm(rect(0, 0, 22, 62), [rect(9, 55, 4, 4)]), text: 'Joe', view: { editingBack: true, savedUp: 90 } },
  'bar turned upright (saved up 0)': { c: charm(rect(0, 0, 92, 17), [ring(6.5, 8.5, 3), ring(85.5, 8.5, 3)]), text: 'Stuart', view: { editingBack: true, savedUp: 0 } },
  'bar turned 45 degrees (saved up 45)': { c: charm(rect(0, 0, 92, 17), [ring(6.5, 8.5, 3), ring(85.5, 8.5, 3)]), text: 'Stuart', view: { editingBack: true, savedUp: 45 } },
  'wide bar, words turned 90': { c: charm(rect(0, 0, 92, 17), [ring(6.5, 8.5, 3), ring(85.5, 8.5, 3)]), text: 'Stu', angle: 90 },
  'tall pendant, words turned 90': { c: charm(rect(0, 0, 22, 62), [rect(9, 55, 4, 4)]), text: 'Joe', angle: 90, view: { editingBack: true, savedUp: 90 } }
};
const hosts = [[463, 350], [926, 1000], [300, 700], [640, 640]];   // [width, height] of the work area, as mountBack measures it

const prepare = d => {
  const res = Fit.calculate({ charm: d.c, lines: [d.text], lineMode: 'auto', viewOptions: G.viewOptionsFor(d.view || {}), maskOptions: { marginMm: .8, keepOut: [] },
    opts: { minCapMm: 1.6, maxHeightFrac: .4, lineGap: .216, minStrokeMm: 0, minGapMm: 0, tryRotated: false } }, fonts, G);
  assert(res.fit, `${d.text}: the words fit: ${res.reason}`);
  let fit = res.fit;
  if (d.angle != null) { fit = G.reflowAt([d.text], fonts.Regular, res.mask, { minCapMm: 1.6, maxHeightFrac: .4, lineGap: .216 }, { centre: fit.centre, angle: d.angle, size: fit.size }, 'auto'); assert(fit.ok, 'the words fit turned'); }
  return { view: res.view, mask: res.mask, fit };
};

// the drawn box, in canvas pixels: the outline as the editor draws it, the text's box corners and the turning handle
const drawn = (cv, job) => {
  const pts = [];
  for (const m of job.view.members) for (const ring2 of G.flatten(m, 16)) for (const p of ring2) pts.push(cv._map.tx(p[0], p[1]));
  assert(cv._box, 'the editor draws the text box');
  pts.push(...cv._box.corners, cv._box.rotate);
  const xs = pts.map(p => p[0]), ys = pts.map(p => p[1]);
  return { x0: Math.min(...xs), x1: Math.max(...xs), y0: Math.min(...ys), y1: Math.max(...ys) };
};

let checked = 0, worst = 0;
for (const [name, d] of Object.entries(designs)) {
  const job = prepare(d);
  for (const host of hosts) {
    const cv = env.renderBack(job, host, { grid: true, hatch: false, editable: true });
    const b = drawn(cv, job), W = cv.width, H = cv.height, why = `${name} in ${host.join('x')}`;
    const pad = PT * cv._map.k;                                      // 1 mm in pixels
    const left = b.x0, right = W - b.x1, top = b.y0, bottom = H - b.y1;
    const off = [(b.x0 + b.x1) / 2 - W / 2, (b.y0 + b.y1) / 2 - H / 2];
    worst = Math.max(worst, Math.abs(off[0]), Math.abs(off[1]));
    assert(Math.abs(off[0]) <= 1 && Math.abs(off[1]) <= 1, `${why}: the drawn box is centred in the canvas (off by ${off.map(v => v.toFixed(2))} px)`);
    for (const [side, v] of [['left', left], ['right', right], ['top', top], ['bottom', bottom]]) assert(Math.abs(v - pad) <= 1, `${why}: ${side} margin is 1 mm (${v.toFixed(1)} px, 1 mm = ${pad.toFixed(1)} px)`);
    // …and the frame is used: the canvas never outgrows the box it was given, and one side fills it
    assert(W <= host[0] && H <= host[1], `${why}: the canvas (${W}x${H}) fits its box`);
    assert(W >= host[0] - 2 || H >= host[1] - 2, `${why}: the canvas (${W}x${H}) fills the box on its tight side`);
    checked++;
  }
}

// the canvas takes the piece's shape: a wide bar a wide strip, an upright pendant a tall one
{
  const bar = env.renderBack(prepare(designs['wide bar with a hoop at each end (the order)']), [926, 1000], { hatch: false, editable: true }),
    tall = env.renderBack(prepare(designs['tall pendant (saved upright)']), [926, 1000], { hatch: false, editable: true });
  assert(bar.width > 2.5 * bar.height, `a wide bar is a wide canvas (${bar.width}x${bar.height})`);
  assert(tall.height > 1.2 * tall.width, `an upright pendant is a tall canvas (${tall.width}x${tall.height})`);
}

// the canvas sits in the middle of its work area, both ways: the rule that decides where a shorter canvas goes
{
  const html = fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8');
  const rule = html.match(/\.rvItem\.full \.backHost canvas\{([^}]*)\}/);
  assert(rule, 'the editor canvas has its layout rule');
  assert(/align-self:\s*center/.test(rule[1]) && !/flex-start|flex-end/.test(rule[1]), `the editor canvas is centred in the work area: ${rule[1]}`);
  const host = html.match(/\.rvItem\.full \.backHost\{([^}]*)\}/);
  assert(host && /justify-content:\s*center/.test(host[1]), 'and across it');
  assert(!/\.backHost[^{]*\{[^}]*align-self:\s*flex-start/.test(html), 'nothing else hangs the drawing from the top edge');
}
console.log(`Engrave centre OK: ${checked} frames (${Object.keys(designs).length} shapes x ${hosts.length} work areas) centred within ${worst.toFixed(2)} px with even 1 mm margins; the canvas is centred in its work area`);
