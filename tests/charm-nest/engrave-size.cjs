// Paul, 10 Oct 2026, the Engrave editor, order 4174565371 (BEADY, a bar charm, "Stuart"): "I can not seem to enlarge the text any
// bigger than the shown size." The header said "SMALL . cap 1.57 mm". That number was not a laser limit: it was Settings "Max height
// (fraction)" (0.4), a taste cap for the AUTOMATIC first placement, applied to the back view's HEIGHT and also as the ceiling of the
// corner handles (resize, the live drag preview). A flat bar's height is its short side, so the automatic size sat exactly on the
// ceiling and the handles could never go past it, on a bar that takes a cap of 3.8 mm.
// Now the words are enlarged by hand as far as they verify on the eroded mask (nothing outside the metal, nothing over a cut-out or
// keep-out or inside the margin of a cut line) and, past that, they stand at the largest that fits and say so. Offline: the real
// geometry and the real editor functions of charm-nest-bridge.js, no browser, no network. (A real browser was not available.)
const assert = require('node:assert/strict'), fs = require('node:fs'), vm = require('node:vm');
const G = require('../../charm-nest-geom.js'), Fit = require('../../charm-nest-engrave-fit.js'), ot = require('../../vendor/opentype-1.3.4.min.js');
const fonts = { Regular: ot.loadSync('vendor/fonts/SourceSans3-Regular.otf'), Semibold: ot.loadSync('vendor/fonts/SourceSans3-Semibold.otf') };
const source = fs.readFileSync('charm-nest-bridge.js', 'utf8');
const MM = 25.4 / 72, PT = 72 / 25.4;

// a bar as the screenshot shows it: 30 mm long, a 4.5 mm body, a hoop at each top corner
const rect = (x0, y0, x1, y1) => [['m', [x0, y0]], ['l', [x1, y0]], ['l', [x1, y1]], ['l', [x0, y1]], ['l', [x0, y0]]];
const circ = (cx, cy, r) => { const k = 0.5523 * r; return [['m', [cx + r, cy]], ['c', [cx + r, cy + k], [cx + k, cy + r], [cx, cy + r]], ['c', [cx - k, cy + r], [cx - r, cy + k], [cx - r, cy]], ['c', [cx - r, cy - k], [cx - k, cy - r], [cx, cy - r]], ['c', [cx + k, cy - r], [cx + r, cy - k], [cx + r, cy]]]; };
const bbOf = sub => { let x0 = 1e9, y0 = 1e9, x1 = -1e9, y1 = -1e9; for (const s of sub) for (let i = 1; i < s.length; i++) { const p = s[i]; x0 = Math.min(x0, p[0]); y0 = Math.min(y0, p[1]); x1 = Math.max(x1, p[0]); y1 = Math.max(y1, p[1]); } return [x0, y0, x1, y1]; };
const mem = sub => ({ kind: 'path', layer: 'CUT', stroke: true, fill: false, strokeRGB: [0, 0, 0], fillRGB: [0, 0, 0], lwPt: 0.25, paintOp: 'S', closed: true, subpaths: [sub], bbox: bbOf(sub) });
const W = 30 * PT, H = 4.5 * PT;
const bar = () => { const o = mem(rect(0, 0, W, H)), hs = [circ(1.4 * PT, H - 0.3 * PT, 1.0 * PT), circ(W - 1.4 * PT, H - 0.3 * PT, 1.0 * PT)].map(mem); return { outline: o, members: [o, ...hs], bbox: o.bbox, widthPt: o.bbox[2] - o.bbox[0], heightPt: o.bbox[3] - o.bbox[1] }; };
const settings = { engraveMaxHeightFrac: 0.4, engraveMinCapMm: 1.6, engraveMinStrokeMm: 0.15, engraveMinGapMm: 0.12, engraveMarginMm: 0.3 };
const first = (lines, keepOut = []) => Fit.calculate({ charm: bar(), lines, lineMode: 'auto', viewOptions: { res: 6 }, maskOptions: { marginMm: settings.engraveMarginMm, keepOut },
  opts: { minStrokeMm: settings.engraveMinStrokeMm, minGapMm: settings.engraveMinGapMm, minCapMm: 1.6, maxHeightFrac: 0.4, lineGap: 0.216, tryRotated: true } }, fonts, G);

// the editor's own functions, taken out of the bridge as the other editor tests do
const grab = (a, b) => { const i = source.indexOf(a), j = source.indexOf(b, i); assert(i > 0 && j > i, 'bridge section not found: ' + a); return source.slice(i, j); };
function editor(job, toasts) {
  const frames = new Map(), handlers = {}, paints = [], capNode = { textContent: '' }; let frameId = 0;
  const ctx = { G, S: { settings }, PT, job, fontFor: w => fonts[w] || fonts.Regular, toast: (m, kind) => toasts.push({ m, kind }), refresh() {}, reRead() {},
    card: { querySelector: q => q === '[data-cap]' ? capNode : null }, requestAnimationFrame: fn => { frames.set(++frameId, fn); return frameId; }, cancelAnimationFrame: id => frames.delete(id) };
  vm.createContext(ctx);
  vm.runInContext(grab('  const fitOpts =', '  const items =') + '\nthis.fitOpts=fitOpts;', ctx);                                  // the real fitOpts, with the real Settings keys
  vm.runInContext(grab('  const capText =', '  function refit(') + '\nthis.capText=capText;', ctx);
  vm.runInContext(grab('  function refit(job, place)', '  /** New words on a placement'), ctx);
  vm.runInContext(grab('  function resize(job, size)', '  function setLineSpacing('), ctx);
  vm.runInContext(grab('  function moveTo(job, centre)', '  /** Turn the text about its centre.'), ctx);
  vm.runInContext(grab('  function rotateTo(job, angle)', '  /** The box around the text'), ctx);
  const canvas = { width: 100, height: 100, _map: { k: 1, tx: (x, y) => [x, 100 - y] }, _box: { rotate: [-100, -100], corners: [[90, 10]], centrePx: [50, 50] }, style: {}, classList: { add() {}, remove() {} },
    setPointerCapture() {}, getBoundingClientRect: () => ({ left: 0, top: 0, width: 100, height: 100 }), _paint: p => paints.push(p), addEventListener: (n, f) => (handlers[n] ||= []).push(f) };
  vm.runInContext(grab('    const wire = bc => {', '    const mountBack =') + '\nthis.wire=wire;', ctx);
  ctx.wire(canvas);
  const emit = (n, x, y, extra) => handlers[n].forEach(f => f({ clientX: x, clientY: y, pointerId: 1, preventDefault() {}, ...extra }));
  // pull the corner handle: the pointer goes `k` times as far from the text's centre as it was when it was taken
  const pull = k => { emit('pointerdown', 60, 50, { shiftKey: true }); emit('pointermove', 50 + 10 * k, 50); const tick = [...frames.values()][0]; frames.clear(); tick(); const shown = paints.at(-1); emit('pointerup', 50 + 10 * k, 50); return { shown, readout: capNode.textContent }; };
  return { ctx, pull, paints };
}
const inkOf = fit => fit.cmds || fit.glyphs.flatMap(g => g.cmds);
const polyPoints = cmds => G.glyphPolys(cmds, 8).flat();

// 1 · the cause, reproduced: the automatic size sits exactly on the old ceiling, flagged SMALL, "cap 1.57 mm"
const r = first(['Stuart']), mask = r.mask, hPt = mask.hPt;
assert(r.fit && r.check.ok);
assert(r.fit.small, 'the automatic size is flagged SMALL (the screenshot)');
assert(Math.abs(r.fit.capMm - 1.57) < 0.03, 'cap 1.57 mm as in the screenshot (' + r.fit.capMm.toFixed(2) + ')');
assert(Math.abs(r.fit.size - 0.4 * hPt) < 0.01, 'and it is exactly the old ceiling, 0.4 x the back height');
const oldCapMm = r.fit.capMm, oldSize = r.fit.size;
const job = () => ({ state: 'review', lineInput: ['Stuart'], lines: ['Stuart'], lineMode: 'auto', lineGap: 0.216, mask, wantSize: oldSize, fit: { ...r.fit } });

// 2 · by hand: Settings still rule the automatic placement, the hand is bounded only by the metal
{
  const j = job(), toasts = [], { ctx } = editor(j, toasts);
  assert.equal(ctx.fitOpts(j).maxHeightFrac, 0.4, 'the automatic first fit still uses the Settings fraction');
  assert(ctx.fitOpts(j, true).maxHeightFrac * hPt >= 4 * Math.max(mask.wPt, hPt) - 1e-9, 'by hand the searches start from a size no text reaches');
  const asked = 2 * oldSize;                                                 // twice the shown size: past the old cap, well inside the metal
  ctx.resize(j, asked);
  assert(Math.abs(j.fit.size - asked) < 1e-9, 'enlarged to what was asked (' + j.fit.size.toFixed(2) + ' of ' + asked.toFixed(2) + ' pt)');
  assert(j.fit.capMm > 2 * oldCapMm * 0.99 && j.fit.capMm > 3, 'cap ' + j.fit.capMm.toFixed(2) + ' mm, past the old 1.57');
  assert(!j.fit.small && !j.fit.atLimit && toasts.length === 0, 'no longer SMALL, and no message when nothing stopped it');
  assert(G.verifyInk(j.fit.cmds, mask).ok, 'every ink pixel is on the usable metal');
  assert.equal(j.wantSize, asked);
}

// 3 · past the metal: it stops at the largest that fits, with the reason; the ink stays inside the charm
let fittedCapMm;
{
  const j = job(), toasts = [], { ctx } = editor(j, toasts);
  ctx.resize(j, 6 * oldSize);                                                // far beyond the charm
  const f = j.fit, font = fonts[f.weight] || fonts.Regular;
  assert(f.atLimit && f.size < 6 * oldSize, 'stopped short of the request');
  assert(G.verifyInk(f.cmds, mask).ok, 'the ink is inside the engraving area');
  const more = G.layoutLines(j.lines, font, f.size + 0.2, 0.216, f.angle, f.centre);
  assert(!G.verifyInk(more.cmds, mask).ok, 'and it is the LARGEST that fits: 0.2 pt more would leave the metal');
  const bb = f.layout.bbox, inner = (H - 2 * 0.3 * PT);
  assert(bb[3] - bb[1] > 0.9 * inner && bb[3] - bb[1] <= inner + 0.2, 'the lettering fills the bar body between its margins (' + ((bb[3] - bb[1]) * MM).toFixed(2) + ' of ' + (inner * MM).toFixed(2) + ' mm)');
  assert(f.capMm >= 3.5 && f.capMm > 2 * oldCapMm, 'the new largest cap on this bar is ' + f.capMm.toFixed(2) + ' mm (was ' + oldCapMm.toFixed(2) + ')');
  assert.equal(toasts.length, 1); assert.match(toasts[0].m, /as large as these words fit on this charm/); assert.match(toasts[0].m, new RegExp(f.capMm.toFixed(2)));
  assert.equal(j.wantSize, f.size, 'the request is not kept: a later move does not spring to an impossible size');
  fittedCapMm = f.capMm;
  const text = ctx.capText(f); assert.match(text, /^3\.\d\d mm · largest that fits$/, 'the readout says why it stops: ' + text);
  ctx.resize(j, 0.8 * f.size); assert(!j.fit.atLimit && j.fit.size < f.size && ctx.capText(j.fit) === j.fit.capMm.toFixed(2) + ' mm', 'smaller again: the note goes');
}

// 4 · the corner handle itself (the real pointer code): live preview, readout, release
{
  const j = job(), toasts = [], { ctx, pull, paints } = editor(j, toasts);
  let out = pull(2);                                                        // twice as far: inside the metal
  assert(inkOf(out.shown).length && G.verifyInk(out.shown.glyphs.flatMap(g => g.cmds), mask).ok, 'the preview is inside the metal');
  assert(Math.abs(j.fit.size - 2 * oldSize) < 1e-6 && j.fit.capMm > 3, 'released at twice the size: cap ' + j.fit.capMm.toFixed(2) + ' mm');
  assert.equal(toasts.length, 0); assert.doesNotMatch(out.readout, /largest/);
  out = pull(10);                                                           // ten times as far: past the charm
  assert(G.verifyInk(out.shown.glyphs.flatMap(g => g.cmds), mask).ok, 'the preview never leaves the metal');
  assert.match(out.readout, /largest that fits/, 'the readout beside the controls says it while dragging: ' + out.readout);
  assert(Math.abs(j.fit.capMm - fittedCapMm) < 0.05, 'released at the largest: ' + j.fit.capMm.toFixed(2) + ' mm'); assert.equal(toasts.length, 1);
  assert(j.fit.size < 10 * 2 * oldSize);
}

// 5 · a keep-out in the middle of the bar still bounds the words: enlarging never covers it
{
  const keep = mem(rect(W / 2 - 3 * PT, 0, W / 2 + 3 * PT, H));              // a 6 mm strip across the bar's centre
  const k = first(['Stuart'], [keep]);
  assert(k.fit && k.check.ok);
  const j = { state: 'review', lineInput: ['Stuart'], lines: ['Stuart'], lineMode: 'auto', lineGap: 0.216, mask: k.mask, wantSize: k.fit.size, fit: { ...k.fit } };
  const toasts = [], { ctx } = editor(j, toasts);
  ctx.resize(j, 6 * k.fit.size);
  assert(j.fit.atLimit, 'stopped by the metal');
  assert(G.verifyInk(j.fit.cmds, k.mask).ok);
  const cx0 = (k.mask.cx), kx0 = cx0 - 3 * PT, kx1 = cx0 + 3 * PT;
  assert(polyPoints(j.fit.cmds).every(([x]) => x <= kx0 || x >= kx1), 'no ink over the keep-out');
  assert.equal(G.at(k.mask, cx0, k.mask.cy), 0, 'the keep-out is not usable metal');
}

// 6 · what is saved is left alone: opening a saved back keeps its size and only raises the ceiling it may be enlarged to
{
  const open = grab('      const font=fontFor(saved.weight,job),layout=', '        job.verify={geometry:check');
  assert(open.includes('fitOpts(job,true)') && open.includes('size:saved.sizePt'), 'the saved size is kept; only the ceiling uses the hand limit');
  assert(!/maxHeightFrac/.test(grab('  async function prepareApproval(job, by, at) {', '  /* Paul, 3 Oct 04:03')), 'approval has no size cap of its own: it re-verifies the ink');
}
console.log('Engrave size OK: bar charm cap 1.57 mm -> up to ' + fittedCapMm.toFixed(2) + ' mm by hand, stops at the metal with a message, keep-out respected, saved sizes untouched');
