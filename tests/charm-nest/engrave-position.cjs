/* ENGPOS (12 Oct 2026): where the words sit on the charm. Offline: the repo's own geometry, fit, PDF writer and the bridge's own nudge / resize / refit (taken out of its source); no browser, no network,
 * no paid reading, no live write. Names are "Name One".
 *
 *   node tests/charm-nest/engrave-position.cjs
 *
 *  A  a face nobody moved is what it always was: the exported laser file is byte for byte the same after nudge(0,0), after a snapshot taken and put back, after a move that is taken back (Cancel),
 *     and after the checkpoint's copy of the job (structuredClone) - and loading the module changes nothing
 *  B  a nudge moves the words in the exported laser file by exactly the millimetres asked (dx right, dy up), for a back and for a front; the size, the lines and the weight stay
 *  C  the safe area: a move that would leave it stops at the edge (clamped, to 0.05 mm), one more step is refused and the fit is not touched; the words never shrink to squeeze in;
 *     centre() goes to the middle of the metal; size() grows/shrinks and says when the metal is the limit
 *  D  the back and a mirrored Right ear: the same call moves the words the way each picture shows; mirrorDx makes a Left and a Right ear move as exact mirror images on the charm
 *  E  of(job): the words' place in a picture of the charm (front and back, any hoop-up turn), the angle, auto
 *  F  locked / not ready jobs: a decided or fit-less job refuses (nothing touched); restore refuses a job that was fitted again
 */
'use strict';
const assert = require('node:assert/strict'), fs = require('node:fs'), vm = require('node:vm'), path = require('node:path'), crypto = require('node:crypto');
const root = path.join(__dirname, '..', '..');
const rq = f => require(path.join(root, f));

/* a frozen clock and a seeded Math.random: pdf-lib stamps dates and random names, nothing else varies a file by */
const FIXED = Date.UTC(2026, 9, 12, 12, 0, 0), RealDate = Date;
global.Date = class extends RealDate { constructor(...a) { if (a.length) super(...a); else super(FIXED); } static now() { return FIXED; } };
let seed = 1; const reseed = () => { seed = 20261012; };
Math.random = () => { seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0; return seed / 4294967296; };

const { CharmNestPDF: P, PDFLib: L } = rq('netlify/functions/_charmNestPdf.js');
const G = rq('charm-nest-geom.js'), Fit = rq('charm-nest-engrave-fit.js'), T = rq('charm-nest-text.js'), Pair = rq('charm-nest-pair.js'), ot = rq('vendor/opentype-1.3.4.min.js');
global.JSZip = rq('vendor/jszip-3.10.1.min.js'); global.CharmNestAssets = rq('charm-nest-assets.js'); global.CharmNestBacks = rq('charm-nest-backs.js');
const E = global.CharmNestExport = rq('charm-nest-export.js');
const PT = 72 / 25.4, MM = 25.4 / 72;
let n = 0; const ok = (c, m) => { assert.ok(c, m); n++; };
const eq = (a, b, m) => { assert.deepEqual(a, b, m); n++; };
const near = (a, b, e, m) => { assert.ok(Math.abs(a - b) <= e, `${m || 'near'}: ${a} vs ${b} (±${e})`); n++; };
const sha = b => crypto.createHash('sha256').update(Buffer.from(b)).digest('hex');

/* ── the fonts the editor fits with ── */
const bytes = f => { const b = fs.readFileSync(path.join(root, f)); return b.buffer.slice(b.byteOffset, b.byteOffset + b.byteLength); };
const emoji = ot.parse(bytes('vendor/fonts/NotoEmoji-Regular.ttf')), emojiMap = rq('vendor/fonts/emoji-sequences.json');
const fonts = Object.fromEntries(['Regular', 'Semibold'].map(w => [w, T.withEmoji(ot.parse(bytes(`vendor/fonts/SourceSans3-${w}.otf`)), emoji, emojiMap, ot.Path)]));

/* ── the bridge's own nudge / resize / refit, out of its source, with the page's pieces stubbed ── */
const source = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8');
const grab = (a, b) => { const i = source.indexOf(a), j = source.indexOf(b, i); assert(i > 0 && j > i, 'bridge section not found: ' + a); return source.slice(i, j); };
const toasts = [], schedules = { n: 0 };
const reopened = [];   // (NODECIDED: the first change of an approved / saved piece is made by reopenForEdit; here a stand-in that records the call and puts the job back to review, as the page's does)
const ctx = { G, S: { settings: {} }, PT, MM, Math, fontFor: w => fonts[w] || fonts.Regular, toast: (m, kind) => toasts.push({ m, kind }), refresh() {}, reRead() {}, Session: { schedule() { schedules.n++; } }, reopenForEdit: job => { reopened.push(job.key); job.state = 'review'; return true; } };
vm.createContext(ctx);
vm.runInContext(grab('  const startEdit =', '\n') + grab('  const fitOpts =', '  const items =') + grab('  function refit(job, place)', '  /** New words on a placement') + grab('  function nudge(job, dxMm, dyMm)', '  /** The middle of the area') + grab('  function resize(job, size)', '  function setLineSpacing(')
  + '\nthis.Engrave = { nudge, resize };', ctx);
global.Engrave = ctx.Engrave; global.CharmNestGeom = G; global.Session = ctx.Session;
const before = { sha: sha(Buffer.from(fs.readFileSync(path.join(root, 'charm-nest-engrave-position.js')))) };
const Pos = rq('charm-nest-engrave-position.js');

/* ── a charm with no mirror symmetry (a mitten) and its Right-ear mirror, written as a master and read back, like the ENGEXPORT test ── */
const S4 = 4;
const THUMB = [[6, 0], [24, 0], [24, 30], [6, 30], [6, 28], [0, 28], [0, 18], [6, 18]].map(p => p.map(v => v * S4)), HOLE = [[14, 20], [18, 20], [18, 24], [14, 24]].map(p => p.map(v => v * S4));
const pathOps = (pts, close) => pts.map((p, i) => p[0] + ' ' + p[1] + (i ? ' l' : ' m')).join(' ') + (close ? ' h' : '');
async function vector(name, w, h, ops) {
  const d = await L.PDFDocument.create(), p = d.addPage([w, h]); p.node.normalize();
  const ref = d.context.register(d.context.obj({ Type: 'OCG', Name: L.PDFString.of(name) }));
  p.node.Resources().set(L.PDFName.of('Properties'), d.context.obj({ art: ref }));
  p.node.addContentStream(d.context.register(d.context.flateStream('/OC /art BDC ' + ops + ' EMC')));
  d.catalog.set(L.PDFName.of('OCProperties'), d.context.obj({ OCGs: [ref], D: { Order: [ref], ON: [ref] } }));
  return d.save({ useObjectStreams: false });
}
let MITTEN = null;
async function mitten() {
  if (MITTEN) return MITTEN;
  const src = await P.parseSource(await vector('Artwork', 160, 160, '0 0 0 RG 0.25 w ' + pathOps(THUMB, true) + ' S ' + pathOps(HOLE, true) + ' S'), 'mitten');
  return (MITTEN = Object.assign(P.groupCharms(src).charms[0], { id: 'm', sourceId: 's', name: 'MITTEN', centerPt: [48, 60], strokePt: .25 }));
}
const charmFor = (base, o) => Object.assign({}, o.mirror ? Pair.pieceGeometry(base, { side: 'R', bodyIndex: 0, mirror: true }) : base, { id: o.id, poolId: o.poolId, name: o.id, side: o.side || null });

/* the job of one face of one piece, fitted the way the page fits it (Fit.calculate = the worker's own function), in state "review" */
function jobOf(charm, o) {
  const face = o.face || 'back';
  const res = Fit.calculate({ charm, lines: ['Ann'], lineMode: 'auto', viewOptions: G.viewOptionsFor(face === 'front' ? { face: 'front' } : {}), maskOptions: { marginMm: .8, keepOut: [] },
    opts: { minCapMm: 1.6, maxHeightFrac: .4, lineGap: .216, minStrokeMm: 0, minGapMm: 0, tryRotated: false } }, fonts, G);
  assert(res.fit, 'the words fit: ' + res.reason);
  // (the automatic size can sit right on the metal's limit; half of it leaves room to move, as a longer-worded or smaller-lettered job has)
  const small = G.reflowAt(['Ann'], fonts.Regular, res.mask, { minCapMm: 1.6, maxHeightFrac: .4, lineGap: .216 }, { centre: res.fit.centre, angle: res.fit.angle, size: res.fit.size * (o.shrink || 0.5) }, 'auto');
  assert(small.ok, 'the smaller words fit');
  res.fit = Object.assign({}, res.fit, small, { weight: res.fit.weight, fittedMax: res.fit.size }); res.lines = small.lines;
  return { key: `4195966039_t1${o.slot ? '#' + o.slot : ''}${face === 'front' ? '~front' : ''}`, slot: o.slot || null, state: 'review', face, charm, view: res.view, mask: res.mask, fit: res.fit, lineInput: ['Ann'], lines: res.lines.slice(), text: res.lines.join('\n'),
    lineMode: 'auto', lineGap: .216, wantSize: res.fit.size, nudged: false, claude: null, verify: { geometry: res.check } };
}
/* the laser file of a job, as prepareApproval + buildBackFile write it (the glyphs relative to the view's centre) */
async function exported(job) {
  const view = job.view, fit = job.fit;
  const glyphs = fit.glyphs.map(g => ({ cmds: g.cmds.map(c => { const o = { type: c.type }; if (c.type !== 'Z') { o.x = c.x - view.cx; o.y = c.y - view.cy; } if (c.type === 'C' || c.type === 'Q') { o.x1 = c.x1 - view.cx; o.y1 = c.y1 - view.cy; } if (c.type === 'C') { o.x2 = c.x2 - view.cx; o.y2 = c.y2 - view.cy; } return o; }) }));
  reseed();
  const built = await P.buildBackFile({ charm: job.charm, mirrored: !!job.charm.mirrored, parsed: { doc: null, segments: [] }, cutMembers: view.cutMembers.map(m => ({ ...m, synthetic: true })), cx: view.cx, cy: view.cy, angleDeg: view.angleDeg, padPt: 5 / MM,
    glyphs, ...(job.face === 'front' ? { face: 'front' } : {}), view: job.face === 'front' ? 'asSeenFromFront' : 'asSeenFromBack', title: job.key, meta: { poolId: 'p' } });
  const parsed = await P.parseSource(built.bytes, 'x');
  const ink = E.leaves(parsed).filter(p => p.layer === 'ENGRAVE').flatMap(p => p.subpaths.flatMap(sp => sp.filter(o => o[0] !== 'h').map(o => o[o.length - 1])));
  return { bytes: built.bytes, sha: sha(built.bytes), ink, T: built.frame.T };
}
const shiftOf = (a, b) => { eq(a.ink.length, b.ink.length, 'same number of ink points'); eq(a.T, b.T, 'same page frame'); const d = a.ink.map((p, i) => [b.ink[i][0] - p[0], b.ink[i][1] - p[1]]); return d; };
const sameShift = (d, dx, dy, tol, m) => { for (const q of d) { near(q[0], dx, tol, m + ' x'); near(q[1], dy, tol, m + ' y'); } };
/* a vector in the view -> the same vector on the charm as drawn (undo the hoop-up turn, then the turn-over): the direction a move really goes on the metal */
const onCharm = (view, v) => { const a = -view.angleDeg * Math.PI / 180, c = Math.cos(a), s = Math.sin(a), u = [v[0] * c - v[1] * s, v[0] * s + v[1] * c]; return view.face === 'front' ? u : [-u[0], u[1]]; };

(async () => {
  const base = await mitten();
  const left = charmFor(base, { id: 'l', poolId: 'p1', side: 'L' }), right = charmFor(base, { id: 'r', poolId: 'p2', side: 'R', mirror: true });
  ok(right.mirrored === true && !left.mirrored, 'the Right ear is the Left\'s mirror image (Pair.pieceGeometry), the Left is as drawn');

  /* ═══ A · nothing moved, nothing changed ═══ */
  {
    const job = jobOf(left, { face: 'back', slot: 'L' }), fit0 = job.fit, file0 = await exported(job);
    ok(typeof Pos.of === 'function' && typeof Pos.snapshot === 'function', 'A0 the module loads and has its calls');
    ok(job.fit === fit0 && job.nudged === false && schedules.n === 0 && toasts.length === 0, 'A1 loading the module and reading touches nothing');
    const r = Pos.nudge(job, 0, 0);
    ok(r.ok && r.moved.dx === 0 && job.fit === fit0 && job.nudged === false && schedules.n === 0, 'A2 nudge(0, 0) is a no-op: the very same fit, not marked moved');
    ok((await exported(job)).sha === file0.sha, 'A3 and the exported laser file is byte for byte the same');
    const snap = Pos.snapshot(job), r2 = Pos.restore(job, snap);
    ok(r2 && job.fit === fit0 && (await exported(job)).sha === file0.sha, 'A4 a snapshot put back on an untouched job changes nothing (same fit, same file)');
    // Cancel after a move: back to the exact file
    const snap2 = Pos.snapshot(job);
    ok(Pos.nudge(job, 1, -0.5).ok && job.nudged === true && (await exported(job)).sha !== file0.sha, 'A5 a move changes the file');
    ok(Pos.restore(job, snap2) === true, 'A6 restore says yes');
    const back = await exported(job);
    ok(back.sha === file0.sha, 'A7 and Cancel puts the laser file back byte for byte');
    ok(job.nudged === false && job.wantSize === snap2.wantSize && Math.abs(job.fit.size - fit0.size) < 1e-9, 'A8 not marked moved again, the size the same');
    // the checkpoint copies the whole job; a restored job gives the same place and the same file
    Pos.nudge(job, 0.75, 0.25);
    const copy = structuredClone(job), file1 = await exported(job), file2 = await exported(copy);
    ok(file1.sha === file2.sha && copy.nudged === true, 'A9 a nudge survives the checkpoint copy (the job as the session keeps it): same place, same laser file');
    eq(Pos.of(copy), Pos.of(job), 'A10 and the picture position is the same');
  }

  /* ═══ B · a nudge moves the file by the millimetres asked ═══ */
  for (const face of ['back', 'front']) {
    const job = jobOf(left, { face, slot: 'L' }), file0 = await exported(job), size0 = job.fit.size, lines0 = job.lines.join('|'), weight0 = job.fit.weight;
    const r = Pos.nudge(job, 0.5, 0.25);
    ok(r.ok && !r.clamped && Math.abs(r.moved.dx - 0.5) < 1e-3 && Math.abs(r.moved.dy - 0.25) < 1e-3, `B1 ${face}: moved 0.5 mm right, 0.25 mm up`);
    const f1 = await exported(job);
    sameShift(shiftOf(file0, f1), 0.5 * PT, 0.25 * PT, 1e-6, `B2 ${face}: every point of the engraving in the laser file moved by exactly that`);
    ok(job.fit.size === size0 && job.lines.join('|') === lines0 && job.fit.weight === weight0 && job.nudged === true, `B3 ${face}: the same size, lines and weight; marked moved`);
    Pos.nudge(job, -2, -1);
    sameShift(shiftOf(file0, await exported(job)), -1.5 * PT, -0.75 * PT, 1e-6, `B4 ${face}: left and down add up`);
    ok(schedules.n > 0, 'B5 each move asks the checkpoint to be written');
  }

  /* ═══ C · the safe area ═══ */
  {
    const job = jobOf(left, { face: 'back', slot: 'L' }), size0 = job.fit.size;
    const r = Pos.nudge(job, 400, 0);
    ok(r.ok && r.clamped && r.moved.dx > 0 && r.moved.dx < 100, 'C1 asked for 400 mm to the right: it went as far as the metal lets it (' + r.moved.dx + ' mm), clamped');
    ok(G.verifyInk(job.fit.cmds, job.mask).ok, 'C2 and every letter is inside the safe area');
    const edge = job.fit.cmds, out = edge.map(k => k.type === 'Z' ? k : { ...k, x: k.x + 0.05 * PT, ...(k.x1 != null ? { x1: k.x1 + 0.05 * PT } : {}), ...(k.x2 != null ? { x2: k.x2 + 0.05 * PT } : {}) });
    ok(!G.verifyInk(out, job.mask).ok, 'C3 the stop is the edge itself: 0.05 mm more would put a letter outside');
    const fitEdge = job.fit, r2 = Pos.nudge(job, 1, 0);
    ok(!r2.ok && r2.clamped && r2.why === 'no room' && job.fit === fitEdge, 'C4 one more step that way is refused and the fit is not touched');
    ok(job.fit.size === size0, 'C5 the words did not shrink to squeeze in');
    const back = Pos.nudge(job, -0.5, 0);
    ok(back.ok && !back.clamped && job.fit.size === size0, 'C6 and the other way is free');
    const c = Pos.centre(job), mid = Pos.maskCentroid(job.mask);
    ok(c.ok && Math.hypot(job.fit.centre[0] - mid[0], job.fit.centre[1] - mid[1]) < 0.01 * PT, 'C7 centre() puts the words in the middle of the metal they may use');
    const big = Pos.size(job, 1.05), s1 = job.fit.size;
    ok(big.ok === true ? s1 > size0 - 1e-9 : big.atLimit === true, 'C8 bigger: grows, or says the metal is the limit');
    let guard = 0; while (Pos.size(job, 1.05).ok && guard++ < 400);
    const top = Pos.size(job, 1.05);
    ok(!top.ok && top.atLimit === true, 'C9 at the largest that fits it says atLimit and does not grow');
    const sBefore = job.fit.size, sm = Pos.size(job, 0.95), s2 = job.fit.size;
    ok(sm.ok && sm.atLimit === false && s2 < sBefore - 0.01 && G.verifyInk(job.fit.cmds, job.mask).ok, 'C10 smaller: shrinks and still verifies');
    toasts.length = 0;
  }

  /* ═══ D · back vs front, and a mirrored Right ear ═══ */
  {
    const jb = jobOf(left, { face: 'back', slot: 'L' }), jf = jobOf(left, { face: 'front', slot: 'L' });
    const fb0 = await exported(jb), ff0 = await exported(jf);
    Pos.nudge(jb, 1, 0.5); Pos.nudge(jf, 1, 0.5);
    sameShift(shiftOf(fb0, await exported(jb)), PT, 0.5 * PT, 1e-6, 'D1 a back moves in its own picture (seen from behind) in the file');
    sameShift(shiftOf(ff0, await exported(jf)), PT, 0.5 * PT, 1e-6, 'D2 and a front in its own picture');
    const db = onCharm(jb.view, [1, 0.5]), df = onCharm(jf.view, [1, 0.5]);
    near(db[0], -df[0], 1e-9, 'D3 on the metal the back moves the opposite way across to the front (it is seen from behind)'); near(db[1], df[1], 1e-9, 'D3b and the same along');
    // the pair: the Left and the Right ear (the Left's mirror image)
    const jl = jobOf(left, { face: 'back', slot: 'L' }), jr = jobOf(right, { face: 'back', slot: 'R' });
    ok(jr.charm.mirrored === true && Math.abs(jr.view.angleDeg + jl.view.angleDeg) < 1e-6, 'D4 the Right ear is cut on the mirrored geometry and its view is turned the opposite way hoop-up');
    const fl0 = await exported(jl), fr0 = await exported(jr);
    const mv = [0.75, 0.4];
    Pos.nudge(jl, mv[0], mv[1]);
    const dxR = Pos.mirrorDx(jr, mv[0]);
    ok(dxR === -mv[0] && Pos.mirrorDx(jl, mv[0]) === mv[0] && Pos.mirrorDx({ slot: 'D2' }, 2) === 2 && Pos.mirrorDx({ slot: null }, 2) === 2 && Pos.mirrorDx(jr, 0) === 0, 'D5 mirrorDx: the Right ear only, and no negative zero');
    Pos.nudge(jr, dxR, mv[1]);
    sameShift(shiftOf(fl0, await exported(jl)), mv[0] * PT, mv[1] * PT, 1e-6, 'D6 the Left ear moved by the call');
    sameShift(shiftOf(fr0, await exported(jr)), dxR * PT, mv[1] * PT, 1e-6, 'D7 the Right ear moved by the mirrored call');
    const cl = onCharm(jl.view, [mv[0], mv[1]]), cr = onCharm(jr.view, [dxR, mv[1]]);
    near(cr[0], -cl[0], 1e-6, 'D8 on the metal the Right ear\'s move is the exact mirror image of the Left\'s (across)');
    near(cr[1], cl[1], 1e-6, 'D9 and the same along');
  }

  /* ═══ E · of(job): the place in a picture of the charm ═══ */
  {
    for (const face of ['front', 'back']) for (const up of [90, 0, 37]) {
      const res = Fit.calculate({ charm: left, lines: ['Ann'], lineMode: 'auto', viewOptions: { res: 6, upAngle: up, ...(face === 'front' ? { face: 'front' } : {}) }, maskOptions: { marginMm: .8, keepOut: [] }, opts: { minCapMm: 1.6, maxHeightFrac: .4, lineGap: .216, minStrokeMm: 0, minGapMm: 0, tryRotated: false } }, fonts, G);
      const view = res.view, job = { state: 'review', view, mask: res.mask, fit: { ...res.fit }, nudged: false };
      // the hole's centre as drawn, and where the same point is in the view (turn over, then turn hoop-up)
      const hole = view.cutMembers.find(m => m.bbox && m !== view.outline && m.bbox[2] - m.bbox[0] < 80), hc = [(hole.bbox[0] + hole.bbox[2]) / 2, (hole.bbox[1] + hole.bbox[3]) / 2];
      const M = view.M, R = view.R, p1 = G.ap(M, hc[0], hc[1]), pv = G.ap(R, p1[0], p1[1]);
      job.fit.centre = pv; job.fit.angle = 25; job.nudged = true;
      const o = Pos.of(job), ob = view.cutMembers.reduce((a, m) => a ? [Math.min(a[0], m.bbox[0]), Math.min(a[1], m.bbox[1]), Math.max(a[2], m.bbox[2]), Math.max(a[3], m.bbox[3])] : m.bbox.slice(), null);
      const fx = (hc[0] - ob[0]) / (ob[2] - ob[0]), fy = 1 - (hc[1] - ob[1]) / (ob[3] - ob[1]);
      near(o.x, face === 'front' ? fx : 1 - fx, 1e-4, `E1 ${face} up ${up}: x is the hole's place on the picture (turned over for a back)`);
      near(o.y, fy, 1e-4, `E2 ${face} up ${up}: y counts down from the top`);
      near(o.size, job.fit.size / (ob[3] - ob[1]), 1e-4, 'E3 size is the em height over the box height');
      near(((o.angle - (view.angleDeg - 25)) % 360 + 360) % 360 % 360, 0, 1e-3, 'E4 angle is the view turn minus the words\' own turn (CSS clockwise)');
      ok(o.auto === false, 'E5 moved: not auto');
    }
    const j = jobOf(left, { face: 'back', slot: 'L' }); ok(Pos.of(j).auto === true, 'E6 untouched: auto'); ok(Pos.of({ fit: null }) === null && Pos.of(null) === null && Pos.of({ fit: j.fit }) === null, 'E7 no placement or no view: null');
  }

  /* ═══ F · locked, not ready, fitted again ═══ */
  {
    const job = jobOf(left, { face: 'back', slot: 'L' }), fit0 = job.fit;
    for (const state of ['skipped', 'none', 'words', 'fitting']) {
      job.state = state;
      const r = Pos.nudge(job, 1, 0), c = Pos.centre(job), s = Pos.size(job, 1.05);
      ok(!r.ok && !c.ok && !s.ok && job.fit === fit0 && job.nudged === false && Pos.canMove(job) === false, `F1 ${state}: refuses, nothing touched`);
      eq(r.why, 'locked', `F2 ${state}: says locked`);
    }
    // NODECIDED (Paul, 16 Oct 2026): an approved or saved piece is NOT locked any more: its placement can be changed, and the first change puts it back to "needs Approve" (the page's reopenForEdit, here a stand-in that records the call)
    for (const state of ['approved', 'written']) {
      const j = jobOf(left, { face: 'back', slot: 'L' }), f0 = j.fit, n0 = reopened.length;
      j.state = state; ok(Pos.canMove(j) === true, `F1 ${state}: the placement can be moved`);
      const r = Pos.nudge(j, 1, 0);
      ok(r.ok && reopened.length === n0 + 1 && j.state === 'review' && j.fit !== f0 && j.nudged === true, `F1b ${state}: the first nudge reopens it (back to review, ONE reopen) and moves it`);
      const r2 = Pos.nudge(j, 1, 0); ok(r2.ok && reopened.length === n0 + 1, `F1c ${state}: the second change does not reopen again`);
      const j2 = jobOf(left, { face: 'back', slot: 'L' }); j2.state = state; const n1 = reopened.length;
      ok(Pos.centre(j2).ok && reopened.length === n1 + 1 && j2.state === 'review', `F1d ${state}: centring it reopens it too`);
      const j3 = jobOf(left, { face: 'back', slot: 'L' }); j3.state = state; const n2 = reopened.length;
      ok(Pos.size(j3, 1.05).ok && reopened.length === n2 + 1 && j3.state === 'review', `F1e ${state}: resizing it reopens it too`);
    }
    // ... but nothing of it without a placement in hand: an approved piece with no mask / fit is still refused
    { const j = jobOf(left, { face: 'back', slot: 'L' }); j.state = 'approved'; j.mask = null; const n0 = reopened.length; ok(!Pos.nudge(j, 1, 0).ok && reopened.length === n0 && Pos.canMove(j) === false, 'F1f an approved piece with no placement in hand is refused and not reopened'); }
    job.state = 'review';
    const noFit = { state: 'review', fit: null }; ok(Pos.nudge(noFit, 1, 0).why === 'no placement yet' && Pos.snapshot(noFit) === null, 'F3 no placement yet: refuses, no snapshot');
    ok(Pos.nudge(job, NaN, 1).ok === false && Pos.nudge(job, 'a', 1).ok === false && Pos.size(job, 0).ok === false && Pos.size(job, -1).ok === false, 'F4 not a number: refused');
    const snap = Pos.snapshot(job);
    const refitted = jobOf(left, { face: 'back', slot: 'L' }); job.mask = refitted.mask; job.fit = refitted.fit;     // fitted again meanwhile (new words / font / charm): a new mask
    ok(Pos.restore(job, snap) === false && job.fit === refitted.fit, 'F5 a snapshot of a job that was fitted again is not put back over the new fit');
    job.mask = snap.mask; job.state = 'approved'; job.fit = Object.assign({}, snap.fit, { centre: [snap.fit.centre[0] + 3, snap.fit.centre[1]] }); { const n0 = reopened.length; ok(Pos.restore(job, snap) === false && job.state === 'approved' && reopened.length === n0, 'F6 nor over a job approved meanwhile (a stale Cancel never changes or reopens an approved piece)'); }
    ok(Pos.restore(null, snap) === false && Pos.restore(job, null) === false && Pos.restore(job, { v: 2 }) === false, 'F7 nor from nothing');
  }

  ok(sha(fs.readFileSync(path.join(root, 'charm-nest-engrave-position.js'))) === before.sha, 'G the module file was not changed while the test ran');
  console.log('PASS ' + n + ' checks: ENGPOS position: untouched is byte-identical (nudge 0, snapshot/restore, Cancel, checkpoint copy); the laser file moves by the mm asked (back and front); clamped at the safe area without shrinking; centre/size; back vs front and Left vs mirrored Right on the metal; of(job); jobs not ready refuse; an approved piece is moved and reopened by its first change');
})().catch(e => { console.error(e); process.exit(1); });
