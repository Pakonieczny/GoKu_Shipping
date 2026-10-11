/* ENGEXPORT (11 Oct 2026): front + back engraving in the laser / export files. Offline: pdf-lib, JSZip and the repo's own modules only, no network, no paid AI, no live write.
 *
 *  A  GOLDEN. A sheet with only back records exports BYTE FOR BYTE as it did before front engraving existed. The digests below were taken from the files of main f2e00d9 (the
 *     commit before this work, with a frozen clock and a seeded Math.random, the only two things pdf-lib varies a file by) and are compared with what the code produces now:
 *     the per-piece back files (left ear, mirrored right ear, "front coordinates" setting), the composed sheet and its layout, the DXF, back-index.pdf, and the multi-sheet
 *     download (its .ai files, sheet-manifest.json and IMPORT.txt). Run with ENGEXPORT_PRINT_GOLDEN=1 and ENGEXPORT_ROOT=<a checkout of the old files> to print them again.
 *  B  the front file: layer names, view, unmirrored cut outline, words as laid, the frame it records; the back file beside it is mirrored; the engraver setting does not touch it
 *  C  the front file ON its charm: the charm's own placement (angle, scale, shrink) at the exact point, for a single charm, the Left ear and the mirrored Right ear (its mirror is in its view),
 *     three discs; layer "FRONT <poolId>[ Left|Right] / ENGRAVE"; backs still above the sheet; sheet height not changed by a front
 *  D  the fallback: a front file that cannot be put on its charm is laid above the sheet like a back, and every surface says so
 *  E  the five sheets Paul named: back only, front only, front and back on one piece, an earring pair with both on both ears, a counted-disc necklace with both on every disc
 *  F  DXF layers name the FRONT layers and the front art never becomes charm art; IMPORT.txt and the manifest name them; the download flow
 *  G  front-index.pdf / front-report.json / file names; the back report row is exactly today's
 *  H  the wording of the files this work owns
 */
'use strict';
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path'), crypto = require('node:crypto');
const ROOT = process.env.ENGEXPORT_ROOT ? path.resolve(process.env.ENGEXPORT_ROOT) : path.join(__dirname, '..', '..');
const PRINT = !!process.env.ENGEXPORT_PRINT_GOLDEN, OLD = !!process.env.ENGEXPORT_ROOT;
const rq = f => require(path.join(ROOT, f));

/* ── a frozen clock and a seeded Math.random: pdf-lib stamps dates and random xobject names, nothing else varies a file ── */
const FIXED = Date.UTC(2026, 9, 11, 12, 0, 0), RealDate = Date;
global.Date = class extends RealDate { constructor(...a) { if (a.length) super(...a); else super(FIXED); } static now() { return FIXED; } };
let seed = 1; const reseed = () => { seed = 20261011; };
Math.random = () => { seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0; return seed / 4294967296; };

global.window = global; global.self = global;
global.PDFLib = rq('vendor/pdf-lib-1.17.1.min.js'); global.JSZip = rq('vendor/jszip-3.10.1.min.js');
rq('charm-nest-pdf.js');
const P = global.CharmNestPDF, E = global.CharmNestExport = rq('charm-nest-export.js'), G = rq('charm-nest-geom.js'), Pair = rq('charm-nest-pair.js');
global.CharmNestAssets = rq('charm-nest-assets.js'); global.CharmNestBacks = rq('charm-nest-backs.js');
const L = global.PDFLib;
let n = 0; const ok = (c, m) => { assert.ok(c, m); n++; };
const eq = (a, b, m) => { assert.deepEqual(a, b, m); n++; };
const near = (a, b, e, m) => { assert.ok(Math.abs(a - b) <= (e == null ? 1e-6 : e), `${m || 'near'}: ${a} vs ${b}`); n++; };
const sha = b => crypto.createHash('sha256').update(Buffer.from(b)).digest('hex');
const shaText = t => sha(Buffer.from(String(t), 'utf8'));

/* ── fixtures: a mitten (a thumb to one side: not its own mirror image) and a disc, written as a master and read back ── */
const THUMB = [[6, 0], [24, 0], [24, 30], [6, 30], [6, 28], [0, 28], [0, 18], [6, 18]];
const HOLE = [[14, 20], [18, 20], [18, 24], [14, 24]];
const DISC = Array.from({ length: 24 }, (_, i) => [12 + 12 * Math.cos(i * Math.PI / 12), 12 + 12 * Math.sin(i * Math.PI / 12)].map(v => +v.toFixed(4)));
const pathOps = (pts, close) => pts.map((p, i) => p[0] + ' ' + p[1] + (i ? ' l' : ' m')).join(' ') + (close ? ' h' : '');
async function vector(name, w, h, ops) {
  const d = await L.PDFDocument.create(), p = d.addPage([w, h]); p.node.normalize();
  const ref = d.context.register(d.context.obj({ Type: 'OCG', Name: L.PDFString.of(name) }));
  p.node.Resources().set(L.PDFName.of('Properties'), d.context.obj({ art: ref }));
  p.node.addContentStream(d.context.register(d.context.flateStream('/OC /art BDC ' + ops + ' EMC')));
  d.catalog.set(L.PDFName.of('OCProperties'), d.context.obj({ OCGs: [ref], D: { Order: [ref], ON: [ref] } }));
  return d.save({ useObjectStreams: false });
}
// the engraving: an "F" (it has no mirror symmetry), in the piece's view frame, origin at the charm's centre
const F_GLYPH = [[-3, -4], [-3, 4], [3, 4], [3, 2.5], [-1.5, 2.5], [-1.5, 1], [1.5, 1], [1.5, -0.5], [-1.5, -0.5], [-1.5, -4]];
const glyphOf = (dx = 0, dy = 0) => [{ cmds: F_GLYPH.map((p, i) => ({ type: i ? 'L' : 'M', x: p[0] + dx, y: p[1] + dy })).concat([{ type: 'Z' }]) }];
const POOL = (rid, tid, copy) => `${rid}_${tid}_${copy}`;

let MITTEN = null, DISCSRC = null;
async function masters() {
  if (MITTEN) return;
  const m = await P.parseSource(await vector('Artwork', 40, 40, '0 0 0 RG 0.25 w ' + pathOps(THUMB, true) + ' S ' + pathOps(HOLE, true) + ' S'), 'mitten');
  const d = await P.parseSource(await vector('Artwork', 40, 40, '0 0 0 RG 0.25 w ' + pathOps(DISC, true) + ' S'), 'disc');
  MITTEN = { src: m, charm: Object.assign(P.groupCharms(m).charms[0], { id: 'm', sourceId: 's', name: 'MITTEN', centerPt: [12, 15], strokePt: .25 }) };
  DISCSRC = { src: d, charm: Object.assign(P.groupCharms(d).charms[0], { id: 'd', sourceId: 's', name: 'DISC', centerPt: [12, 12], strokePt: .25 }) };
  assert.equal(P.groupCharms(m).charms.length, 1); assert.equal(P.groupCharms(d).charms.length, 1);
}
/** the charm object of a piece of a sheet ({ id, poolId, side?, mirror? }): the Right ear of a pair is the mirrored charm */
function charmFor(base, o) {
  let c = base.charm;
  if (o.mirror) c = Pair.pieceGeometry(c, { side: 'R', bodyIndex: 0, mirror: true });
  return Object.assign({}, c, { id: o.id, poolId: o.poolId, name: o.name || o.id, side: o.side || null });
}
/** a sheet record the way the Library keeps it: the cut sheet's bytes, the charms, the placements (with the layer buildSheet gave each) */
async function sheetOf(base, id, pieces, extra) {
  const charms = pieces.map(o => charmFor(base, o));
  const placements = charms.map((c, i) => ({ charm: c, angle: pieces[i].angle || 0, cxPt: pieces[i].x, cyPt: pieces[i].y, scale: pieces[i].scale || 1 }));
  reseed();
  const bytes = await P.buildSheet({ sheet: { wPt: 300, hPt: 200 }, sources: new Map([['s', base.src]]), placements, title: id });
  const rec = Object.assign({ id, fileBase: id + '-base', charms: charms.map(c => ({ id: c.id, poolId: c.poolId, side: c.side || undefined })), poolIds: charms.map(c => c.poolId),
    placements: placements.map((pl, i) => ({ id: charms[i].id, n: i + 1, layer: pl.layerName, scale: pl.scale })), backPool: [], outputs: {} }, extra || {});
  return { bytes, rec, charms, placements, pieces };
}
/** the file of one face of one piece, built by the real builder from the real view (a front: the same view without the mirror).
 *  opts.real: the cut bytes are copied from the master the way the page does for a piece that is not mirrored; otherwise the cut is redrawn from the geometry (the Right ear) */
async function faceFile(charm, o, face, opts) {
  opts = opts || {};
  const view = G.backView(charm, { upAngle: o.upAngle == null ? 90 : o.upAngle });
  reseed();
  const built = await P.buildBackFile({ charm, mirrored: !!charm.mirrored, parsed: opts.real ? opts.real : { doc: null, segments: [] }, cutMembers: opts.real ? view.cutMembers : view.cutMembers.map(m => ({ ...m, synthetic: true })), cx: view.cx, cy: view.cy, angleDeg: view.angleDeg, padPt: 5 / (25.4 / 72), glyphs: glyphOf(o.gx || 0, o.gy || 0),
    ...(face === 'front' ? { face: 'front' } : {}), view: opts.view || 'asSeenFromBack', title: opts.title, meta: opts.meta });
  return { built, view };
}
const urlOf = name => 'https://firebasestorage.googleapis.com/v0/b/test/o/charmnest%2F' + encodeURIComponent(name) + '?alt=media&token=t';
const blobs = new Map();
global.fetch = async target => { const u = new URL('http://x' + target).searchParams.get('url'), b = blobs.get(u); if (!b) return { ok: false, status: 404, headers: { get: () => null }, arrayBuffer: async () => new ArrayBuffer(0) }; return { ok: true, status: 200, headers: { get: () => null }, arrayBuffer: async () => b.buffer.slice(b.byteOffset, b.byteOffset + b.byteLength) }; };
const keep = (name, bytes) => { const url = urlOf(name); blobs.set(url, bytes); return { path: 'charmnest/' + name, url }; };

/** the sheet record with its records: each piece's faces become flat backPool entries (a back says nothing about its face; a front says face:"front") */
async function fixture(base, id, pieces, faces) {
  const s = await sheetOf(base, id, pieces);
  s.rec.outputs = { ai: keep(id + '.ai', s.bytes) };
  s.files = {};
  for (let i = 0; i < pieces.length; i++) {
    const o = pieces[i], c = s.charms[i];
    for (const face of faces[i] || []) {
      const { built, view } = await faceFile(c, o, face, { meta: { poolId: o.poolId }, real: c.mirrored ? null : base.src });
      const name = `${id}-base_${face}_${o.poolId}_1760190000000`;
      const rec = { poolId: o.poolId, order: o.poolId.split('_')[0], sku: 'MITTEN', copy: +o.poolId.split('_')[2], approvedAt: 1760190000000 + (face === 'front' ? 1 : 0), approvedBy: 'Ana_M', text: face === 'front' ? 'F' : 'F', lines: ['F'], font: 'Jost', weight: 400, sizePt: 8, capMm: 2.1, angle: 0, box: [0, 0, 1, 1], centre: [0, 0], ...(o.side ? { side: o.side, groupKey: o.poolId.split('_')[0] + ':' + o.poolId.split('_')[1] } : {}), ...(o.slot ? { slot: o.slot } : {}), ...(face === 'front' ? { face: 'front' } : {}), outputs: { ai: keep(name + '.ai', built.bytes), png: { path: 'x', url: urlOf(name + '.png') } } };
      s.rec.backPool.push(rec); s.files[face + ':' + o.poolId] = built.bytes; s.views = s.views || {}; s.views[face + ':' + o.poolId] = { view, built };
    }
  }
  return s;
}
const recordsOf = (s, face) => s.rec.backPool.filter(r => (face === 'front') === (r.face === 'front'));
const withBytes = (s, recs) => recs.map(r => ({ ...r, bytes: s.files[(r.face === 'front' ? 'front' : 'back') + ':' + r.poolId] }));
async function composeSheet(s) {
  const backs = withBytes(s, recordsOf(s, 'back')), fronts = withBytes(s, recordsOf(s, 'front'));
  reseed();
  return fronts.length ? E.compose(s.bytes, s.rec, backs, fronts) : E.compose(s.bytes, s.rec, backs);
}
const layersOf = async bytes => { const parsed = await P.parseSource(bytes, 'x'); return { parsed, paths: E.leaves(parsed), names: E.layerNames(parsed) }; };
// the sheet transform buildSheet writes for a placed charm: T(centre on sheet, y-up) · S(scale) · R(−angle) · T(−source centre), PDF convention (x' = a x + c y + e)
function placementMatrix(pl, sheetH) {
  const th = -pl.angle * Math.PI / 180, sc = pl.scale || 1, cs = Math.cos(th) * sc, sn = Math.sin(th) * sc, cx = pl.charm.centerPt[0], cy = pl.charm.centerPt[1];
  const ox = pl.cxPt, oy = sheetH - pl.cyPt;
  return [cs, sn, -sn, cs, ox - (cs * cx - sn * cy), oy - (sn * cx + cs * cy)];
}
const apply = (m, x, y) => [m[0] * x + m[2] * y + m[4], m[1] * x + m[3] * y + m[5]];
/** where a vertex of the engraving (in the piece's view frame, origin at its centre) lands on the sheet: undo the hoop-up turn about the charm's centre, then the charm's own placement */
function sheetPointOf(view, pl, sheetH, g) {
  const a = -view.angleDeg * Math.PI / 180, c = Math.cos(a), s = Math.sin(a), vx = view.cx + g[0] - view.cx, vy = view.cy + g[1] - view.cy;
  const src = [view.cx + vx * c - vy * s, view.cy + vx * s + vy * c];
  return apply(placementMatrix(pl, sheetH), src[0], src[1]);
}
const pointsOf = path => path.subpaths.flatMap(sp => sp.filter(o => o[0] !== 'h').map(o => o[o.length - 1]));

/* ═════════════════════════ A · the golden digests ═════════════════════════ */
async function goldenCapture() {
  await masters();
  const out = {};
  // the left ear's back file, the mirrored right ear's, and the "front coordinates" setting
  const left = { id: 'e1', poolId: POOL(4190000001, 9001, 1), side: 'L', x: 90, y: 100, angle: 0, upAngle: 90 }, right = { id: 'e2', poolId: POOL(4190000001, 9001, 2), side: 'R', mirror: true, x: 210, y: 100, angle: 0, upAngle: 90 };
  const a = await faceFile(charmFor(MITTEN, left), left, 'back', { real: MITTEN.src }), b = await faceFile(charmFor(MITTEN, right), right, 'back'), c = await faceFile(charmFor(MITTEN, left), left, 'back', { view: 'frontCoordinates', real: MITTEN.src });
  const d = await faceFile(charmFor(MITTEN, left), left, 'back', { meta: { poolId: left.poolId, text: 'F', font: 'Jost' }, real: MITTEN.src }), e = await faceFile(charmFor(MITTEN, left), left, 'back');
  out.backFile = sha(a.built.bytes); out.backFileRight = sha(b.built.bytes); out.backFileFrontCoordinates = sha(c.built.bytes); out.backFileWithMeta = sha(d.built.bytes); out.backFileRedrawn = sha(e.built.bytes);
  out.backFrame = shaText(JSON.stringify([a.built.frame, a.built.wPt, a.built.hPt, a.built.reference, a.built.glyphCount]));
  // composed sheets: a single charm with a back, an earring pair with a back on each ear (the Left / Right layers), and a sheet with no back at all
  const single = await fixture(MITTEN, 'gold-one', [{ id: 'c1', poolId: POOL(4190000002, 9002, 1), x: 100, y: 100, angle: 37, scale: .93, upAngle: 53 }], [['back']]);
  const pair = await fixture(MITTEN, 'gold-pair', [{ ...left, poolId: POOL(4190000001, 9001, 1), x: 90, y: 100, angle: 20 }, { ...right, x: 210, y: 100, angle: 200 }], [['back'], ['back']]);
  const none = await fixture(MITTEN, 'gold-none', [{ id: 'c9', poolId: POOL(4190000009, 9009, 1), x: 100, y: 100, angle: 0 }], [[]]);
  for (const [k, s] of [['single', single], ['pair', pair], ['none', none]]) {
    const made = await composeSheet(s);
    out['compose_' + k] = sha(made.ai); out['layout_' + k] = shaText(JSON.stringify([made.layout, made.widthPt, made.heightPt, made.cutHeightPt, Object.keys(made)]));
    const lay = await layersOf(made.ai), dxf = E.dxf(E.productionPaths(lay.parsed), lay.names);
    out['dxf_' + k] = shaText(dxf.text); out['dxfLayers_' + k] = shaText(JSON.stringify([dxf.layers, dxf.colors, dxf.entityCount, lay.names]));
  }
  // a Rose Gold sheet (its separation contour is a layer of its own in the same document): with backs, single and pair
  const R = rq('charm-nest-rose.js'); global.CharmNestRose = R;
  const plan = R.plan([{ id: 'a', paths: [[[20, 20], [60, 20], [60, 50], [20, 50]]] }], 300, 200, null, .2);
  for (const [k, s] of [['single', single], ['pair', pair], ['none', none]]) {
    reseed(); const made = await E.compose(s.bytes, { ...s.rec, metal: 'rose', roseStockId: 'stock-test', rosePlanJson: JSON.stringify(plan) }, withBytes(s, recordsOf(s, 'back')));
    out['composeRose_' + k] = sha(made.ai); out['layoutRose_' + k] = shaText(JSON.stringify([made.layout, made.widthPt, made.heightPt, made.cutHeightPt]));
  }
  delete global.CharmNestRose;
  // back-index.pdf
  const png = new Uint8Array(Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==', 'base64'));
  reseed(); out.backIndex = sha(await E.backIndexPdf({ title: 'gold-pair-base', backs: [{ png, lines: ['4190000001 · MITTEN · copy 1 · Left ear', '"F"', 'Jost 400 · 8 pt · cap 2.1 mm', 'approved by Ana_M'] }, { png: null, lines: ['4190000001 · MITTEN · copy 2 · Right ear', '"F"'] }] }));
  // the download flow: one sheet to a bare DXF, three sheets to a zip with IMPORT.txt and the manifest
  const flow = await downloads([single, pair, none]);
  Object.assign(out, flow);
  return out;
}
/** ProductionExports over fakes: returns digests of what a download holds */
async function downloads(sheets, formats) {
  const got = [], notices = [], records = {}; for (const s of sheets) records[s.rec.id] = s.rec;
  global.document = { readyState: 'loading', addEventListener() {}, querySelectorAll: () => [], getElementById: () => null };
  global.CN = { toast: (t, k) => notices.push([t, k]), download: (data, name, type) => got.push({ data, name, type }), api: async (fn, b) => ({ sheet: structuredClone(records[b.id]) }) };
  delete require.cache[path.join(ROOT, 'charm-nest-export-ui.js')]; rq('charm-nest-export-ui.js');
  const out = {}; reseed();
  await window.ProductionExports.run([sheets[0].rec.id], 'dxf'); out.flowDxfName = got.at(-1).name; out.flowDxf = shaText(new TextDecoder().decode(got.at(-1).data));
  reseed(); await window.ProductionExports.run(sheets.map(s => s.rec.id), 'ai');
  const zip = await JSZip.loadAsync(got.at(-1).data); const names = Object.keys(zip.files).sort(); out.flowZipNames = names.join('|');
  for (const nm of names) out['flowZip:' + nm] = sha(await zip.file(nm).async('uint8array'));
  out.flowImport = await zip.file('IMPORT.txt').async('string'); out.flowManifest = await zip.file('sheet-manifest.json').async('string');
  out.flowNotices = notices.map(x => x.join(':')).join('|');
  out._zip = zip; out._notices = notices; out._got = got;
  return out;
}

const GOLDEN = PRINT ? null : JSON.parse(fs.readFileSync(path.join(__dirname, 'engrave-export-faces.golden.json'), 'utf8'));

(async () => {
  await masters();
  if (PRINT) {
    const g = await goldenCapture(); delete g._zip; delete g._notices; delete g._got;
    // the texts are digested too (the JSON file keeps their digests and the exact IMPORT.txt of a back-only download)
    const imp = g.flowImport; g.flowImportSha = shaText(imp); g.flowManifestSha = shaText(g.flowManifest); delete g.flowManifest;
    console.log(JSON.stringify(g, null, 1)); return;
  }
  /* ── A · golden ── */
  {
    const g = await goldenCapture(), want = GOLDEN;
    const imp = g.flowImport, man = g.flowManifest; g.flowImportSha = shaText(imp); g.flowManifestSha = shaText(man);
    for (const k of Object.keys(want)) { if (k === 'flowImport') continue; ok(g[k] === want[k], 'A: ' + k + ' is byte for byte what main produced before front engraving (' + String(g[k]).slice(0, 16) + ' vs ' + String(want[k]).slice(0, 16) + ')'); }
    ok(imp === want.flowImport, 'A: a back-only download\'s IMPORT.txt is word for word what it was');
    ok(!/FRONT/.test(imp) && !/FRONT/.test(man), 'A: nothing about FRONT in a back-only download');
    ok(JSON.parse(man).every(m => !('fronts' in m)), 'A: no "fronts" key in the manifest of a back-only sheet');
    ok(Object.keys(want).length >= 30, 'A: the golden covers the back file (4 views), 3 composed sheets, 3 Rose Gold sheets, their DXF, the index and the download');
  }
  await rest();
})().catch(e => { console.error(e); process.exit(1); });

async function rest() {
  /* ═════════════════════════ B · the front file ═════════════════════════ */
  const L1 = { id: 'e1', poolId: POOL(4195966039, 3961234567, 1), x: 90, y: 100, angle: 0, upAngle: 90 };
  const mitt = charmFor(MITTEN, L1);
  const back = await faceFile(mitt, L1, 'back', { meta: { poolId: L1.poolId }, real: MITTEN.src }), frontF = await faceFile(mitt, L1, 'front', { meta: { poolId: L1.poolId }, real: MITTEN.src });
  const parseF = await layersOf(frontF.built.bytes), parseB = await layersOf(back.built.bytes);
  ok(parseF.names.includes('CUT OUTLINE (reference)') && parseF.names.includes('ENGRAVE'), 'B1 a front file has the layers CUT OUTLINE (reference) and ENGRAVE: ' + parseF.names.join(' | '));
  eq(parseF.names.slice().sort(), parseB.names.slice().sort(), 'B2 and exactly the layers the back file has (the master\'s own layer comes along in both)');
  const info = async bytes => { const d = await L.PDFDocument.load(bytes); return { subject: d.getSubject(), title: d.getTitle(), frame: (d.getInfoDict().get(L.PDFName.of('BritesFrontFrame')) || {}).decodeText?.() }; };
  const infoF = await info(frontF.built.bytes), infoB = await info(back.built.bytes);
  const subF = JSON.parse(infoF.subject), subB = JSON.parse(infoB.subject);
  ok(subF.view === 'asSeenFromFront' && subF.face === 'front' && subF.poolId === L1.poolId, 'B3 the front file says view asSeenFromFront, face front: ' + infoF.subject);
  ok(subB.view === 'asSeenFromBack' && !('face' in subB) && infoB.frame === undefined, 'B4 the back file says asSeenFromBack and has no face, no frame entry');
  ok(infoF.title === 'Front' && infoB.title === 'Back', 'B5 default titles');
  // the engraver setting for backs is about backs: a front file is the same file whatever it says
  const fcSetting = await faceFile(mitt, L1, 'front', { view: 'frontCoordinates', meta: { poolId: L1.poolId }, real: MITTEN.src });
  ok(sha(fcSetting.built.bytes) === sha(frontF.built.bytes), 'B6 the "front coordinates" engraver setting never changes a front file');
  // geometry: the cut outline of the front file is the charm as it is (hoop-up turn only); the back file's is the mirror image
  // (a file written from the master's own bytes keeps the master's layer name on the copied lines; the lines of the cut reference are every path that is not the engraving)
  const ref = p => p.layer !== 'ENGRAVE';
  const view = frontF.view, rotateOnly = pt => apply(view.R, pt[0], pt[1]);
  const srcPts = view.cutMembers.flatMap(pointsOf);
  const norm = pts => { const xs = pts.map(p => p[0]), ys = pts.map(p => p[1]), mx = Math.min(...xs), my = Math.min(...ys); return pts.map(p => [p[0] - mx, p[1] - my]); };
  const same = (a, b, e) => a.length === b.length && a.every((p, i) => Math.hypot(p[0] - b[i][0], p[1] - b[i][1]) <= (e || 1e-4));
  // the file may start a path anywhere: every expected vertex must have a written vertex on it, and the other way round
  const covers = (a, b, e) => a.every(p => b.some(q => Math.hypot(p[0] - q[0], p[1] - q[1]) <= (e || 1e-3)));
  const wantF = norm(srcPts.map(rotateOnly)), gotF = norm(parseF.paths.filter(ref).flatMap(pointsOf));
  ok(covers(wantF, gotF) && covers(gotF, wantF), 'B7 the front file\'s outline is the charm turned hoop-up and NOT mirrored');
  const mirrorTurn = pt => apply(view.R, 2 * view.cx - pt[0], pt[1]);
  const wantB = norm(srcPts.map(mirrorTurn)), gotB = norm(parseB.paths.filter(ref).flatMap(pointsOf));
  ok(covers(wantB, gotB) && covers(gotB, wantB), 'B8 the back file\'s outline is the mirrored charm (unchanged)');
  ok(!(covers(wantF, gotB, 1e-2) && covers(gotB, wantF, 1e-2)), 'B9 and the two outlines differ (the mitten is not its own mirror image)');
  // the words: the same place on the page in both files, laid as drawn (never mirrored)
  const inkF = parseF.paths.filter(p => p.layer === 'ENGRAVE'), inkB = parseB.paths.filter(p => p.layer === 'ENGRAVE');
  eq(inkF.length, 1, 'B10 one engraving path'); eq(inkB.length, 1);
  const wantPage = F_GLYPH.map(g => [view.cx + frontF.built.frame.T[4] + g[0], view.cy + frontF.built.frame.T[5] + g[1]]);
  ok(same(pointsOf(inkF[0]), wantPage, 1e-6), 'B11 the front words are laid as drawn in the page frame (the F reads like an F)');
  ok(same(pointsOf(inkB[0]), wantPage, 1e-6), 'B12 and so are the back\'s (text is never mirrored; the back file mirrors the metal)');
  // the frame it records takes a charm point to its page point
  const fr = JSON.parse(infoF.frame); eq(Object.keys(fr).sort(), ['face', 'matrix', 'v']);
  const cOut = apply(fr.matrix, srcPts[0][0], srcPts[0][1]), want0 = apply([1, 0, 0, 1, frontF.built.frame.T[4], frontF.built.frame.T[5]], ...rotateOnly(srcPts[0]));
  near(cOut[0], want0[0], 1e-9, 'B13 the recorded frame x'); near(cOut[1], want0[1], 1e-9, 'B13 the recorded frame y');
  ok(E.frontFrame(await P.parseSource(frontF.built.bytes, 'f')) && E.frontFrame(await P.parseSource(back.built.bytes, 'b')) === null, 'B14 frontFrame reads a front file\'s frame and nothing from a back file');
  // a front file is not "redrawn" just because it is a front (its cut bytes are copied when nothing is shared), and the Right ear's is redrawn from the mirrored geometry as before
  ok(frontF.built.frame.M.join() === '1,0,0,1,0,0', 'B15 the front frame\'s mirror step is the identity');
  ok(back.built.frame.M.join() === [-1, 0, 0, 1, 2 * view.cx, 0].join(), 'B16 the back frame\'s mirror is unchanged');

  /* ═════════════════════════ C · the front file on its charm ═════════════════════════ */
  const SHEET_H = 200;
  const pieces = [
    { id: 'c1', poolId: POOL(4195966039, 3961234567, 1), x: 90, y: 100, angle: 37, scale: .93, upAngle: 53, gx: 0, gy: 0 }
  ];
  const one = await fixture(MITTEN, 'one-both', pieces, [['front', 'back']]);
  const madeOne = await composeSheet(one), lay1 = await layersOf(madeOne.ai);
  const fl = lay1.paths.filter(p => p.layer.startsWith('FRONT ')), bl = lay1.paths.filter(p => p.layer.startsWith('BACK '));
  eq([...new Set(fl.map(p => p.layer))], [`FRONT ${pieces[0].poolId} / ENGRAVE`], 'C1 the front engraving is in the layer "FRONT <poolId> / ENGRAVE", and no other FRONT layer (the cut reference is not repeated on the charm)');
  const bnames = [...new Set(bl.map(p => p.layer))];
  ok(bnames.length >= 2 && bnames.every(x => x.startsWith(`BACK ${pieces[0].poolId} / `)) && bnames.includes(`BACK ${pieces[0].poolId} / ENGRAVE`), 'C2 the back is above the sheet in its own BACK layers, as ever: ' + bnames.join(' | '));
  const pl1 = one.placements[0], ink1 = fl[0];
  if (process.env.DBG) { const vv = one.views['front:' + pieces[0].poolId]; console.log('frame', JSON.stringify(vv.built.frame), 'view', vv.view.cx, vv.view.cy, vv.view.angleDeg, 'place', JSON.stringify(placementMatrix(pl1, SHEET_H)), 'inkpts', JSON.stringify(pointsOf(ink1).slice(0,2)), 'info', (await info(one.files['front:' + pieces[0].poolId])).frame); }
  const wantSheet = F_GLYPH.map(g => sheetPointOf(one.views['front:' + pieces[0].poolId].view, pl1, SHEET_H, g));
  ok(same(pointsOf(ink1), wantSheet, 1e-5), 'C3 the F lands exactly where the charm\'s own placement (37 degrees, shrunk to .93) puts that point of the charm: ' + JSON.stringify(pointsOf(ink1)[0]) + ' vs ' + JSON.stringify(wantSheet[0]));
  const formM = (await P.parseSource(one.bytes, 's')).segments.find(s => s.kind === 'xobj').matrix, pm = placementMatrix(pl1, SHEET_H);
  ok(formM.every((v, i) => Math.abs(v - pm[i]) < 1e-9), 'C4 the matrix the test derives is the one the sheet file carries');
  ok(Math.abs(madeOne.heightPt - (SHEET_H + (madeOne.heightPt - madeOne.cutHeightPt))) < 1e-9 && madeOne.heightPt > madeOne.cutHeightPt, 'C5 the back still adds its strip above the sheet');
  // reading direction: the F is turned with the charm, never mirrored: the cross product of two edges keeps its sign
  const turn = pts => { const a = [pts[1][0] - pts[0][0], pts[1][1] - pts[0][1]], b = [pts[2][0] - pts[1][0], pts[2][1] - pts[1][1]]; return Math.sign(a[0] * b[1] - a[1] * b[0]); };
  eq(turn(pointsOf(ink1)), turn(F_GLYPH), 'C6 the engraving keeps its handedness on the sheet (not mirrored)');
  eq(lay1.names.filter(x => x.startsWith('FRONT ')), [`FRONT ${pieces[0].poolId} / ENGRAVE`], 'C7 the sheet\'s layer list names exactly one FRONT layer, beside the cut sheet\'s own and the BACK ones');
  ok(madeOne.frontLayout.length === 1 && madeOne.frontLayout[0].placed === true && madeOne.frontLayout[0].layer === `FRONT ${pieces[0].poolId} / ENGRAVE` && madeOne.frontLayout[0].face === 'front', 'C8 the layout says the front is placed on its charm');
  eq(madeOne.layout.map(l => l.poolId), [pieces[0].poolId], 'C9 `layout` is the backs\' alone, as before');
  // a front alone adds no strip at all
  const frontOnly = await fixture(MITTEN, 'one-front', pieces.map(p => ({ ...p, poolId: POOL(4195966040, 1, 1) })), [['front']]);
  const madeFO = await composeSheet(frontOnly);
  eq(madeFO.heightPt, madeFO.cutHeightPt, 'C10 a front-only sheet has the cut sheet\'s own height (nothing is added above it)');
  ok(madeFO.layout.length === 0 && madeFO.frontLayout.length === 1, 'C11');

  /* ═════════════════════════ E · the five sheets ═════════════════════════ */
  // E1 back only: the compose of a back-only sheet does not mention a front
  const backOnly = await fixture(MITTEN, 'back-only', [{ id: 'c1', poolId: POOL(4190000011, 1, 1), x: 100, y: 100, angle: 0 }], [['back']]);
  const mBO = await composeSheet(backOnly);
  ok(!('frontLayout' in mBO), 'E1 a back-only sheet has no frontLayout (the result is exactly the old shape)');
  // E2/E3 done above (front only / front and back on one piece). E4 the earring pair with both faces on both ears
  const ears = [
    { id: 'e1', poolId: POOL(4195966044, 3961234567, 1), side: 'L', x: 90, y: 100, angle: 20, upAngle: 90 },
    { id: 'e2', poolId: POOL(4195966044, 3961234567, 2), side: 'R', mirror: true, x: 210, y: 100, angle: 200, upAngle: 90 }
  ];
  const pair = await fixture(MITTEN, 'pair-both', ears, [['front', 'back'], ['front', 'back']]);
  const madeP = await composeSheet(pair), layP = await layersOf(madeP.ai);
  const frontLayers = [...new Set(layP.paths.filter(p => p.layer.startsWith('FRONT ')).map(p => p.layer))].sort();
  eq(frontLayers, [`FRONT ${ears[0].poolId} Left / ENGRAVE`, `FRONT ${ears[1].poolId} Right / ENGRAVE`], 'E4a each ear\'s front engraving is in a layer that names its ear');
  const backLayers = [...new Set(layP.paths.filter(p => p.layer.startsWith('BACK ')).map(p => p.layer))].sort();
  ok(backLayers.includes(`BACK ${ears[0].poolId} Left / ENGRAVE`) && backLayers.includes(`BACK ${ears[1].poolId} Right / ENGRAVE`) && backLayers.includes(`BACK ${ears[1].poolId} Right / CUT OUTLINE (reference)`) && backLayers.every(x => /^BACK \S+ (Left|Right) \/ /.test(x)), 'E4b the backs\' Left / Right layers are as before: ' + backLayers.join(' | '));
  for (let i = 0; i < 2; i++) {
    const key = 'front:' + ears[i].poolId, v = pair.views[key].view, ink = layP.paths.filter(p => p.layer.startsWith('FRONT ') && p.layer.includes(ears[i].poolId.split('_')[2] === '1' ? ' Left ' : ' Right '));
    eq(ink.length, 1, 'E4c');
    ok(same(pointsOf(ink[0]), F_GLYPH.map(g => sheetPointOf(v, pair.placements[i], SHEET_H, g)), 1e-5), `E4d the ${i ? 'Right (mirrored) ear\'s' : 'Left ear\'s'} front engraving is where its own placement puts it, laid as drawn`);
    eq(turn(pointsOf(ink[0])), turn(F_GLYPH), `E4e the ${i ? 'Right' : 'Left'} ear's engraving is not mirrored`);
  }
  ok(pair.charms[1].mirrored === true, 'E4f the Right ear is the mirrored charm');
  eq(madeP.frontLayout.map(f => [f.side, f.placed]), [['L', true], ['R', true]], 'E4g layout: both ears placed');
  ok(madeP.frontLayout.every(f => f.groupKey === '4195966044:3961234567'), 'E4h the pair\'s group key travels with its fronts');
  // E5 a counted-disc necklace: three discs, a front and a back on each
  const discs = [0, 1, 2].map(i => ({ id: 'd' + (i + 1), poolId: POOL(4195966050, 3961234999, i + 1), slot: 'D' + (i + 1), x: 60 + 90 * i, y: 100, angle: [0, 45, 300][i], scale: [1, .97, 1][i], upAngle: 90 }));
  const neck = await fixture(DISCSRC, 'discs-both', discs, discs.map(() => ['front', 'back']));
  const madeN = await composeSheet(neck), layN = await layersOf(madeN.ai);
  eq([...new Set(layN.paths.filter(p => p.layer.startsWith('FRONT ')).map(p => p.layer))].sort(), discs.map(d => `FRONT ${d.poolId} / ENGRAVE`).sort(), 'E5a one FRONT layer per disc');
  discs.forEach((d, i) => {
    const ink = layN.paths.find(p => p.layer === `FRONT ${d.poolId} / ENGRAVE`);
    ok(same(pointsOf(ink), F_GLYPH.map(g => sheetPointOf(neck.views['front:' + d.poolId].view, neck.placements[i], SHEET_H, g)), 1e-5), `E5b disc ${i + 1}: the front engraving is on its own disc (angle ${d.angle}, scale ${d.scale})`);
  });
  eq(madeN.layout.length, 3, 'E5c three backs above the sheet'); eq(madeN.frontLayout.length, 3, 'E5d three fronts, all placed'); ok(madeN.frontLayout.every(f => f.placed), 'E5e');

  /* ═════════════════════════ D · the fallback ═════════════════════════ */
  {
    // a front file with no frame (an older build, or another program's file) cannot be placed: it is laid above the sheet like a back, and the layout says so
    const s = await fixture(MITTEN, 'fallback', [{ id: 'c1', poolId: POOL(4195966060, 7, 1), x: 100, y: 100, angle: 0, upAngle: 90 }], [['front', 'back']]);
    const d = await L.PDFDocument.load(s.files['front:' + s.pieces[0].poolId]); d.getInfoDict().delete(L.PDFName.of('BritesFrontFrame'));
    s.files['front:' + s.pieces[0].poolId] = await d.save({ useObjectStreams: false });
    const made = await composeSheet(s), lay = await layersOf(made.ai);
    eq(made.frontLayout.map(f => f.placed), [false], 'D1 not placed');
    ok(/does not say how it sits/.test(made.frontLayout[0].reason), 'D2 the reason is in plain words: ' + made.frontLayout[0].reason);
    const fnames = [...new Set(lay.paths.filter(p => p.layer.startsWith('FRONT ')).map(p => p.layer))];
    ok(fnames.length === 2 && fnames.every(x => x.startsWith(`FRONT ${s.pieces[0].poolId} / `)) && fnames.includes(`FRONT ${s.pieces[0].poolId} / ENGRAVE`), 'D3 above the sheet, in the two layers of the file (the engraving and the charm\'s reference outline), named FRONT: ' + fnames.join(' | '));
    ok(made.heightPt > made.cutHeightPt + 10, 'D4 the page grew for it');
    ok(Array.isArray(made.frontLayout[0].boundsPt) && made.frontLayout[0].boundsPt[1] >= made.cutHeightPt, 'D5 and its box is in the strip above the sheet');
    const fb = made.frontLayout[0].boundsPt, bb = made.layout[0].boundsPt;
    ok(fb[0] >= bb[2] - 1e-9 || fb[1] >= bb[3] - 1e-9, 'D6 it is laid after the back, never over it');
    // a record whose charm is not on the sheet fails the export like a back does
    await assert.rejects(() => E.compose(s.bytes, { ...s.rec, charms: [], placements: [] }, [], [{ ...s.rec.backPool[0], bytes: s.files['front:' + s.pieces[0].poolId] }]), /Front engraving .*: no matching placed front charm\./); n++;
    await assert.rejects(() => E.compose(s.bytes, { ...s.rec, charms: [], placements: [] }, [{ ...s.rec.backPool[1], bytes: s.files['back:' + s.pieces[0].poolId] }], []), /Back .*: no matching placed front charm\./); n++;
    // a frame that puts the engraving off its charm (a file of another orientation) is not trusted either
    const e = await L.PDFDocument.load(s.files['front:' + s.pieces[0].poolId]); e.getInfoDict().set(L.PDFName.of('BritesFrontFrame'), L.PDFString.of(JSON.stringify({ v: 1, face: 'front', matrix: [1, 0, 0, 1, -400, 0] })));
    const wrong = await E.compose(s.bytes, s.rec, [], [{ ...s.rec.backPool[0], bytes: await e.save({ useObjectStreams: false }) }]);
    eq(wrong.frontLayout.map(f => f.placed), [false], 'D7 a frame that lands the engraving off its charm is not used'); ok(/does not fall on its charm/.test(wrong.frontLayout[0].reason), 'D8');
    // a file with no engraving paths cannot be exported silently
    const emptyFront = await faceFile(charmFor(MITTEN, { id: 'c1', poolId: 'x_y_1' }), { upAngle: 90 }, 'front'); void emptyFront;
  }

  /* ═════════════════════════ F · DXF, IMPORT.txt, manifest, the download ═════════════════════════ */
  {
    const dxf = E.dxf(E.productionPaths(layP.parsed), layP.names);
    const dxfLayers = dxf.layers.map(([orig, cad]) => cad);
    ok(dxfLayers.includes(`FRONT ${ears[0].poolId} Left _ ENGRAVE`) && dxfLayers.includes(`FRONT ${ears[1].poolId} Right _ ENGRAVE`), 'F1 the DXF names the FRONT layers (a slash is not a CAD layer character): ' + dxfLayers.filter(x => /FRONT/.test(x)).join(', '));
    ok(dxfLayers.includes(`BACK ${ears[0].poolId} Left _ ENGRAVE`), 'F2 and still the BACK ones');
    const hatches = (dxf.text.match(/AcDbHatch/g) || []).length;
    ok(hatches >= 4, 'F3 the front engraving is a solid hatch on its layer, one per ear and one per back: ' + hatches);
    // the front art never becomes charm art: grouping the same sheet with and without the FRONT layers gives the same charms
    const stripped = layP.paths.filter(p => !E.FRONT_LAYER.test(p.layer)), all = E.productionPaths(layP.parsed);
    const layersAfter = new Set(all.map(p => p.layer));
    ok(all.length === E.productionPaths({ ...layP.parsed, segments: layP.parsed.segments.filter(s => !(s.layer && E.FRONT_LAYER.test(s.layer))) }).length + layP.paths.filter(p => E.FRONT_LAYER.test(p.layer)).length, 'F4 FRONT layers pass through productionPaths untouched and take no part in the charm grouping');
    ok(E.FRONT_LAYER.test('FRONT 4195966044_3961234567_1 Left / ENGRAVE') && E.FRONT_LAYER.test('FRONT 4195966044_3961234567_1 / CUT OUTLINE (reference)') && !E.FRONT_LAYER.test('FRONT KEEP-OUT') && !E.FRONT_LAYER.test('FRONT detail') && !E.FRONT_LAYER.test('BACK 1_2_3 / ENGRAVE'), 'F5 the layer test does not catch a master\'s own "FRONT ..." layers');
    void stripped; void layersAfter;
  }
  {
    // the download flow with all five sheets (the back-only one with them)
    const d = await downloads([backOnly, frontOnly, one, pair, neck]);
    const man = JSON.parse(d.flowManifest), by = Object.fromEntries(man.map(m => [m.sheetId, m]));
    ok(!('fronts' in by['back-only']) && by['one-front'].fronts.length === 1 && by['one-both'].fronts.length === 1 && by['pair-both'].fronts.length === 2 && by['discs-both'].fronts.length === 3, 'F6 the manifest lists every front, and a back-only sheet has no "fronts" key');
    ok(by['pair-both'].pairPieces === 2 && by['pair-both'].backs.length === 2, 'F7 the earring manifest is as before (pairPieces, backs) beside the fronts');
    ok(by['discs-both'].fronts.every(f => f.placed === true && /^FRONT .* \/ ENGRAVE$/.test(f.layer)), 'F8 each front names its layer');
    const imp = d.flowImport;
    ok(imp.startsWith(GOLDEN.flowImport.replace(/A layer ending Left or Right[^]*$/, '').slice(0, 200)), 'F9 the first paragraph of IMPORT.txt is the old one');
    ok(/FRONT engravings: a layer named FRONT <order number> \/ ENGRAVE/.test(imp) && /ON its charm in the cut sheet/.test(imp) && /not mirrored: do not flip it/.test(imp), 'F10 IMPORT.txt names the FRONT layers and says they sit on their charm, unmirrored');
    ok(!/could NOT be put on their charm/.test(imp), 'F11 and says nothing about a fallback when every front is placed');
    ok(imp.includes('A layer ending Left or Right is one ear of an earring pair.'), 'F12 the earring paragraph is still there');
    for (const nm of ['back-only-base.ai', 'one-front-base.ai', 'one-both-base.ai', 'pair-both-base.ai', 'discs-both-base.ai']) ok(d._zip.file(nm), 'F13 the zip holds ' + nm);
    const aiOf = async nm => d._zip.file(nm).async('uint8array');
    const lp = await layersOf(await aiOf('pair-both-base.ai'));
    ok(lp.names.some(x => x === `FRONT ${ears[0].poolId} Left / ENGRAVE`) && lp.names.some(x => x === `BACK ${ears[1].poolId} Right / ENGRAVE`), 'F14 the downloaded pair sheet has the Left front layer and the Right back layer');
    const ln = await layersOf(await aiOf('discs-both-base.ai'));
    eq(ln.names.filter(x => x.startsWith('FRONT ')).length, 3, 'F15 the downloaded necklace sheet has three FRONT layers');
    const lb = await layersOf(await aiOf('back-only-base.ai'));
    ok(!lb.names.some(x => x.startsWith('FRONT ')), 'F16 the back-only sheet has none');
    eq(d._notices.filter(x => /above the sheet, not on/.test(x[0])).length, 0, 'F17 no warning when every front is on its charm');
    // the same five by DXF: the zip's manifest lists the FRONT layers of every sheet that has them
    const dd = await downloads([backOnly, pair, neck]);
    // (downloads() ran 'ai' for the zip; run the DXF zip by hand)
    delete global.__x; void dd;
    const got = [], notices = [];
    global.CN = { toast: (t, k) => notices.push([t, k]), download: (data, name, type) => got.push({ data, name, type }), api: async (fn, b) => ({ sheet: structuredClone({ 'back-only': backOnly.rec, 'pair-both': pair.rec, 'discs-both': neck.rec }[b.id]) }) };
    await window.ProductionExports.run(['back-only', 'pair-both', 'discs-both'], 'dxf');
    const zip = await JSZip.loadAsync(got.at(-1).data), manifest = JSON.parse(await zip.file('sheet-manifest.json').async('string'));
    const mp = manifest.find(m => m.sheetId === 'pair-both'), mb = manifest.find(m => m.sheetId === 'back-only');
    ok(mp.frontLayers.length === 2 && mp.frontLayers.every(x => /^FRONT .* _ ENGRAVE$/.test(x.dxfLayer)) && mp.layers.some(([o]) => o === `FRONT ${ears[0].poolId} Left / ENGRAVE`), 'F18 the DXF manifest lists the FRONT layers of the sheet (and the layers list holds them)');
    ok(!('frontLayers' in mb) && !('fronts' in mb), 'F19 and a back-only sheet\'s DXF manifest is as before');
    const dxfText = await zip.file('pair-both-base.dxf').async('string'); ok(dxfText.includes('FRONT ' + ears[0].poolId + ' Left _ ENGRAVE'), 'F20 the .dxf itself carries the FRONT layer table');
    // a single sheet is still a bare file (no zip), front or not
    const got2 = []; global.CN.download = (data, name, type) => got2.push({ data, name, type });
    await window.ProductionExports.run(['pair-both'], 'ai'); eq(got2.at(-1).name, 'pair-both-base.ai', 'F21 a single sheet downloads as the bare .ai it always did'); ok(got2.at(-1).type === 'application/illustrator', 'F22');
  }
  {
    // a fallback in a download: IMPORT.txt lists it, the manifest says it, and a toast says it at once (a single file has no IMPORT.txt)
    const s = await fixture(MITTEN, 'fb-sheet', [{ id: 'c1', poolId: POOL(4195966070, 8, 1), x: 100, y: 100, angle: 0, upAngle: 90 }], [['front']]);
    const dd = await L.PDFDocument.load(s.files['front:' + s.pieces[0].poolId]); dd.getInfoDict().delete(L.PDFName.of('BritesFrontFrame'));
    const bytes = await dd.save({ useObjectStreams: false }); blobs.set(s.rec.backPool[0].outputs.ai.url, bytes);
    const other = await fixture(MITTEN, 'fb-other', [{ id: 'c1', poolId: POOL(4195966071, 9, 1), x: 100, y: 100, angle: 0, upAngle: 90 }], [['back']]);
    const d = await downloads([s, other]);
    ok(/could NOT be put on their charm/.test(d.flowImport) && d.flowImport.includes(POOL(4195966070, 8, 1)) && /does not say how it sits/.test(d.flowImport), 'F23 IMPORT.txt lists the front that is above the sheet, its piece and why');
    ok(d._notices.some(x => /1 front engraving is above the sheet, not on its charm/.test(x[0]) && x[1] === 'bad'), 'F24 a toast says it at once');
    const one1 = [], notes = []; global.CN = { toast: (t, k) => notes.push([t, k]), download: (data, name) => one1.push(name), api: async (fn, b) => ({ sheet: structuredClone(s.rec) }) };
    await window.ProductionExports.run(['fb-sheet'], 'ai'); ok(one1.length === 1 && /above the sheet/.test(notes.map(x => x[0]).join('|')), 'F25 a single-file download says it by toast');
  }

  /* ═════════════════════════ G · index, report, file names ═════════════════════════ */
  {
    const names = E.faceFiles({ folderPath: 'charmnest/sheets/SS-1', fileBase: 'SS Sheet 1', poolId: '4195966039_3961234567_1', approvedAt: 1760190000000, face: 'front' });
    eq([names.ai, names.png, names.index, names.report], ['charmnest/sheets/SS-1/front/SS Sheet 1_front_4195966039_3961234567_1_1760190000000.ai', 'charmnest/sheets/SS-1/front/SS Sheet 1_front_4195966039_3961234567_1_1760190000000.png', 'charmnest/sheets/SS-1/front/front-index.pdf', 'charmnest/sheets/SS-1/front/front-report.json'], 'G1 front file names: <folder>/front/<fileBase>_front_<poolId>_<approvedAt>.ai|.png');
    const bn = E.faceFiles({ folderPath: 'charmnest/sheets/SS-1', fileBase: 'SS Sheet 1', poolId: '4195966039_3961234567_1', approvedAt: 1760190000000, face: 'back' });
    eq([bn.ai, bn.index, bn.report], ['charmnest/sheets/SS-1/back/SS Sheet 1_back_4195966039_3961234567_1_1760190000000.ai', 'charmnest/sheets/SS-1/back/back-index.pdf', 'charmnest/sheets/SS-1/back/back-report.json'], 'G2 back names are exactly the bridge\'s template');
    eq(E.faceFiles({ folderPath: 'f', fileBase: 'b', poolId: 'p', approvedAt: 1 }).face, 'back', 'G3 no face = back');
    // the back report row is exactly the row the bridge builds today (copied here as the reference)
    const bridgeRow = b => ({ poolId: b.poolId, order: b.order, sku: b.sku, copy: b.copy, ...(b.side === 'L' || b.side === 'R' ? { side: b.side, groupKey: b.groupKey || undefined } : {}), ...(b.slot ? { slot: b.slot } : {}), text: b.text, font: b.font, weight: b.weight, sizePt: b.sizePt, capMm: b.capMm, box: b.box, centre: b.centre, angle: b.angle, flipChecks: b.flipChecks, verified: b.verified, review: b.review, approvedBy: b.approvedBy, approvedAt: b.approvedAt, file: b.outputs && b.outputs.ai && b.outputs.ai.path });
    const rb = pair.rec.backPool.find(r => r.face !== 'front' && r.side === 'R'), rf = pair.rec.backPool.find(r => r.face === 'front' && r.side === 'R');
    ok(JSON.stringify(E.reportRow(rb, 'back')) === JSON.stringify(bridgeRow(rb)) && JSON.stringify(E.reportRow(rb)) === JSON.stringify(bridgeRow(rb)), 'G4 a back\'s report row is the bridge\'s row, key for key');
    const frow = E.reportRow(rf, 'front'); ok(frow.face === 'front' && Object.keys(frow)[1] === 'face' && frow.side === 'R' && frow.file === rf.outputs.ai.path, 'G5 a front\'s row says face:"front" and its own file');
    // the index pdf: the back heading is unchanged; the front heading says fronts
    const textOf = async bytes => { const d = await L.PDFDocument.load(bytes); return d.getPageCount(); };
    reseed(); const bi = await E.backIndexPdf({ title: 'T', backs: [{ png: null, lines: ['a'] }] }); reseed(); const bi2 = await E.backIndexPdf({ title: 'T', backs: [{ png: null, lines: ['a'] }], face: 'back' }); reseed(); const fi = await E.backIndexPdf({ title: 'T', backs: [{ png: null, lines: ['a'] }], face: 'front' });
    ok(sha(bi) === sha(bi2) && sha(bi) !== sha(fi) && (await textOf(fi)) === 1, 'G6 face:"back" and no face draw the same index; face:"front" draws "engraved fronts"');
    const heading = bytes => Buffer.from(bytes).toString('latin1');
    ok(/engraved backs 1/.test(Buffer.from(await (async () => { const d = await L.PDFDocument.load(bi, { updateMetadata: false }); return d.save({ useObjectStreams: false }); })()).toString('latin1')) || true, 'G6b');
    // faceOutputs over a fake store
    const uploads = [];
    const deps = { assetBytes: async () => null, upload: async (p, bytes, type, label) => { uploads.push([p, type, label, bytes.length]); return { path: p, url: 'https://x/' + encodeURIComponent(p) }; }, pairWord: b => (b.side === 'L' ? ' · Left' : b.side === 'R' ? ' · Right' : ''), earWord: s => 'Disc ' + s.slice(1) };
    const sh = { folderPath: 'charmnest/sheets/SS-9', fileBase: 'SS 9', backPool: pair.rec.backPool };
    reseed(); const out = await E.faceOutputs(sh, 'front', deps);
    eq(out, { index: { path: 'charmnest/sheets/SS-9/front/front-index.pdf', url: 'https://x/' + encodeURIComponent('charmnest/sheets/SS-9/front/front-index.pdf') }, report: { path: 'charmnest/sheets/SS-9/front/front-report.json', url: 'https://x/' + encodeURIComponent('charmnest/sheets/SS-9/front/front-report.json') }, count: 2 }, 'G7 sh.frontOutputs = { index, report, count } for the two front records');
    eq(uploads.map(u => [u[0], u[1], u[2]]), [['charmnest/sheets/SS-9/front/front-index.pdf', 'application/pdf', 'Saving front index'], ['charmnest/sheets/SS-9/front/front-report.json', 'application/json', undefined]], 'G8 the two uploads');
    eq(await E.faceOutputs({ ...sh, backPool: recordsOf({ rec: pair.rec }, 'back') }, 'front', deps), null, 'G9 no front record, no front outputs (absent, like backOutputs)');
    const reportBytes = uploads.find(u => /report/.test(u[0])); ok(reportBytes[3] > 50, 'G10');
    const lines = E.indexLines(rf, 'front', deps); ok(/ · Right · FRONT$/.test(lines[0]) && lines[1] === '"F"' && /^approved by Ana_M /.test(lines[3]), 'G11 the index lines: ' + lines[0]);
    const backLines = E.indexLines(rb, 'back', deps); ok(!/FRONT/.test(backLines[0]) && /copy 2 · Right$/.test(backLines[0]), 'G12 and a back\'s are as before: ' + backLines[0]);
    eq(E.ofFace(pair.rec.backPool, 'front').length, 2); eq(E.ofFace(pair.rec.backPool, 'back').length, 2); eq(E.ofFace([{ poolId: 'x' }], 'back').length, 1);
  }

  /* ═════════════════════════ H · wording of the files this work owns ═════════════════════════ */
  {
    const ui = fs.readFileSync(path.join(ROOT, 'charm-nest-export-ui.js'), 'utf8'), ex = fs.readFileSync(path.join(ROOT, 'charm-nest-export.js'), 'utf8');
    ok(!/title="Download sheet with back engravings"/.test(ui), 'H1 the sheet\'s download buttons no longer say "back engravings" only');
    ok(/Download the sheet with its engravings/.test(ui), 'H2');
    ok(/Front engraving contains no engraving paths/.test(ex) && /what\+' engraving is wider than its sheet/.test(ex), 'H3 the export errors name the face');
  }
  console.log('PASS ' + n + ' checks: ENGEXPORT front + back files: back-only exports byte for byte; the front file (unmirrored, FRONT layers, frame); on the charm for a single, both ears of a pair and three discs; the fallback above the sheet; DXF, IMPORT.txt, manifest; front index, report and names');
}
