// KEYONE (flag-fix-1011): the shared pieces of key-one.cjs and key-ears-same.cjs: two keys (the same cut outline), a ball beside a racket (two different charms), and the bridge's real Pool run
// in a vm with small stubs (the way pairs-pool.cjs runs it). load(rootDir) reads the modules of THAT checkout, so a test can run one scenario against another checkout of the app.
'use strict';
const assert = require('assert'), fs = require('fs'), path = require('path'), vm = require('vm');
const F = require('./pairs-fixtures.cjs');
module.exports = function load(root) {
const Pair = require(path.join(root, 'charm-nest-pair.js'));
const PP = require(path.join(root, 'charm-nest-pool-pieces.js'));
const O = require(path.join(root, 'charm-nest-orders.js'));
/* ── designs: two keys (the same cut outline, different letters cut out of them), and a ball beside a racket (two different charms) ── */
const bboxOf = pts => [Math.min(...pts.map(p => p[0])), Math.min(...pts.map(p => p[1])), Math.max(...pts.map(p => p[0])), Math.max(...pts.map(p => p[1]))];
const path_ = (pts, extra) => Object.assign({ kind: 'path', closed: true, stroke: true, fill: false, layer: 'CUT', strokeRGB: [0, 0, 0], lwPt: .25, bbox: bboxOf(pts), subpaths: [pts.map((p, i) => [i ? 'l' : 'm', p]).concat([['l', pts[0]]])] }, extra || {});
const rect = (x0, y0, x1, y1, extra) => path_([[x0, y0], [x1, y0], [x1, y1], [x0, y1]], extra);
const ink = (x0, y0, x1, y1) => rect(x0, y0, x1, y1, { layer: 'ENGRAVE', strokeRGB: [1, 0, 0] });
const keyAt = x => [[x + 4, 0], [x + 12, 0], [x + 12, 26], [x + 20, 26], [x + 20, 36], [x, 36], [x, 26], [x + 4, 26]];   // a key-like cut line: the same outline at any x
const charmFrom = (sku, bodies) => {
  const members = []; bodies.forEach((b, bi) => { b.outline.parent = bi; members.push(b.outline); for (const i of b.inks) { i.parent = bi; members.push(i); } });   // (parent: the drawing group each belongs to, as the master has them: one group per key)
  const bbox = members.reduce((u, m) => [Math.min(u[0], m.bbox[0]), Math.min(u[1], m.bbox[1]), Math.max(u[2], m.bbox[2]), Math.max(u[3], m.bbox[3])], [1e9, 1e9, -1e9, -1e9]);
  return { sku, outline: bodies[0].outline, members, bbox, widthPt: bbox[2] - bbox[0], heightPt: bbox[3] - bbox[1], areaPt2: (bbox[2] - bbox[0]) * (bbox[3] - bbox[1]) };
};
const DESIGNS = {
  // KEY_5598's shape: two keys, the same outline, an H cut out of the first and an M out of the second (here: one rectangle each of different size, so they are different charms by ink)
  KEY_5598: () => charmFrom('KEY_5598', [{ outline: path_(keyAt(0)), inks: [ink(4, 8, 8, 20)] }, { outline: path_(keyAt(30)), inks: [ink(34, 8, 42, 20)] }]),
  // the same, but a different count of cut-outs in the second key: its two bodies trace to different shapes
  KEYS_UNEQUAL: () => charmFrom('KEYS_UNEQUAL', [{ outline: path_(keyAt(0)), inks: [ink(4, 8, 8, 20)] }, { outline: path_(keyAt(30)), inks: [ink(34, 8, 42, 20), ink(34, 22, 38, 25)] }]),
  // a ball and a racket: clearly different charms of one design
  TENNIS_MIS: () => F.charmOf('TENNIS-MIS'),
  ONE_PENDANT: () => F.charmOf('ONE-PENDANT'),
  PAIR_STUD: () => F.charmOf('PAIR-STUD'),
  PAIR_HOOP: () => F.charmOf('PAIR-HOOP'),
};
const fnv = s => { let h = 0x811c9dc5; for (let i = 0; i < s.length; i++) { h ^= s.charCodeAt(i); h = Math.imul(h, 0x01000193) >>> 0; } return h.toString(16).padStart(8, '0'); };
// (like the real hash: the shape's size and the member count, never where it lies on the page: two same-sized bodies with as many cut-outs trace to ONE hash)
const hashOf = ch => fnv(Math.round((ch.bbox[2] - ch.bbox[0]) * 2) + 'x' + Math.round((ch.bbox[3] - ch.bbox[1]) * 2) + '|' + ch.members.length);
const entryOf = sku => { const c = DESIGNS[sku](), d = Pair.describe(c), e = { sku, members: c.members.length, widthPt: c.widthPt, heightPt: c.heightPt, areaPt2: c.areaPt2, charmHash: hashOf(c), holes: 0, engravable: false }; if (d.count > 1) e.pair = { v: 1, bodies: d.count, mismatched: !!d.mismatched }; return e; };

const src = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8');
const a = src.indexOf('const Pool = window.Pool = (() => {'), b = src.indexOf('\n/* Carry-forward is keyed', a);
assert(a > 0 && b > a, 'the Pool module is where the test expects it');
const oneA = src.indexOf('const oneRow = row => {'), oneB = src.indexOf('const earPair = row =>', oneA);   // (a checkout from before the change has no oneRow: the picture helpers are then left out)
const MM = 25.4 / 72;
function world(designs, rowsSeed) {
  const logs = [], calls = [], shown = [], pages = { gold: { metal: 'gold', charms: [], placements: [], status: 'idle', sheetId: null } };
  const entries = {};
  for (const sku of designs) entries[sku] = Object.assign(entryOf(sku), { sku, aiPath: 'charmnest/master/sku/' + sku + '.ai', aiUrl: 'https://x/' + sku, masterHash: 'mh-' + sku, upAngle: 0 });
  const c = {
    console, Date, JSON, Map, Set, Math, Number, String, Array, Object, Promise, Error, Uint8Array, setTimeout, clearTimeout, performance: { now: () => Date.now() }, structuredClone,
    window: { CharmNestPair: Pair, CharmNestPoolPieces: PP }, O, MM, labelOf: m => m, stockFor: () => ({ wPt: 1400, hPt: 700 }),
    S: { cloud: { ok: true }, settings: { insetPt: 0, maxFill: .8, silhouetteRes: 6, minPt: 6 }, poolSources: {}, sheets: { gold: { active: 0 } } },
    B: { pool: { rows: new Map(rowsSeed || []), sources: new Map() }, orders: { byKey: new Map(), rows: [] }, run: null },
    agent: (...x) => { logs.push(x.slice(1).join(' ')); }, api: async (fn, body) => { calls.push({ fn, body: JSON.parse(JSON.stringify(body)) }); return {}; },
    Master: { entryFor: sku => entries[sku] || null, fetchEntry: async () => null, keepOutOf: () => [], skuRegex: () => /^$/ },
    CharmNestAssets: { bytes: async () => new Uint8Array(1) },
    P: {
      parseSource: async () => ({}), groupCharmsAsync: null, integrateRings: () => ({ left: [], welded: 0 }), parseSkuLabel: () => null,
      buildSilhouettes: async (_p, charms) => { for (const ch of charms) { const w = ch.bbox[2] - ch.bbox[0], h = ch.bbox[3] - ch.bbox[1]; Object.assign(ch, { bits: new Uint8Array(4), w: 2, h: 2, scale: 6, areaPt2: Math.round(w * h * .8), widthPt: w, heightPt: h, centerPt: [(ch.bbox[0] + ch.bbox[2]) / 2, (ch.bbox[1] + ch.bbox[3]) / 2], thumb: 't', hash: hashOf(ch) }); } return charms; },
      frontPreview: (charm, px, opts) => { shown.push({ charm, px, opts }); return 'picture'; },
    },
    allSheets: () => Object.values(pages), pagesOf: m => [pages[m]], sheetDirty: () => {}, renderCard: () => {}, Orders: { rows: () => [], interpretAll() {}, render() {} },
    CN: {}, Gate: {}, Review: {}, RunCtl: {}, Cleanups: null, CustomPrint: null, CustomSheet: null, LiveNest: null, addPage: () => { throw new Error('no new page expected'); }
  };
  c.window.Cleanups = null; c.window.Master = c.Master; c.window.B = c.B;
  vm.createContext(c);
  c.P.groupCharmsAsync = async () => ({ charms: [structuredClone(DESIGNS[c.__sku]())], orphans: [] });
  { const r0 = src.indexOf('function replaceCutWhy('), r1 = src.indexOf('window.replaceCutWhy = replaceCutWhy;', r0); if (r0 > 0 && r1 > r0) vm.runInContext(src.slice(r0, r1), c); }   // (INPLACE, 20 Oct 2026: the pool asks which sheets were cut)
  vm.runInContext(src.slice(a, b), c);
  if (oneA > 0 && oneB > oneA) vm.runInContext(src.slice(oneA, oneB) + '\nwindow.__oneRow = oneRow; window.__pairRow = pairRow;', c);
  return { c, logs, calls, shown, pages, Pool: c.window.Pool, entries, oneRow: c.window.__oneRow, pairRow: c.window.__pairRow, use(sku) { c.__sku = sku; } };
}
const specOf = (rid, tid, sku, title, vars, entry) => {
  const sp = O.interpretLine({ receiptId: rid, updateTs: 1791500001 }, { transactionId: tid, listingId: '1', sku, title, quantity: 1, metalKey: 'gold', metalLabel: 'Gold', personalization: [], buyerMessage: '', variations: [{ name: 'Metal Choice', value: 'Gold' }].concat(vars || []) }, { optionMaps: {}, aliases: {}, noDesign: {}, masterEntry: s => (s === sku ? entry : null) });
  return { designSku: sku, material: 'gold', quantity: 1, size: null, form: sp.form || (/earring/i.test(title) ? 'earrings' : 'necklace'), chain: null, pair: sp.pair, pieceCount: sp.pieceCount };
};
const lineRow = (rid, tid, sku, qty, extra) => ({ key: `${rid}_${tid}`, state: 'pulled', order: { receiptId: rid, createTs: 1791500000, updateTs: 1791500001 }, line: { transactionId: tid, listingId: '1', sku, title: (extra && extra.title) || '' }, spec: Object.assign({ designSku: sku, material: 'gold', quantity: qty, size: null, form: 'necklace', chain: null }, extra || {}), problems: [], poolIds: [], engrave: null, arrivedAt: 0 });
const pairSpec = (extra) => ({ earring: false, single: false, mismatched: false, glued: false, perUnit: 1, twoBodies: true, notes: [], sides: [null], ...(extra || {}) });
return { root, Pair, PP, O, DESIGNS, hashOf, entryOf, world, specOf, lineRow, pairSpec, src, MM };
};
