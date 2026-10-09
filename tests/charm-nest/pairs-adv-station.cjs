// ADVSTATION (pairs-1009, phase 2 adversarial review of everything after the sheet exists): labels and stickers, laser export of the Right ear,
// stations, engraving per ear, the order window's kind words. Offline: repo code, the fake Firestore, real master files only READ. Nothing is written.
//   node tests/charm-nest/pairs-adv-station.cjs
// A  the sticker count, the pool's piece count and the station count agree for the same line (real lines of the Sep 17 snapshot + the shapes that broke)
// B  the exported Right ear is the exact x-mirror of the Left, on real stored master files (holes, hoop, hatch, mismatched bodies)
// C  the Right ear is written on the SAME layers as the Left (CUT / ENGRAVE / HATCH of the real per-SKU file)            [OPEN for PAIRMASTER until fixed]
// D  the back-side flip is not the left/right mirror: the Right ear's back view is the mirror of the Left ear's back view
// E  a stud pair engraved with different words per ear: the line's record, readiness per sheet, one welded seal for the pair
// F  quantity 2 (L R L R) and discs at the station helper; a card with two pair lines prints stickers that can be told apart
// G  the order window calls a matching pair a pair, not a mismatched pair                                                [OPEN for PAIRORDERWIN until fixed]
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path'), vm = require('node:vm');
const root = path.join(__dirname, '../..');
const read = f => fs.readFileSync(path.join(root, f), 'utf8');
const refuseNestedArrays = require('./_noNestedArrays.cjs');
const O = require('../../charm-nest-orders.js');
const Pair = require('../../charm-nest-pair.js');
const PL = require('../../charm-nest-pair-labels.js');
const Sides = require('../../charm-nest-engrave-sides.js');
const ok = m => console.log('  ✓ ' + m);
const open = (who, cond, m) => { if (cond) console.log('  ✓ ' + m + ' (fixed)'); else console.log('  OPEN for ' + who + ': ' + m); return !!cond; };
const REAL = '/mnt/project-files/backups/catalog-1009/LIVING/files/charmnest/sandbox/master/';
const SAMPLES = '/mnt/project-files/plans/pairs-1009/PAIRMASTER-samples/';

(async () => {
  /* ── A · one count for the sticker, the pool and the station ── */
  const win = { CharmNestOrders: O };
  vm.runInContext(read('station-live-order.js'), vm.createContext({ window: win, document: { addEventListener() {} }, console }));
  const J = v => JSON.parse(JSON.stringify(v)), S0 = win.StationLiveOrder;
  const S = { ears: l => J(S0.ears(l)), units: l => S0.units(l), tag: t => S0.tag(t), pieces: (l, r) => J(S0.pieces(l, r)) };   // (the page helper runs in its own realm: plain copies compare)
  const order = { receiptId: '4170000001', updateTs: 1789100000 };
  const T = over => Object.assign({ transaction_id: 5200000001, listing_id: 1718, sku: 'A', title: 'Moon Charm', quantity: 1, variations: [{ formatted_name: 'Metal Choice', formatted_value: 'Gold' }] }, over);
  const poolOf = t => O.interpretLine(order, { transactionId: String(t.transaction_id), listingId: String(t.listing_id), sku: t.sku, title: t.title, quantity: t.quantity, personalization: [], variations: t.variations.map(v => ({ name: v.formatted_name, value: v.formatted_value })) }, { optionMaps: {}, aliases: {}, noDesign: {}, masterEntry: () => null });
  const V = (n, v) => [{ formatted_name: n, formatted_value: v }];
  const SHAPES = {
    'stud pair': [T({ title: 'Crab Charm Stud Earrings' }), 2],
    'stud pair q2': [T({ title: 'Crab Charm Stud Earrings', quantity: 2 }), 4],
    'huggie charm set': [T({ title: 'Star Charm Add On Charm', variations: V('Charm Type', 'Huggie CHARM SET') }), 2],
    'single earring': [T({ title: 'Star Single Earring', variations: V('Type', 'Single earring') }), 1],
    'three discs': [T({ title: 'Initial Disc Necklace', variations: V('Number of Discs / Metal', '3 discs • gold') }), 3],
    'mismatched SKU': [T({ sku: 'MISMATCHED_7134', title: 'Mismatched stud earrings' }), 2],
    // the shapes that broke (the first is a real line of the Sep 17 snapshot, order 4172944386)
    'single charm, SKU with a plus (real order 4172944386)': [T({ sku: 'CAT(+FISH) - Cat Only', title: 'Cat Add On Charm Dainty Cat Charm Gold Animal Charm Handmade Jewelry Gift', variations: [{ formatted_name: 'Metal Choice', formatted_value: 'Gold Filled' }, { formatted_name: 'Charm Type', formatted_value: 'CHARM + Engraving' }] }), 1],
    'necklace charm, earrings in the title': [T({ title: 'Lighthouse Charm Earrings or Necklace', variations: V('Charm Type', 'Necklace CHARM') }), 1],
    'charm for huggie hoops (title)': [T({ title: 'Add on charm for huggie hoops', variations: V('Charm Type', 'CHARM + Engraving') }), 1],
    'SKU with an ampersand, necklace': [T({ sku: 'HEART & STAR', title: 'Heart Charm Necklace' }), 1],
    'SKU with a comma, zodiac charm': [T({ sku: 'LEO, VIRGO', title: 'Zodiac Charm' }), 1]
  };
  for (const [name, [t, want]] of Object.entries(SHAPES)) {
    const spec = poolOf(t), ears = S.ears([t]), units = S.units([t]);
    assert.equal(spec.pieceCount, want, name + ': the pool makes ' + want);
    assert.equal(units, spec.pieceCount, name + ': the station counts the pool\'s pieces');
    const stickers = want >= 2 && spec.pair.earring ? want : 0;                         // a sticker per ear of an earring pair, else the one order sticker (no pieces list)
    assert.equal(ears.length, stickers, name + ': stickers ' + ears.length + ', pool pieces ' + spec.pieceCount + ' (earring pair: ' + spec.pair.earring + ')');
    if (!spec.pair.earring) { assert.equal(S.tag(t), '', name + ': no pair words on the cell'); assert(!S.pieces([t]).pieces.some(p => p.both || p.side), name + ': no Left / Right tile'); }
    else assert.deepEqual(S.pieces([t]).pieces.map(p => p.side), ears.map(e => e.side), name + ': board tiles and stickers say the same ears');
  }
  ok('A1 sticker count = pool piece count = station count, for pairs, q2, discs, singles and the lines whose words once fooled the sticker');
  // a real mismatched Tennis listing line (SKU names two designs with a slash, title says Mismatched): a pair of ears at the station, as the pool will make it
  { const t = T({ sku: 'Huggie Hoops-Tennis Ball/Racket3', title: 'Mismatched Tennis Ball and Raquet Huggie Hoops Charm', variations: V('Metal Choice', 'Silver') }); assert.deepEqual(S.ears([t]).map(e => e.side), ['L', 'R']); assert.equal(S.units([t]), poolOf(t).pieceCount); ok('A2 the real mismatched Tennis line is still a Left and a Right'); }
  // an order: a stud pair, a charm and a necklace of discs together: stickers only for the pair, every unit counted once
  { const lines = [T({ transaction_id: 1, title: 'Crab Charm Stud Earrings' }), T({ transaction_id: 2, sku: 'CAT(+FISH) - Cat Only', title: 'Cat Add On Charm' }), T({ transaction_id: 3, title: 'Initial Disc Necklace', variations: V('Number of Discs / Metal', '3 discs • gold') })];
    assert.equal(S.ears(lines).length, 2); assert.equal(S.units(lines), 2 + 1 + 3); const pc = S.pieces(lines, '4170000001'); assert.equal(pc.pieceCount, 6); assert.equal(pc.pieces.length, 6);
    assert.deepEqual(pc.pieces.map(p => p.side || '-').join(''), 'LR----'); assert(pc.pieces.slice(3).every(p => p.of === 3 && p.grp === '4170000001:3'), 'discs: one group of 3, no side'); ok('A3 a mixed order: two ear stickers, six pieces, the discs are one group of three'); }

  /* ── B / C · the exported Right ear on real stored master files ── */
  const haveReal = fs.existsSync(REAL + 'BEAR_7932.ai') && fs.existsSync(SAMPLES + 'MISMATCHED_7134.ai');
  if (!haveReal) { console.log('  (real master files are not on this machine: sections B, C and D are skipped)'); }
  else {
    const g = globalThis; g.window = g; g.self = g; g.PDFLib = require('../../vendor/pdf-lib-1.17.1.min.js');
    require('../../charm-nest-geom.js'); require('../../charm-nest-vector.js'); require('../../charm-nest-pdf.js');
    const P = g.CharmNestPDF, G = require('../../charm-nest-geom.js'), Ex = require('../../charm-nest-export.js');
    const charmOf = async file => {   // readMasterCharm of the page: the biggest charm, the others folded in, the hoop welded
      const parsed = await P.parseSource(new Uint8Array(fs.readFileSync(file)), path.basename(file));
      const gr = P.groupCharms(parsed, { minPt: 6 });
      const charm = gr.charms.reduce((a, b) => (b.bbox[2] - b.bbox[0]) * (b.bbox[3] - b.bbox[1]) > (a.bbox[2] - a.bbox[0]) * (a.bbox[3] - a.bbox[1]) ? b : a);
      for (const c of gr.charms) if (c !== charm) { for (const m of c.members) if (!charm.members.includes(m)) charm.members.push(m); charm.topIndices = [...new Set(charm.topIndices.concat(c.topIndices))]; charm.bbox = [Math.min(charm.bbox[0], c.bbox[0]), Math.min(charm.bbox[1], c.bbox[1]), Math.max(charm.bbox[2], c.bbox[2]), Math.max(charm.bbox[3], c.bbox[3])]; }
      const r = P.integrateRings(charm); assert.equal(r.left.length, 0, 'the hoop joins its charm');
      charm.sourceId = 's'; charm.strokePt = charm.strokePt || .5; return { parsed, charm };
    };
    const centreOf = c => [(c.bbox[0] + c.bbox[2]) / 2, (c.bbox[1] + c.bbox[3]) / 2];
    // sheet with the Left ear at x=100 and the Right ear at x=300, same turn; returns the leaf paths of each half, in file order
    async function ears(parsed, left, right, angle) {
      left = Object.assign({}, left, { centerPt: centreOf(left), name: '4190000001 · X · 1/2', side: 'L' }); right = Object.assign({}, right, { centerPt: centreOf(right), name: '4190000001 · X · 2/2', side: 'R' });
      const bytes = await P.buildSheet({ sheet: { wPt: 420, hPt: 300 }, sources: new Map([['s', parsed]]), placements: [{ charm: left, angle: angle || 0, cxPt: 100, cyPt: 150, scale: 1 }, { charm: right, angle: angle || 0, cxPt: 300, cyPt: 150, scale: 1 }], title: 't' });
      const sheet = await P.parseSource(bytes, 'sheet'), paths = Ex.leaves(sheet).filter(p => p.layer !== 'SHEET (do not cut)');
      const mid = p => (p.bbox[0] + p.bbox[2]) / 2;
      return { sheet, bytes, L: paths.filter(p => mid(p) < 210), R: paths.filter(p => mid(p) >= 210), names: Ex.layerNames(sheet) };
    }
    const ptsOf = p => { const a = []; for (const sp of p.subpaths) for (const o of sp) { if (o[0] === 'h') continue; for (let i = 1; i < o.length; i++) a.push(o[i]); } return a; };
    const attr = p => [p.fill ? 1 : 0, p.stroke ? 1 : 0, (p.strokeRGB || []).map(v => v.toFixed(2)).join(), (p.fillRGB || []).map(v => v.toFixed(2)).join(), (+p.lwPt || 0).toFixed(2)].join('|');
    // the Right ear's path i is the Left ear's path i reflected about the vertical through its own centre, point by point (angle 0: the nester placed both upright)
    function assertMirror(who, L, R) {
      assert.equal(L.length, R.length, who + ': same number of paths (' + L.length + ' / ' + R.length + ')');
      let worst = 0;
      for (let i = 0; i < L.length; i++) {
        const a = ptsOf(L[i]), b = ptsOf(R[i]); assert.equal(a.length, b.length, who + ': path ' + i + ' has the same points'); assert.equal(attr(L[i]), attr(R[i]), who + ': path ' + i + ' keeps its colour, width and fill');
        for (let k = 0; k < a.length; k++) worst = Math.max(worst, Math.hypot((100 - a[k][0]) - (b[k][0] - 300), a[k][1] - b[k][1]));
      }
      assert(worst < 0.01, who + ': the Right ear is the x-mirror of the Left within ' + worst.toFixed(4) + ' pt'); return worst;
    }
    // one-body designs: a stud with CUT / ENGRAVE / HATCH, a huggie with a welded hoop, a plain animal
    for (const name of ['BEAR_7932.ai', 'ACORN_5207_HUGGIE_.ai', 'BEAR_1.ai', 'ANGEL_33719_HUGGIE_.ai']) {
      if (!fs.existsSync(REAL + name)) continue;
      const { parsed, charm } = await charmOf(REAL + name);
      const left = Pair.pieceGeometry(charm, { side: 'L', bodyIndex: 0, mirror: false }), right = Pair.pieceGeometry(charm, { side: 'R', bodyIndex: 0, mirror: true });
      assert.notEqual(right, left); assert.equal(Pair.mirrorOf(right).members.length, left.members.length);
      for (const ang of [0, 180]) { const e = await ears(parsed, left, right, ang); if (ang === 0) assertMirror(name, e.L, e.R); else assert.equal(e.L.length, e.R.length); }
    }
    ok('B1 real one-body designs (stud with CUT/ENGRAVE/HATCH, huggies with a welded hoop): the Right ear is the exact x-mirror of the Left, every path, colour and width kept');
    // mismatched pairs: the Left ear is the left body as drawn, the Right ear is the right body turned over
    for (const name of ['MISMATCHED_7134.ai', 'KEY_5598.ai', 'HEART_2206.ai']) {
      const { parsed, charm } = await charmOf(SAMPLES + name);
      assert.equal(Pair.bodiesOf(charm).length, 2, name + ' has two bodies'); assert(Pair.isMismatched(charm));
      const bodies = Pair.bodiesOf(charm), lg = Pair.pieceGeometry(charm, { side: 'L', bodyIndex: 0, mirror: false }), rg = Pair.pieceGeometry(charm, { side: 'R', bodyIndex: 1, mirror: true });
      const e = await ears(parsed, lg, rg, 0);
      const w = b => b.bbox[2] - b.bbox[0], mid = ps => (Math.min(...ps.map(p => p.bbox[0])) + Math.max(...ps.map(p => p.bbox[2]))) / 2;
      const wid = ps => Math.max(...ps.map(p => p.bbox[2])) - Math.min(...ps.map(p => p.bbox[0]));
      assert(Math.abs(wid(e.L) - w(bodies[0])) < 0.05 && Math.abs(wid(e.R) - w(bodies[1])) < 0.05, name + ': each ear is its own body (' + wid(e.L).toFixed(1) + ' / ' + wid(e.R).toFixed(1) + ' pt wide)');
      assert.equal(e.L.length, bodies[0].members.filter(m => m.kind === 'path').length); assert.equal(e.R.length, bodies[1].members.filter(m => m.kind === 'path').length);
      // the Right body's points are the as-drawn body 1 reflected about the vertical through the middle of its box, then put where the nester put the piece
      const [ccx, ccy] = centreOf(rg), src = bodies[1].members.filter(m => m.kind === 'path'); let worst = 0;
      e.R.forEach((p, i) => { const a = ptsOf(src[i]), b = ptsOf(p); for (let k = 0; k < a.length; k++) worst = Math.max(worst, Math.hypot((300 - (a[k][0] - ccx)) - b[k][0], (150 + (a[k][1] - ccy)) - b[k][1])); });
      assert(worst < 0.01, name + ': the Right ear is body 2 turned over (' + worst.toFixed(4) + ' pt)');
    }
    ok('B2 real mismatched pairs (MISMATCHED_7134, KEY_5598, HEART_2206): the Left ear is the left body, the Right ear the right body mirrored, each at its own size');

    // C · the layers: the Right ear is on the master's own layers (CUT / ENGRAVE / HATCH), so the laser sees one cut layer and one engrave layer for both ears
    for (const name of ['BEAR_7932.ai', 'ACORN_5207_HUGGIE_.ai', 'BEAR_1.ai', 'ANGEL_33719_HUGGIE_.ai']) {
      if (!fs.existsSync(REAL + name)) continue;
      const { parsed, charm } = await charmOf(REAL + name); const left = Pair.pieceGeometry(charm, { side: 'L', mirror: false }), right = Pair.pieceGeometry(charm, { side: 'R', mirror: true });
      const e = await ears(parsed, left, right, 0), lay = ps => ps.map(p => /(?:^|\s)(?:Left|Right)$/.test(p.layer) ? 'EAR' : p.layer).sort().join('|');   // (a piece's own layer is named for its ear: Left / Right)
      assert.equal(lay(e.R), lay(e.L), name + ': the Right ear is on the layers the Left is on, path for path (Left ' + lay(e.L) + ', Right ' + lay(e.R) + ')');
      const dxf = t => { const m = {}; const L = t.split('\r\n'); for (let i = 0; i < L.length - 1; i++) if (L[i] === '0' && (L[i + 1] === 'LWPOLYLINE' || L[i + 1] === 'HATCH')) { let j = i; while (L[j] !== '8') j++; m[L[j + 1]] = (m[L[j + 1]] || 0) + 1; } return m; };
      const d = dxf(Ex.dxf(Ex.productionPaths(e.sheet), e.names).text);
      for (const k of Object.keys(d)) if (k !== 'SHEET (do not cut)' && !/Left|Right/.test(k)) assert(d[k] % 2 === 0, name + ': layer ' + k + ' holds the same number of entities for both ears (' + d[k] + ')');
      const ownL = Object.entries(d).filter(([k]) => /Left/.test(k)).reduce((a, [, v]) => a + v, 0), ownR = Object.entries(d).filter(([k]) => /Right/.test(k)).reduce((a, [, v]) => a + v, 0); assert.equal(ownL, ownR, name + ': as many entities on the Left\'s own layer as on the Right\'s (' + ownL + '/' + ownR + ')');
      // the file is still one valid PDF: the layers the Right ear names are listed where the Left's are
      const re = await P.parseSource(e.bytes, 'again'); assert(re.segments.length >= 2);
    }
    ok('C1/C2 the exported Right ear is on the master\'s own layers (CUT / ENGRAVE / HATCH) in the .ai and in the DXF, entity for entity like the Left (4 real per-SKU files)');

    // D · the back flip is not the left/right mirror
    for (const name of ['BEAR_7932.ai', 'ACORN_5207_HUGGIE_.ai', 'BEAR_1.ai', 'ANGEL_33719_HUGGIE_.ai']) {
      const { charm } = await charmOf(REAL + name), L = Pair.pieceGeometry(charm, { side: 'L', mirror: false }), R = Pair.pieceGeometry(charm, { side: 'R', mirror: true });
      const vL = G.backView(L, { res: 6 }), vR = G.backView(R, { res: 6 });
      const polys = v => v.members.map(m => Pair._flatten(m, 12)).flat(), dist = (p, pl) => { let b = 1e9; for (const poly of pl) for (let i = 0, j = poly.length - 1; i < poly.length; j = i++) { const ax = poly[j][0], ay = poly[j][1], bx = poly[i][0], by = poly[i][1], dx = bx - ax, dy = by - ay, l2 = dx * dx + dy * dy, t = l2 ? Math.max(0, Math.min(1, ((p[0] - ax) * dx + (p[1] - ay) * dy) / l2)) : 0; b = Math.min(b, Math.hypot(ax + t * dx - p[0], ay + t * dy - p[1])); } return b; };
      const cL = (vL.outline.bbox[0] + vL.outline.bbox[2]) / 2, cR = (vR.outline.bbox[0] + vR.outline.bbox[2]) / 2, yL = (vL.outline.bbox[1] + vL.outline.bbox[3]) / 2, yR = (vR.outline.bbox[1] + vR.outline.bbox[3]) / 2;
      const mL = polys(vL).map(poly => poly.map(p => [cR - (p[0] - cL), p[1] - yL + yR])); let worst = 0;
      for (const poly of polys(vR)) for (const p of poly) worst = Math.max(worst, dist(p, mL));
      assert(worst < 0.01, name + ': back(Right) is the mirror of back(Left), off by ' + worst.toFixed(4) + ' pt'); assert(Object.values(vR.checks).every(Boolean), name + ': the flip checks pass on the mirrored charm');
      assert(Math.abs(vL.upAngle + vR.upAngle - 180) < 1e-6 || Math.abs(vL.upAngle - vR.upAngle) < 4, name + ': hoop-up angles are mirror angles (' + vL.upAngle.toFixed(1) + ' / ' + vR.upAngle.toFixed(1) + ')');
    }
    ok('D1 the back view of the Right ear is the mirror of the Left ear\'s back view (hoop-up), not a second flip: 4 real designs');
  }

  /* ── E · a stud pair engraved with different words per ear ── */
  {
    const rid = '4190000007', tx = '5000000070', ids = [1, 2, 3, 4].map(n => `${rid}_${tx}_${n}`);   // quantity 2: L R L R
    const pool = new Map(ids.map((id, i) => [id, { poolId: id, side: i % 2 ? 'R' : 'L', mirror: i % 2 === 1, bodyIndex: 0, groupKey: `${rid}:${tx}`, groupSize: 4 }]));
    const ctx = { poolRow: id => pool.get(id), charmOf: () => null, entryFor: () => null, pair: Pair };
    const row = { key: `${rid}_${tx}`, poolIds: ids.slice(), spec: { designSku: 'PAIR-STUD' }, line: {} };
    const plan = Sides.plan(ctx, row, []);
    assert(plan.split); assert.deepEqual(plan.slots, ['L', 'R']); assert.deepEqual(Sides.idsOfSlot(ctx, row, 'L'), [ids[0], ids[2]]); assert.deepEqual(Sides.idsOfSlot(ctx, row, 'R'), [ids[1], ids[3]]);
    // Left says "Anna" and is approved; Right says "Ben" and still waits on its words
    const jobs = [{ slot: 'L', copies: [ids[0], ids[2]], engraveRec: { needed: true, state: 'written', approved: true, text: 'Anna', approvedBy: 'Paul', approvedAt: 1 } }, { slot: 'R', copies: [ids[1], ids[3]], engraveRec: { needed: true, state: 'words', approved: false, text: 'Ben' } }];
    const sum = Sides.summary(ctx, undefined, jobs);
    assert.equal(sum.approved, false, 'the line is not approved while one ear waits'); assert.equal(sum.state, 'words'); assert.match(sum.text, /Left: Anna/); assert.match(sum.text, /Right: Ben/);
    assert.equal(sum.pieces[ids[0]].approved, true); assert.equal(sum.pieces[ids[1]].approved, false); assert.equal(sum.pieces[ids[3]].state, 'words');
    refuseNestedArrays(JSON.parse(JSON.stringify(sum)), 'engrave summary');
    const R = require('../../charm-nest-readiness.js');
    const dec = R.decisions([{ key: row.key, poolIds: ids, engrave: sum, state: 'pooled' }]);
    assert.equal(dec[ids[0]].approved, true); assert.equal(dec[ids[2]].approved, true); assert.equal(dec[ids[1]].approved, false); assert.equal(dec[ids[3]].approved, false);
    // a sheet that holds both Lefts is released on the Left approval alone; a sheet that holds a Left and a Right waits
    const backs = ids.map(id => ({ poolId: id, approvedAt: 1, approvedBy: 'Paul', verified: { geometry: { ok: true }, file: { ok: true } }, outputs: { ai: { path: 'x', url: 'u' } } }));
    const sheetOf = (sid, idList, approvedIds) => ({ id: sid, poolIds: idList, engraving: Object.fromEntries(idList.map(id => [id, dec[id]])), backPool: backs.filter(b => approvedIds.includes(b.poolId)), placedCount: idList.length, placements: idList.map(id => ({ id })) });
    const leftSheet = R.sheet(sheetOf('s1', [ids[0], ids[2]], [ids[0], ids[2]])), mixed = R.sheet(sheetOf('s2', [ids[0], ids[1]], [ids[0]]));
    assert.equal(leftSheet.stages.approval, true, 'the sheet of Lefts is approved'); assert.equal(mixed.stages.approval, false, 'a sheet holding an unapproved Right ear is not');
    // seals: one welded event per stud LINE, never one per ear (the Welding page keys it by the transaction)
    const weld = read('weld-1.html'); assert(/id: `weld-1-\$\{id\}-welded-\$\{tids\[i\]\}-\$\{m\}`/.test(weld) && /StationTimeline\.did\("welded"/.test(weld), 'the welded seal is keyed by order (and stud line), nothing per piece');
    ok('E1 stud pair with different words per ear: two jobs, the line is not approved until both are, readiness per sheet follows each ear, no nested array, one welded seal per line');
  }

  /* ── F · stickers of a card with two pair lines can be told apart; q2 numbering ── */
  {
    const row = (form, q) => ({ spec: { form, quantity: q }, line: { quantity: q } });
    const one = PL.stickerPieces([row('earrings', 1), row('huggie', 1)], () => null);
    assert.equal(one.length, 4);
    const key = s => s.side + s.n + '/' + s.of;
    assert.equal(new Set(one.map(key)).size, 4, 'four stickers of one order, no two alike: ' + one.map(key).join(' '));
    assert.deepEqual(one.map(s => s.side), ['L', 'R', 'L', 'R']); assert.deepEqual(PL.stickerPieces([row('earrings', 2)], () => null).map(key), ['L1/2', 'R1/2', 'L2/2', 'R2/2']);
    assert.equal(PL.stickerPieces([row('charm', 1), row('earring-single', 1)], () => null), null, 'no pair on the card: the one order sticker');
    ok('F1 a card with two pair lines numbers its ears across the card (1/2, 2/2), so no two stickers of an order are alike');
  }

  /* ── G · the order window ── */
  {
    const Core = require('../../charm-nest-order-pieces.js'), rid = '4200000009', tx = '9009';
    const mk = (copy, side) => ({ poolId: `${rid}_${tx}_${copy}`, orderId: rid, transactionId: tx, lineKey: `${rid}_${tx}`, sku: 'PAIR-STUD', material: 'gold', copy, state: 'ready', sheetId: null, updatedAt: 1, side, bodyIndex: 0, groupKey: `${rid}:${tx}`, groupSize: 2, mirror: side === 'R' });
    const line = { key: `${rid}_${tx}`, transactionId: tx, sku: 'PAIR-STUD', title: 'Crab stud earrings', material: 'gold', quantity: 1, state: 'pooled', poolIds: [`${rid}_${tx}_1`, `${rid}_${tx}_2`], spec: { pieceCount: 2, form: 'earrings', pair: { earring: true, mismatched: false, kind: 'pair' } }, problems: [] };
    const kindOf = l => Pair.kindOf({ spec: l.spec, quantity: l.quantity });
    const ps = Core.resolve({ orderId: rid, lines: [line], pools: [mk(1, 'L'), mk(2, 'R')], sheets: [], sheetsKnown: true, pairOf: () => null, kindOf });
    assert.deepEqual(ps.map(p => p.side), ['L', 'R']);
    open('PAIRORDERWIN', ps.every(p => p.kind === 'pair') && Core.spread(ps).groups[0].kind === 'pair', 'G1 a matching stud pair is kind "pair" in the order window\'s pieces (it says "' + ps[0].kind + '")');
  }
})().catch(e => { console.error(e); process.exit(1); });
