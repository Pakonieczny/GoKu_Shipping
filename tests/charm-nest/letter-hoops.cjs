// The two hooped alphabets (LETTERS-HOOPS, Paul 9 Nov 2026), OFFLINE: the committed files under charm-repo-adds/letters-hoops-2026-11-09/ are read
// back with the app's own parser and geometry (charm-nest-pdf.js, charm-nest-vector.js) and checked against the 26 source letters committed beside them.
// It proves, for every one of the 52 per-SKU files:
//   · it is ONE charm, ONE closed cut outline, nothing loose (a second weld, integrateRings, finds no hoop left to join), the hoop's hole is a cut line;
//   · the huggie set's letter is 6 mm (a c e n o r s u v x z) or 7 mm (b d f g h i j k l p q t y m w) on its longest side, scaled whole, never larger (Paul, 9 Nov 2026); the necklace set's is the source size;
//   · its metal is exactly the source letter (scaled, for a huggie) + the reference ring (necklace Ø 6.5 pt / hole 3.684 pt, huggie Ø 9.354 / 5.844, as NECKLACE_CHARM-n and
//     NECKLACE_CHARM-n (HUGGIE) carry them) − the ring's hole, plus only the specks placement.json lists as filled: the letter keeps its size and outline;
//   · the ring sits at the letter's top (its hole just above the letter's own top under it), at least 0.5 pt from every counter, and shares a real joint with it;
//   · records.json names the 52 SKUs (LETTER_NECKLACE-n, LETTER_NECKLACE-n (HUGGIE), a = 0 … z = 25), each with its own .ai and picture in files/, none blocked or open.
//   node tests/charm-nest/letter-hoops.cjs
'use strict';
const fs = require('fs'), path = require('path'), assert = require('assert');
const root = path.join(__dirname, '../..');
const { CharmNestPDF: P } = require(path.join(root, 'netlify/functions/_charmNestPdf.js'));
const V = require(path.join(root, 'charm-nest-vector.js'));
const DIR = path.join(root, 'charm-repo-adds/letters-hoops-2026-11-09');
let n = 0; const ok = (c, m) => { assert(c, m); n++; };
const area = p => Math.abs(p.reduce((v, q, i) => v + q[0] * p[(i + 1) % p.length][1] - q[1] * p[(i + 1) % p.length][0], 0) / 2);
const regionArea = r => r.reduce((v, p) => v + (p.hole ? -1 : 1) * area(p.points), 0);
const circ = (cx, cy, r, k = 192) => Array.from({ length: k }, (_, i) => [cx + r * Math.cos(2 * Math.PI * i / k), cy + r * Math.sin(2 * Math.PI * i / k)]);
const bbox = ps => { let a = [Infinity, Infinity, -Infinity, -Infinity]; for (const p of ps) for (const q of p) a = [Math.min(a[0], q[0]), Math.min(a[1], q[1]), Math.max(a[2], q[0]), Math.max(a[3], q[1])]; return a; };
const L = 'abcdefghijklmnopqrstuvwxyz';

async function charmOf(file) {
  const buf = fs.readFileSync(file);
  const parsed = await P.parseSource(new Uint8Array(buf.buffer, buf.byteOffset, buf.byteLength), path.basename(file));
  const live = P.groupCharms(parsed, { minPt: 6 }).charms.filter(c => c.mergedInto == null);
  return { parsed, live };
}
/** A charm's metal: its outline (even-odd: its own subpaths are windows) less every other cut line. */
function metalOf(c) {
  const out = c.outline.subpaths.map(sp => V.flatten(sp).points);
  const rule = c.outline.fill && !String(c.outline.paintOp || '').endsWith('*') ? 'nonzero' : 'evenodd';
  const body = V.boolean(out, [], 'union', rule);
  const cuts = P.cutLinesOf(c).flatMap(m => m.subpaths.map(sp => V.flatten(sp).points));
  return cuts.length ? V.boolean(body.map(p => p.points), cuts, 'difference', 'evenodd', 'evenodd') : body;
}
const signedPolys = r => r.map(p => p.points);

(async () => {
  const rec = JSON.parse(fs.readFileSync(path.join(DIR, 'records.json'), 'utf8'));
  const place = JSON.parse(fs.readFileSync(path.join(DIR, 'placement.json'), 'utf8'));
  const want = [];
  for (let i = 0; i < 26; i++) want.push(`LETTER_NECKLACE-${i}`);
  for (let i = 0; i < 26; i++) want.push(`LETTER_NECKLACE-${i} (HUGGIE)`);
  ok(JSON.stringify(rec.entries.map(e => e.sku)) === JSON.stringify(want), 'records.json names the 52 SKUs, a…z necklace then a…z huggie');
  ok(/^[0-9a-f]{64}$/.test(rec.masterHash) && rec.masterName === 'LETTER-HOOPS_2026-1109.ai' && rec.sheet && rec.sheet.pageW > 0 && !(rec.blocked || []).length, 'one master sheet, its hash, nothing blocked');
  { const crypto = require('crypto'); ok(crypto.createHash('sha256').update(fs.readFileSync(path.join(DIR, rec.masterName))).digest('hex') === rec.masterHash, 'the committed master sheet is the file the records were made from'); }

  // the sources
  const src = [];
  for (let i = 0; i < 26; i++) {
    const { live } = await charmOf(path.join(DIR, 'source', `LETTER_EARRING-${i}.ai`));
    ok(live.length === 1 && live[0].members.length === 1, `source ${L[i]}: one outline`);
    const c = live[0], m = metalOf(c);
    src.push({ metal: m, counters: m.filter(p => p.hole).map(p => p.points), bb: bbox(m.filter(p => !p.hole).map(p => p.points)) });
  }

  for (const e of rec.entries) {
    const huggie = / \(HUGGIE\)$/.test(e.sku), i = +/-(\d+)/.exec(e.sku)[1], H = place.hoops[huggie ? 'huggie' : 'necklace'];
    const pl = place.charms.find(x => x.sku === e.sku), what = `${e.sku} (${L[i]}, ${huggie ? 'huggie' : 'necklace'})`;
    ok(pl && pl.letter === L[i] && pl.source === `LETTER_EARRING-${i}`, `${what}: placement recorded`);
    ok(!e.open && !e.blocked && e.readerV === P.READER_VERSION, `${what}: closed, not blocked`);
    const ai = path.join(DIR, 'files', e.aiPath), png = path.join(DIR, 'files', e.thumbPath);
    ok(fs.existsSync(ai) && fs.existsSync(png) && fs.readFileSync(png).slice(1, 4).toString() === 'PNG', `${what}: its .ai and picture are committed`);
    const { live } = await charmOf(ai);
    ok(live.length === 1, `${what}: one charm in its file`);
    const c = live[0];
    const again = P.integrateRings({ ...c, members: c.members.slice() });
    ok(again.welded === 0 && !again.left.length, `${what}: the hoop is already welded (nothing loose to join)`);
    ok(c.outline.closed && c.outline.subpaths.length === 1, `${what}: one closed cut outline`);
    // the hoop's hole: a circle of the reference size among the cut lines
    const holes = P.cutLinesOf(c).map(m => V.flatten(m.subpaths[0]).points);
    const fit = p => { const b = bbox([p]); const cx = (b[0] + b[2]) / 2, cy = (b[1] + b[3]) / 2, r = (b[2] - b[0] + b[3] - b[1]) / 4; return { cx, cy, r, dev: Math.max(...p.map(q => Math.abs(Math.hypot(q[0] - cx, q[1] - cy) - r))) }; };
    const ringHole = holes.map(fit).filter(f => f.dev < 0.02 && Math.abs(f.r - H.hole) < 0.02);
    ok(ringHole.length === 1, `${what}: the hoop's hole (Ø ${(2 * H.hole).toFixed(3)} pt) is cut`);
    ok(e.holes === holes.length && e.members === c.members.length, `${what}: the record counts its holes`);
    // the metal, laid over the source letter: the letter + the ring − its hole (+ the specks placement.json says were filled)
    const s = src[i], h = ringHole[0];
    const dx = h.cx - pl.hoop.cx - s.bb[0], dy = h.cy - pl.hoop.cy - s.bb[1];
    // (the huggie set's letter is scaled about its own bottom-left corner to 6 or 7 mm, placement.json `scale`; the necklace set's is the source's, scale 1)
    const k = pl.scale;
    ok(huggie ? k > 0 && k <= 1 : k === 1, `${what}: ${huggie ? 'scale ' + k : 'the source size'}`);
    const letter = signedPolys(s.metal).map(p => p.map(q => [s.bb[0] + (q[0] - s.bb[0]) * k + dx, s.bb[1] + (q[1] - s.bb[1]) * k + dy]));
    if (huggie) {
      const lw = (s.bb[2] - s.bb[0]) * k, lh = (s.bb[3] - s.bb[1]) * k, longMm = Math.max(lw, lh) * 25.4 / 72, want = 'bdfghijklpqtymw'.includes(L[i]) ? 7 : 6;
      ok(Math.abs(longMm - want) < 0.1 || (k === 1 && longMm < want * 1.01), `${what}: the letter's longest side is ${longMm.toFixed(2)} mm (${want} mm for its shape)`);
    }
    const letterRegion = V.boolean(letter, [], 'union', 'evenodd');
    let expect = V.boolean(letterRegion.map(p => p.points), [circ(h.cx, h.cy, H.outer)], 'union', 'evenodd', 'nonzero');
    const holesNow = expect.filter(p => p.hole), counters = V.boolean(letter, [], 'union', 'evenodd').filter(p => p.hole).map(p => p.points);
    const specks = holesNow.filter(p => area(p.points) < 5 && !counters.some(c2 => Math.abs(area(c2) - area(p.points)) < 0.01));
    ok(specks.length === pl.checks.specksFilled.length, `${what}: the specks filled are the ones listed (${specks.length})`);
    if (specks.length) expect = V.boolean(expect.map(p => p.points), specks.map(p => p.points), 'union', 'evenodd', 'nonzero');
    expect = V.boolean(expect.map(p => p.points), [circ(h.cx, h.cy, H.hole)], 'difference', 'evenodd', 'nonzero');
    const got = metalOf(c);
    // how far apart they are: the area of their union less the area of their common part (a curve is a polygon within 0.002 mm in both, so a
    // hairline along the edge is allowed: 0.004 pt on average along the outline, about a thousandth of a millimetre)
    const G = signedPolys(got), E = signedPolys(expect);
    const off = regionArea(V.boolean(G, E, 'union', 'evenodd', 'evenodd')) - regionArea(V.boolean(G, E, 'intersection', 'evenodd', 'evenodd'));
    const perimeter = got.reduce((v, p) => v + p.points.reduce((w, q, k) => w + Math.hypot(p.points[(k + 1) % p.points.length][0] - q[0], p.points[(k + 1) % p.points.length][1] - q[1]), 0), 0);
    ok(off < 0.004 * perimeter, `${what}: the metal is the source letter + the ring − its hole (off by ${off.toFixed(4)} pt² over ${perimeter.toFixed(0)} pt of edge)`);
    ok(Math.abs(regionArea(got) - regionArea(expect)) < 0.004 * perimeter, `${what}: same metal area`);
    // the letter keeps its size: the metal below the ring is the source letter's box
    const gb = bbox(got.filter(p => !p.hole).map(p => p.points));
    ok(Math.abs(gb[1] - (s.bb[1] + dy)) < 0.01, `${what}: the letter's foot is where it was`);
    // where the ring sits: its hole's lowest point just above the letter's own top under it (the number set's 0.19 / 0.01 pt), clear of every counter by 0.5 pt
    const strip = V.boolean(letter, [[[h.cx - H.hole, -1e4], [h.cx + H.hole, -1e4], [h.cx + H.hole, 1e4], [h.cx - H.hole, 1e4]]], 'intersection', 'evenodd');
    const topUnder = Math.max(...strip.filter(p => !p.hole).flatMap(p => p.points.map(q => q[1])));
    ok(Math.abs((h.cy - H.hole) - topUnder - (H.gap + (pl.checks.liftedPt || 0))) < 0.02, `${what}: the hole sits ${(H.gap + (pl.checks.liftedPt || 0)).toFixed(2)} pt above the letter's top`);
    const dCounter = counters.length ? Math.min(...counters.map(c2 => { let d = Infinity; for (let k = 0; k < c2.length; k++) { const a = c2[k], b = c2[(k + 1) % c2.length], vx = b[0] - a[0], vy = b[1] - a[1], l2 = vx * vx + vy * vy, t = l2 ? Math.max(0, Math.min(1, ((h.cx - a[0]) * vx + (h.cy - a[1]) * vy) / l2)) : 0; d = Math.min(d, Math.hypot(h.cx - a[0] - t * vx, h.cy - a[1] - t * vy)); } return d - H.outer; })) : Infinity;
    ok(dCounter >= 0.49, `${what}: the ring keeps ${Number.isFinite(dCounter) ? dCounter.toFixed(2) : 'all'} pt of metal from every counter`);
    // the ring's centre: on the stem or dot of b d f h k l t i j, else the centre of the letter
    const letterBox = bbox(V.boolean(letter, [], 'union', 'evenodd').filter(p => !p.hole).map(p => p.points));
    if (!'bdfhkltij'.includes(L[i])) ok(Math.abs(h.cx - (letterBox[0] + letterBox[2]) / 2) < 0.01, `${what}: centred on the letter`);
    else ok(h.cx > letterBox[0] && h.cx < letterBox[2] && /stem|dot/.test(pl.rule), `${what}: on its ${pl.rule.includes('dot') ? 'dot' : 'stem'}`);
  }
  console.log(`letter-hoops: ${n} checks passed`);
})().catch(e => { console.error(e); process.exit(1); });
