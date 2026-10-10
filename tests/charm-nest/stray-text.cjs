// "There should be no text here beside the bar necklace" (Paul, 10 Oct 2026, the BAR_3290 card). A master's artists type a
// customer's sample name beside a charm, outline it and leave the outlined letters on the CUT layer as closed black-filled shapes
// (no live text behind them). The grouping handed the first two letters, the ones within 24 pt, to the bar: the S as blue
// hatching, the "o" as a hoop-sized cut-out (the card said "3 holes"), and both sat in the bar's size, its stored file and its
// thumbnail. Ink that lies wholly outside a charm's cut outline and does not touch it is on no metal: it is not the charm.
//   node tests/charm-nest/stray-text.cjs
const assert = require('node:assert/strict');
const path = require('node:path');
global.self = global; global.PDFLib = require(path.join(__dirname, '../../vendor/pdf-lib-1.17.1.min.js')); require('../../charm-nest-pdf.js');
const P = global.CharmNestPDF, G = require('../../charm-nest-geom.js');
const { PDFDocument, PDFName, PDFString, StandardFonts } = PDFLib;

// ── 1 · the bar, as the master draws it: cut outline + two hoop holes on CUT, blue hatching on HATCH, a row of outlined letters on CUT ──
const num = n => (+n).toFixed(3), K = 0.5523;
const box = (x, y, w, h) => `${num(x)} ${num(y)} m ${num(x + w)} ${num(y)} l ${num(x + w)} ${num(y + h)} l ${num(x)} ${num(y + h)} l h`;
const disc = (cx, cy, r, cw) => { const k = K * r, s = cw ? -1 : 1; return `${num(cx + r)} ${num(cy)} m ${num(cx + r)} ${num(cy + s * k)} ${num(cx + k)} ${num(cy + s * r)} ${num(cx)} ${num(cy + s * r)} c ${num(cx - k)} ${num(cy + s * r)} ${num(cx - r)} ${num(cy + s * k)} ${num(cx - r)} ${num(cy)} c ${num(cx - r)} ${num(cy - s * k)} ${num(cx - k)} ${num(cy - s * r)} ${num(cx)} ${num(cy - s * r)} c ${num(cx + k)} ${num(cy - s * r)} ${num(cx + r)} ${num(cy - s * k)} ${num(cx + r)} ${num(cy)} c h`; };
const BAR = [20, 50, 105, 70];                                   // x0, y0, x1, y1 (pt): 30 mm x 7 mm
async function master() {
  const doc = await PDFDocument.create(), page = doc.addPage([240, 100]), font = await doc.embedFont(StandardFonts.Helvetica);
  const ocg = n => doc.context.register(doc.context.obj({ Type: 'OCG', Name: PDFString.of(n) }));
  page.node.Resources().set(PDFName.of('Properties'), doc.context.obj({ MC1: ocg('HATCH'), MC2: ocg('CUT') }));
  const ops = ['/OC /MC1 BDC', 'q 0 0 1 rg'];                   // HATCH: blue fills inside the bar
  for (let i = 0; i < 4; i++) ops.push(disc(40 + i * 14, 60, 4 + (i % 2), false) + ' f');
  ops.push('Q', 'EMC', '/OC /MC2 BDC', `q 0 0 0 RG 0.283 w ${box(BAR[0], BAR[1], BAR[2] - BAR[0], BAR[3] - BAR[1])} S ${disc(25, 60, 1.7)} S ${disc(100, 60, 1.7)} S Q`);   // CUT: outline and two hoop holes
  // the sample name, outlined: black fills, no stroke. 8 letters, 4.7 pt apart, the first 8 pt from the bar's end. The "o" is a washer (two circles).
  const x0 = BAR[2] + 8, letters = [];
  for (let i = 0; i < 8; i++) { const x = x0 + i * 4.7; letters.push(i === 1 ? `${disc(x + 2.2, 54.5, 2.2)} ${disc(x + 2.2, 54.5, 1.2, true)} f*` : `${box(x, 52, 3.6, 5 + (i % 3))} f`); }
  ops.push(`q 0 0 0 rg ${letters.join('\n')} Q`, 'EMC');
  page.node.set(PDFName.of('Contents'), doc.context.obj([doc.context.register(doc.context.flateStream(ops.join('\n') + '\n'))]));
  page.drawText('BAR_3290', { x: 45, y: BAR[1] - 8, size: 5, font });
  return new Uint8Array(await doc.save({ useObjectStreams: false }));
}
const holes = c => P.cutLinesOf(c).length, pathsOf = f => f.segments.concat(f.nested).filter(s => s.kind === 'path');
const barOf = g => g.charms.find(c => Math.abs(c.outline.bbox[0] - BAR[0]) < 1 && Math.abs(c.outline.bbox[2] - BAR[2]) < 1);
const letterLike = m => m.kind === 'path' && m.bbox[0] > BAR[2];       // anything of the bar charm that stands right of its end

(async () => {
  const bytes = await master(), parsed = await P.parseSource(bytes, 'master.ai');
  const old = barOf(P.groupCharms(parsed, { minPt: 6, keepStray: true })), g = P.groupCharms(parsed, { minPt: 6 }), bar = barOf(g);
  assert(old && bar, 'the bar is found');

  // before (keepStray = the old reader): the letters within 24 pt are members, one of them is a "hole", the size grows
  const near = old.members.filter(letterLike);
  assert(near.length >= 2 && holes(old) === 3, `old reader: ${near.length} letters ride on the bar and the card says ${holes(old)} holes`);
  assert(old.bbox[2] > BAR[2] + 8, 'old reader: the letters are in the charm\'s box');

  // after: the bar is its outline, two holes and its hatching
  assert.equal(bar.members.filter(letterLike).length, 0, 'no letter is a member of the bar');
  assert.equal(holes(bar), 2, 'a bar has two holes');
  assert.equal(bar.members.filter(m => m !== bar.outline && !P.isCutLine(m)).length, 4, 'the four hatching shapes inside the bar stay');
  assert.deepEqual(bar.bbox.map(v => +v.toFixed(2)), bar.outline.bbox.map(v => +v.toFixed(2)), 'the charm\'s box is its cut outline again');
  const taken = g.stray.filter(t => t.charm === bar.index);
  assert(taken.length === near.length && taken.every(t => t.seg.stray && /letter/.test(t.why)), `the ${near.length} letters are listed as stray: ` + JSON.stringify(taken.map(t => t.why)));
  assert(!g.stray.some(t => t.charm !== bar.index), 'nothing else on the page is touched');

  // the SKU label under the bar is read as before, and the per-SKU file is the bar alone
  const lab = P.labelCharms(parsed, g.charms, { pattern: P.SKU_PATTERN_DEFAULT, gapPt: 8, widen: 0.25 });
  assert.equal(lab.labels.get(bar.index).sku, 'BAR_3290');
  const ai = await P.buildSingleCharm(bar, parsed), file = await P.parseSource(new Uint8Array(ai), 'one.ai');
  const g2 = P.groupCharms(file, { minPt: 6 });
  assert.equal(g2.charms.length, 1); assert.equal(holes(g2.charms[0]), 2); assert.equal(g2.charms[0].members.length, 7, 'the file holds the outline, 2 holes and 4 hatching shapes: no letter');

  // a file the old reader wrote (the letters are in it) reads clean now: the page drops them again at read time
  const oldAi = await P.buildSingleCharm(old, parsed), oldFile = await P.parseSource(new Uint8Array(oldAi), 'old.ai');
  assert(pathsOf(oldFile).length > pathsOf(file).length, 'the old file does hold the letters');
  const g3 = P.groupCharms(oldFile, { minPt: 6 }), c3 = g3.charms[0];
  assert.equal(g3.charms.length, 1); assert.equal(holes(c3), 2, 'read back: 2 holes'); assert.equal(c3.members.filter(letterLike).length, 0, 'read back: no letter');
  assert.deepEqual(c3.bbox.map(v => Math.round(v)), c3.outline.bbox.map(v => Math.round(v)));

  // the card is drawn without them: no black letter, no blue S beyond the bar's own hatching
  const fills = []; P.drawCharm(new Proxy({}, { get: (o, k) => k === 'fill' ? () => fills.push(o.fillStyle) : (k in o ? o[k] : () => {}), set: (o, k, v) => { o[k] = v; return true; } }), bar, (x, y) => [x, y], 4);
  assert.deepEqual(fills, ['rgb(0,0,255)', 'rgb(0,0,255)', 'rgb(0,0,255)', 'rgb(0,0,255)'], 'only the four hatching shapes are painted');

  // ── 2 · what must NOT go: nothing that is on the piece, against it, or a real hoop ──
  const seg = (o) => Object.assign({ kind: 'path', closed: true, stroke: true, fill: false, strokeRGB: [0, 0, 0], fillRGB: [0, 0, 0], lwPt: 0.28, paintOp: 'S', depth: 0, layer: 'CUT' }, o);
  const bb = subs => { const p = subs.flat().flatMap(o => o.slice(1)); return [Math.min(...p.map(q => q[0])), Math.min(...p.map(q => q[1])), Math.max(...p.map(q => q[0])), Math.max(...p.map(q => q[1]))]; };
  const rect = (x0, y0, x1, y1) => [['m', [x0, y0]], ['l', [x1, y0]], ['l', [x1, y1]], ['l', [x0, y1]], ['h']];
  const blob = (cx, cy, r, cw) => { const k = K * r, s = cw ? -1 : 1; return [['m', [cx + r, cy]], ['c', [cx + r, cy + s * k], [cx + k, cy + s * r], [cx, cy + s * r]], ['c', [cx - k, cy + s * r], [cx - r, cy + s * k], [cx - r, cy]], ['c', [cx - r, cy - s * k], [cx - k, cy - s * r], [cx, cy - s * r]], ['c', [cx + k, cy - s * r], [cx + r, cy - s * k], [cx + r, cy]], ['h']]; };
  const path = (subs, o) => seg(Object.assign({ subpaths: subs, bbox: bb(subs) }, o));
  const fill = (subs, o) => path(subs, Object.assign({ stroke: false, fill: true, paintOp: 'f' }, o));
  const group = (segs, opts) => { segs.forEach((s, i) => Object.assign(s, { index: i, start: i * 10, end: i * 10 + 9 })); return P.groupCharms({ segments: segs, nested: [], pageW: 400, pageH: 200, mediaBox: [0, 0, 400, 200] }, Object.assign({ minPt: 6 }, opts || {})); };
  const body = () => path([rect(0, 0, 60, 20)]);
  const mine = (segs, opts) => { const g = group(segs, opts); return { g, c: g.charms.find(c => c.outline === segs[0]) }; };
  const word = (x, y, n) => Array.from({ length: n }, (_, i) => fill([rect(x + i * 4.7, y, x + i * 4.7 + 3.6, y + 5)]));      // outlined letters in a row

  {   // inside the cut outline, everything stays: outlined letters of an engraved name (hatching), a cut-out, a hole
    const b = body(), inside = word(10, 6, 5), cut = path([blob(5, 10, 1.7)]);
    const { g, c } = mine([b, cut, ...inside]);
    assert(inside.every(m => c.members.includes(m)) && c.members.includes(cut) && g.stray.length === 0, 'engraving inside the cut outline stays, name or not');
  }
  {   // against the body: a bail that touches or overlaps the edge, a ring on it
    const b = body(), bail = path([rect(60, 7, 64, 13)]), over = fill([rect(58, 8, 63, 12)]);
    const { g, c } = mine([b, bail, over]);
    assert(c.members.includes(bail) && c.members.includes(over) && g.stray.length === 0, 'an attachment that touches or overlaps the body stays');
  }
  {   // a sample name typed at the very edge of a bar (the digits beside the VERTICAL_6607 bars stand 0.5 to 0.8 pt off it) is beside the bar, not on it
    const b = body(), row = word(60.7, 7, 3);
    const { g, c } = mine([b, ...row]);
    assert(row.every(m => !c.members.includes(m)) && g.stray.length === 3, 'a name typed 0.7 pt off the bar is not part of it');
  }
  {   // hoops: a ring against the body, and a real ring standing 6 pt clear (a HUGGIE ring): both are welded by the reader, so both stay
    const b = body(), touching = path([blob(-3.2, 10, 3.3), blob(-3.2, 10, 1.8, true)]), clear = path([blob(69, 10, 3.3), blob(69, 10, 1.8, true)]);
    const { g, c } = mine([b, touching, clear]);
    assert(c.members.includes(touching) && c.members.includes(clear) && g.stray.length === 0, 'a hoop the weld joins stays');
    const r = P.integrateRings(c); assert.equal(r.left.length, 0); assert(r.welded >= 1, 'and it is welded');
  }
  {   // a jump ring drawn as two circles, beside a body whose own box is letter-sized and whose centre lies outside its outline (a bow): the body is no letter, the two circles are no word
    const bow = path([[['m', [0, 0]], ['l', [18, 0]], ['l', [18, 5]], ['l', [6, 5]], ['l', [6, 12]], ['l', [18, 12]], ['l', [18, 17]], ['l', [0, 17]], ['h']]]);
    const outer = path([blob(9, -5.2, 4.7)]), inner = path([blob(9, -5.2, 2.9)]);
    const { g, c } = mine([bow, outer, inner]);
    assert(c.members.includes(outer) && c.members.includes(inner) && g.stray.length === 0, 'the two circles of a jump ring beside a bow stay');
    assert.equal(P.integrateRings(c).left.length, 0, 'and the weld joins them');
  }
  {   // a ring-sized shape that is a letter of a row ("o", "0") is a letter; one ring on its own at that distance is a hoop
    const b = body(), o = fill([blob(77.2, 10, 2.2), blob(77.2, 10, 1.2, true)], { paintOp: 'f*' }), row = [fill([rect(70, 8, 73.6, 13)]), o, fill([rect(79, 8, 82.6, 13)]), fill([rect(83.7, 8, 87.3, 13)])];
    const A = mine([b, ...row]); assert(!A.c.members.includes(o) && A.g.stray.some(t => t.seg === o), 'the "o" of a word beside the bar is dropped');
    const b2 = body(), lone = fill([blob(72, 10, 2.2), blob(72, 10, 1.2, true)], { paintOp: 'f*' });
    const B = mine([b2, lone]); assert(B.c.members.includes(lone) && B.g.stray.length === 0, 'the same ring alone, within reach of the body, is a hoop and stays');
  }
  {   // specks are dropped; a drawing that is not lettering is left alone (it is listed, not rewritten)
    const b = body(), dot = fill([rect(75, 9, 76.5, 10.5)]), art = fill([rect(66, 2, 90, 30)], { layer: 'HATCH' });
    const { g, c } = mine([b, dot, art]);
    assert(!c.members.includes(dot) && g.stray.some(t => t.seg === dot && /mark/.test(t.why)), 'a speck beside the body is dropped');
    assert(c.members.includes(art), 'a large drawing beside the body is not lettering: it stays for a person to look at');
  }
  {   // live text beside a round piece: outside its shape, inside its box
    const circle = path([blob(40, 40, 15)]), text = { kind: 'text', str: 'Jo', bbox: [52, 52, 55, 55], layer: 'CUT', fillRGB: [0, 0, 0], fill: true, start: 5, end: 9 };
    const { g, c } = mine([circle, text], { keepMarkers: true, keepSampleText: true });
    assert(!c.members.includes(text) && g.stray.some(t => t.seg === text && /text/.test(t.why)), 'text in the corner of the box, off the shape, is dropped');
    const inside = { ...text, bbox: [38, 38, 42, 42] };
    assert(mine([path([blob(40, 40, 15)]), inside], { keepMarkers: true, keepSampleText: true }).c.members.some(m => m.kind === 'text'), 'text on the shape stays');
  }
  console.log('stray-text OK · the BAR_3290 letters leave the bar (2 holes, its own size, no letter in the file), engraving, attachments and hoops stay');
})().catch(e => { console.error(e); process.exit(1); });
