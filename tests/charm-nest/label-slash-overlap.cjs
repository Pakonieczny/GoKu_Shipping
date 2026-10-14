// A master file's labels and parts (Paul's SKU file of 14 Oct 2026: 36 charms, only 26 labelled): the reader must index every charm of the page.
//   · a label with a "/" ("Masks Happy/Sad", "Spider / Web_1", "Cute Triceratops w/ hearts": real Etsy SKUs) was no SKU at all; it is read with a hyphen in the
//     slash's place (a SKU is a library document name and cannot hold a "/"); a fraction, a date, "N/A" and "FRONT/BACK" are still callouts
//   · a label whose box pokes 1-3 pt into the bottom of its charm's box (a mortarboard's tassel, a crescent's horn) still stands under that charm; 5 pt does not
//   · an outline drawn as a closed bar plus ONE open run that only retraces the bar's top edge is a closed outline (the Tiny Bar)
//   · a ring of three concentric circles touching its charm is that charm's hoop, not a charm of its own
//   · a ring beside its charm, out of the grouping's reach but within integrateRings' reach, joins the charm and is welded; a ring with no charm near it is reported
//   · the same outline written twice at one place is one charm
// Offline: a synthetic master built here, nothing is sent anywhere.
//   node tests/charm-nest/label-slash-overlap.cjs
const path = require('path'), assert = require('assert');
const { PDFDocument, PDFName, StandardFonts, rgb } = require(path.join(__dirname, '../../vendor/pdf-lib-1.17.1.min.js'));
const { CharmNestPDF: P } = require(path.join(__dirname, '../../netlify/functions/_charmNestPdf.js'));
const num = n => (+n).toFixed(3);
const circle = (cx, cy, r) => { const k = 0.5523 * r; return `${num(cx + r)} ${num(cy)} m ${num(cx + r)} ${num(cy + k)} ${num(cx + k)} ${num(cy + r)} ${num(cx)} ${num(cy + r)} c ${num(cx - k)} ${num(cy + r)} ${num(cx - r)} ${num(cy + k)} ${num(cx - r)} ${num(cy)} c ${num(cx - r)} ${num(cy - k)} ${num(cx - k)} ${num(cy - r)} ${num(cx)} ${num(cy - r)} c ${num(cx + k)} ${num(cy - r)} ${num(cx + r)} ${num(cy - k)} ${num(cx + r)} ${num(cy)} c h`; };
// a house-shaped charm 40 x 34 pt with its bottom edge at y = by (centre x = cx): a stroked, closed, near-black outline
const house = (cx, by) => `${num(cx - 20)} ${num(by)} m ${num(cx + 20)} ${num(by)} l ${num(cx + 20)} ${num(by + 24)} l ${num(cx + 8)} ${num(by + 34)} l ${num(cx - 8)} ${num(by + 34)} l ${num(cx - 20)} ${num(by + 24)} l h`;
const pen = body => `q 0.05 0.05 0.05 RG 0.5 w ${body} S Q`;

(async () => {
  const doc = await PDFDocument.create();
  const page = doc.addPage([700, 520]);
  const font = await doc.embedFont(StandardFonts.Helvetica);
  const ops = [], labels = [];
  // where a text's box top sits above the baseline we hand pdf-lib (measured on the parser's own reading, so the test is about the gap, not the font)
  const probe = await PDFDocument.create(); const pp = probe.addPage([100, 100]); pp.drawText('PROBE', { x: 10, y: 50, size: 6, font: await probe.embedFont(StandardFonts.Helvetica) });
  const probed = await P.parseSource(new Uint8Array(await probe.save({ useObjectStreams: false })), 'probe.ai');
  const tb = probed.segments.concat(probed.nested).find(s => s.kind === 'text').bbox, up = tb[3] - 50, wid = tb[2] - tb[0];
  // label: text whose box TOP is `gap` pt below the outline's bottom edge `by` (negative: it pokes into the box)
  const label = (text, cx, by, gap) => labels.push({ text, x: cx - font.widthOfTextAtSize(text, 6) / 2, y: by - gap - up });
  const col = i => 70 + i * 100, row = j => 440 - j * 105;      // charm j of row i: bottom edge at row(j)

  const C = {};                                                  // the charms of the page, by what each one is about
  // row 0: the slash labels, each cleanly 2 pt under its charm
  C.masks = [col(0), row(0)]; ops.push(pen(house(...C.masks))); label('Masks Happy/Sad', C.masks[0], C.masks[1], 2);
  C.spider = [col(1), row(0)]; ops.push(pen(house(...C.spider))); label('Spider / Web_1', C.spider[0], C.spider[1], 2);
  C.trice = [col(2), row(0)]; ops.push(pen(house(...C.trice))); label('Cute Triceratops w/ hearts', C.trice[0], C.trice[1], 2);
  // row 1: label overlap (a cap with its label 2 pt inside the box, a crescent 1.3 pt, a bat 5 pt: too deep to be under it)
  C.cap = [col(0), row(1)]; ops.push(pen(house(...C.cap))); label('GRAD CAP 1', C.cap[0], C.cap[1], -2);
  C.moon = [col(1), row(1)]; ops.push(pen(house(...C.moon))); label('Crescent_50058', C.moon[0], C.moon[1], -1.3);
  C.deep = [col(2), row(1)]; ops.push(pen(house(...C.deep))); label('Bat_1', C.deep[0], C.deep[1], -5);
  // row 2: the Tiny Bar (a closed 6-point bar and one open run along its top edge), a hoop of three concentric circles touching a charm, the same outline twice
  C.bar = [col(0), row(2)];
  { const [cx, by] = C.bar, bar = `${num(cx - 24)} ${num(by)} m ${num(cx + 24)} ${num(by)} l ${num(cx + 26)} ${num(by + 2)} l ${num(cx + 26)} ${num(by + 8)} l ${num(cx - 26)} ${num(by + 8)} l ${num(cx - 26)} ${num(by + 2)} l h ${num(cx - 20)} ${num(by + 8)} m ${num(cx + 20)} ${num(by + 8)} l`; ops.push(pen(bar)); label('Tiny Bar_7427', cx, by, 2); }
  C.hoop = [col(1), row(2)]; ops.push(pen(house(...C.hoop))); label('High_36971', C.hoop[0], C.hoop[1], 2);
  { const [cx, by] = C.hoop, cy = by + 34 + 2.75; ops.push(pen(`${circle(cx, cy, 3.25)} ${circle(cx, cy, 1.85)} ${circle(cx, cy, 3.25)}`)); }   // 6.5 pt across, touching the roof's top edge
  C.twice = [col(2), row(2)]; ops.push(pen(house(...C.twice))); ops.push(pen(house(...C.twice))); label('Hummingbird cut out_1', C.twice[0], C.twice[1], 2);
  // row 3: a ring 10 pt clear of its charm (outside the grouping's reach, inside the hoop reach), and a ring with no charm anywhere near it
  C.far = [col(0), row(3)]; ops.push(pen(house(...C.far))); label('Dancing_81994', C.far[0], C.far[1], 2);
  { const [cx, by] = C.far, cy = by + 34 + 10 + 3.25; ops.push(pen(`${circle(cx, cy, 3.25)} ${circle(cx, cy, 1.85)}`)); }
  ops.push(pen(`${circle(col(2), row(3) + 20, 3.25)} ${circle(col(2), row(3) + 20, 1.85)}`));   // a ring alone on the page
  // row 3, middle: callouts that are not SKUs
  for (const [k, t] of ['1/2', 'FRONT/BACK', 'N/A', '3/4 in', '10/14'].entries()) labels.push({ text: t, x: col(1) - 20 + (k % 2) * 40, y: row(3) - 12 * (1 + (k >> 1)) });

  const stream = doc.context.flateStream(ops.join('\n') + '\n'); page.node.set(PDFName.of('Contents'), doc.context.obj([doc.context.register(stream)]));
  for (const l of labels) page.drawText(l.text, { x: l.x, y: l.y, size: 6, font, color: rgb(0.1, 0.1, 0.1) });
  const bytes = new Uint8Array(await doc.save({ useObjectStreams: false }));

  // --- parseSkuLabel: the slash is read as a hyphen; callouts stay callouts
  const ps = t => P.parseSkuLabel(t);
  assert.deepStrictEqual([ps('Masks Happy/Sad').sku, ps('Spider / Web_1').sku, ps('Cute Triceratops w/ hearts').sku], ['MASKS HAPPY-SAD', 'SPIDER - WEB_1', 'CUTE TRICERATOPS W- HEARTS'], 'a "/" is read as a hyphen');
  assert.strictEqual(ps('Masks Happy/Sad').slash, true, 'and says so');
  for (const t of ['1/2', '3/4 in', '10/14', 'N/A', 'FRONT/BACK', 'front / back', '/ABC', 'ABC/']) assert.strictEqual(ps(t), null, `"${t}" is a callout or malformed, not a SKU`);
  assert.deepStrictEqual(ps('Camp5 - Mountain Peak_1'), { sku: 'CAMP5 - MOUNTAIN PEAK_1', size: null }, 'a label without a slash reads exactly as before');

  // --- the page
  const parsed = await P.parseSource(bytes, 'label-slash-overlap.ai');
  const g = P.groupCharms(parsed, {});
  const lab = P.labelCharms(parsed, g.charms, { pattern: P.SKU_PATTERN_DEFAULT, gapPt: 18, widen: 0.25 });
  const at = ([cx, by]) => g.charms.find(c => c.mergedInto == null && Math.abs((c.outline.bbox[0] + c.outline.bbox[2]) / 2 - cx) < 12 && Math.abs(c.outline.bbox[1] - by) < 6);
  const skuOf = k => { const c = at(C[k]); return c ? c.sku || null : 'NO CHARM'; };

  assert.strictEqual(skuOf('masks'), 'MASKS HAPPY-SAD', 'slash label 1');
  assert.strictEqual(skuOf('spider'), 'SPIDER - WEB_1', 'slash label 2');
  assert.strictEqual(skuOf('trice'), 'CUTE TRICERATOPS W- HEARTS', 'slash label 3');
  assert(lab.labels.get(at(C.masks).index).slash, 'the label says it was read with a hyphen');
  assert.strictEqual(skuOf('cap'), 'GRAD CAP 1', 'a label 2 pt inside the charm\'s box is still under it');
  assert.strictEqual(skuOf('moon'), 'CRESCENT_50058', 'a label 1.3 pt inside the charm\'s box is still under it');
  assert.strictEqual(skuOf('deep'), null, 'a label 5 pt inside the charm\'s box is not under it');
  assert(lab.orphans.some(o => o.sku === 'BAT_1'), 'and is reported as a label with no charm above');
  assert.strictEqual(skuOf('bar'), 'TINY BAR_7427', 'a closed bar with one open run that retraces its top edge is a charm, and its label is under it');
  { const bar = at(C.bar); assert(bar.outline.closed && bar.outline.retraced === 1, 'the retracing run is set aside, the path is closed'); }
  assert.strictEqual(skuOf('hoop'), 'HIGH_36971', 'the charm with the three-circle hoop');
  assert.strictEqual(g.charms.filter(c => c.mergedInto == null && c.outline.bbox[2] - c.outline.bbox[0] < 8 && c.outline.bbox[3] - c.outline.bbox[1] < 8 && Math.abs((c.outline.bbox[0] + c.outline.bbox[2]) / 2 - C.hoop[0]) < 5).length, 0, 'the hoop touching the roof is no charm of its own');
  assert(at(C.hoop).members.some(m => m.bbox && m.bbox[2] - m.bbox[0] < 8), 'it is a member of its charm');
  assert.strictEqual(skuOf('twice'), 'HUMMINGBIRD CUT OUT_1', 'the outline written twice');
  assert.strictEqual(g.charms.filter(c => c.mergedInto == null && Math.abs((c.outline.bbox[0] + c.outline.bbox[2]) / 2 - C.twice[0]) < 12 && Math.abs(c.outline.bbox[1] - C.twice[1]) < 6).length, 1, 'is ONE charm');
  assert(lab.absorbed.some(a => a.why === 'same outline drawn twice'), 'and the report says so');
  assert.strictEqual(skuOf('far'), 'DANCING_81994', 'the charm with a ring 10 pt away');
  assert(lab.absorbed.some(a => a.why === 'loose hoop' && a.sku === 'DANCING_81994'), 'the ring 10 pt away joined it');
  { const c = at(C.far), r = P.integrateRings(c); assert.strictEqual(r.welded, 1, 'and integrateRings welds it (a neck to the body)'); assert.strictEqual(r.left.length, 0); }
  assert.strictEqual(lab.looseRings.length, 1, 'the ring alone on the page is reported, not dropped'); assert(lab.looseRings[0].bbox[0] > col(2) - 10 && /no charm/.test(lab.looseRings[0].why), lab.looseRings[0].why);
  assert(lab.unlabelled.includes(lab.looseRings[0].index), 'and stays as it was');
  // every charm that has a label under it, and nothing else, is unlabelled: only the alone ring and the too-deep Bat
  assert.deepStrictEqual(lab.unlabelled.map(i => g.charms[i]).map(c => c.outline.bbox[2] - c.outline.bbox[0] < 8 ? 'ring' : 'charm').sort(), ['charm', 'ring'], 'only the Bat charm and the lone ring stay unlabelled: ' + JSON.stringify(lab.unlabelled));
  // the callouts are no labels
  assert(!lab.orphans.some(o => /^(1\/2|FRONT|N\/A|3\/4|10\/14)/i.test(o.str)), 'callouts with a slash are not orphan labels');
  // the orders find these master SKUs from the SKU Etsy wrote (spacing and punctuation set aside, one master SKU only: charm-nest-orders.js masterSku)
  { const O = require(path.join(__dirname, '../../charm-nest-orders.js')), lib = ['MASKS HAPPY-SAD', 'SPIDER - WEB_1', 'CUTE TRICERATOPS W- HEARTS'], ix = new Map(lib.map(k => [O.looseKey(k), k]));
    const entry = k => (lib.includes(k) ? { sku: k } : null), loose = k => ix.get(O.looseKey(k)) || '';
    assert.deepStrictEqual(['Masks Happy/Sad', 'Spider / Web_1', 'Cute Triceratops w/ hearts'].map(k => O.masterSku(k, entry, loose)), lib, 'an order\'s own SKU reaches the hyphenated master SKU'); }
  console.log('label slash/overlap OK · slash read as hyphen, 1-3 pt overlap, retraced bar, 3-circle hoop, loose hoop joined and welded, lone ring reported, doubled outline');
})().catch(e => { console.error(e); process.exit(1); });
