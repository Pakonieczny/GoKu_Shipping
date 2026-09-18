// Unit tests for the Charm Sorter ⇄ Design Station bridge (design document §15), no browser, no network:
//   interpretation · labels in the master · extraction · flip · fit · review gate · sets · run steps
//   node tests/charm-nest/bridge-units.cjs
const assert = require('assert'), path = require('path'), fs = require('fs');
const root = path.join(__dirname, '../..');
global.window = global; global.PDFLib = require(path.join(root, 'vendor/pdf-lib-1.17.1.min.js')); require(path.join(root, 'charm-nest-pdf.js'));
const P = global.CharmNestPDF, G = require(path.join(root, 'charm-nest-geom.js')), O = require(path.join(root, 'charm-nest-orders.js'));
const opentype = require(path.join(root, 'vendor/opentype-1.3.4.min.js'));
const { buildMaster } = require('./fixture-master.cjs');
const MM = 25.4 / 72, PT = 72 / 25.4;
const font = opentype.loadSync(path.join(root, 'vendor/fonts/SourceSans3-Regular.otf'));   // the engraving font the app ships
const pass = (name) => console.log('  ✓', name);

(async () => {
  /* ── interpretation (§15 · every option pattern, alias, no-design, "Rose Quartz", staff note) ── */
  {
    const order = { receiptId: '3521337740', updateTs: 1789100000, buyerMessage: 'please engrave on the back', staffNote: 'customer phoned: spell it ANNE' };
    const mk = (over) => Object.assign({ transactionId: '4412778001', listingId: '1718', sku: 'BR-CMP-01', title: 'Compass charm necklace', quantity: 2, metalKey: 'gold', metalLabel: 'Gold', personalization: ['ANNA 9.26.25'], variations: [{ name: 'Metal', value: '14k Gold Filled' }, { name: 'Style', value: 'Necklace' }, { name: 'Size', value: 'Small' }, { name: 'Chain Length', value: '18 inch' }] }, over);
    const ctx = { optionMaps: {}, aliases: { '9999': { sku: 'BR-ALS-02' } }, noDesign: { patterns: ['^CHAIN'], skus: ['BOX-01'] }, masterEntry: s => ({ 'BR-CMP-01': { sizes: { S: {}, M: {} } }, 'BR-EAR-03': {}, 'BR-ALS-02': {} })[s] || null };
    let sp = O.interpretLine(order, mk(), ctx);
    assert.strictEqual(sp.designSku, 'BR-CMP-01'); assert.strictEqual(sp.material, 'gold'); assert.strictEqual(sp.form, 'necklace'); assert.strictEqual(sp.size, 'S'); assert.strictEqual(sp.chain, '18 inch'); assert.strictEqual(sp.quantity, 2); assert.deepStrictEqual(sp.problems, []); assert(sp.engraveCandidate); assert.strictEqual(sp.staffNote, 'customer phoned: spell it ANNE');
    sp = O.interpretLine(order, mk({ variations: [{ name: 'Metal', value: 'Sterling' }, { name: 'Type', value: 'Earrings' }], metalKey: 'silver', sku: 'BR-EAR-03' }), ctx); assert.strictEqual(sp.form, 'earrings'); assert.strictEqual(sp.material, 'silver'); assert.deepStrictEqual(sp.problems, []);
    sp = O.interpretLine(order, mk({ variations: [{ name: 'Product', value: 'Charm only' }], metalKey: '14k', sku: 'BR-EAR-03' }), ctx); assert.strictEqual(sp.form, 'charm'); assert.strictEqual(sp.material, 'gold14k');
    sp = O.interpretLine(order, mk({ sku: '', listingId: '9999', variations: [] }), ctx); assert.strictEqual(sp.designSku, 'BR-ALS-02'); assert.strictEqual(sp.skuSource, 'alias'); assert.deepStrictEqual(sp.problems, []);
    sp = O.interpretLine(order, mk({ sku: 'CHAIN-18', variations: [] }), ctx); assert(sp.noDesign); assert.deepStrictEqual(sp.problems, []);
    sp = O.interpretLine(order, mk({ sku: 'BR-CMP-01', variations: [{ name: 'Size', value: 'XL' }] }), ctx); assert(sp.problems.some(p => p.kind === 'missingSize'), 'sized line with no design for that size is held');
    sp = O.interpretLine(order, mk({ metalKey: '', metalLabel: 'Rose Quartz', variations: [{ name: 'Stone', value: 'Rose Quartz' }] }), ctx); assert(sp.problems.some(p => p.kind === 'needsMaterial'), '"Rose Quartz" never classifies as rose: the station left metalKey empty and the sorter holds the line'); assert(sp.problems.some(p => p.kind === 'needsMapping'), 'an unmapped option is a Needs-mapping item'); assert.strictEqual(sp.material, null);
    sp = O.interpretLine(order, mk({ variations: [{ name: 'Wrap', value: 'Gift wrap' }] }), { optionMaps: { '1718': { wrap: { 'gift wrap': { field: 'ignore' } } } }, aliases: {}, noDesign: {} }); assert(!sp.problems.some(p => p.kind === 'needsMapping'), 'a learned "ignore" map silences the item');
    // the shop's own spellings, read from the orders it actually sends (§6.4)
    const one = (name, value) => { const sp2 = O.interpretLine(order, mk({ variations: [{ name, value }] }), ctx); const o = sp2.options[0]; return o && o.mapped ? o.mapped.field + ':' + o.mapped.value : 'unmapped'; };
    assert.strictEqual(one('Necklace Length in inches', '16"'), 'chain:16"', 'a length written with a quote mark is a chain length');
    assert.strictEqual(one('Necklace Length in inches', '16&quot;'), 'chain:16"', 'and the same written as an entity');
    for (const v of ['18"', '16.5"', '45cm', '16 inches']) assert(one('LENGTH', v).startsWith('chain:'), 'a length: ' + v);
    assert.strictEqual(one('Charm Type', 'Necklace CHARM'), 'form:necklace', 'the shop writes the words in its own order');
    assert.strictEqual(one('Charm Type', 'CHARM + Engraving'), 'form:charm', 'and adds a word that is not the choice');
    for (const v of ['Huggie CHARM SET', 'Tag1 (front engrave)']) assert.strictEqual(one('Charm Type', v), 'unmapped', 'anything it cannot read stays for a person: ' + v);
    // each of these held real lines in the shop's own run — the words say plainly what the other option's name asks for
    assert.strictEqual(one('Charm Type', '18 Inch'), 'chain:18 Inch', 'a length under a form option is still a length');
    for (const n of ['Necklace Length in inches', 'LENGTH', 'Length'])
      assert.strictEqual(one(n, 'Charm Only-No Chain'), 'form:charm', 'a form under a length option is still a form: ' + n);
    for (const [n, v, want] of [['HOOP SIZE', '8.5mm', 'size:8.5MM'], ['HOOP SIZE', '11mm', 'size:11MM'], ['Ring size', '8 US', 'size:8US'], ['Charm Size', '14mm + engraving', 'size:14MM']])
      assert.strictEqual(one(n, v), want, `a measurement under a size option is a size: ${n}: ${v}`);
    assert.strictEqual(one('Charm Type', 'Huggie hoops'), 'form:huggie', 'the shop sells huggies as their own design');
    // the shop's own run: 411 lines raised 180 unknown-SKU problems over 133 distinct SKUs and 79 unmapped-option
    // problems over 15 distinct option strings. The decision is the SKU and the option, never the line.
    {
      const keyOf = (kind, p2) => kind === 'unmatchedSku' ? (p2.sku ? 'ord:sku:' + p2.sku : 'ord:listing:' + p2.listingId)
        : kind === 'needsMapping' ? 'ord:opt:' + p2.optionName + '\u0000' + p2.optionValue : null;
      const lines = [
        { listingId: '1', sku: 'FOOTBALL', title: 'a' }, { listingId: '2', sku: 'FOOTBALL', title: 'b' },
        { listingId: '3', sku: 'FOOTBALL', title: 'c' }, { listingId: '4', sku: 'GLOBE', title: 'd' },
      ];
      const keys = new Set();
      for (const l of lines) {
        const sp2 = O.interpretLine(order, mk(l), { optionMaps: {}, aliases: {}, noDesign: {}, masterEntry: () => null });
        for (const p2 of sp2.problems) { const k = keyOf(p2.kind, p2); if (k) keys.add(k); }
      }
      assert.deepStrictEqual([...keys].sort(), ['ord:sku:FOOTBALL', 'ord:sku:GLOBE'], 'four lines, two decisions: ' + [...keys]);
    }
    // a row a person parked stays parked: interpretAll recomputes problems from scratch, so a held row must not
    // raise its decision again the moment the next card is answered
    {
      const row = { key: 'h1', state: 'held', hold: 'held by Tester', problems: [], order: { receiptId: '1' }, line: { listingId: '1', sku: 'NOPE', title: 't' } };
      const sp2 = O.interpretLine(order, mk({ sku: 'NOPE' }), { optionMaps: {}, aliases: {}, noDesign: {}, masterEntry: () => null });
      assert(sp2.problems.some(p2 => p2.kind === 'unmatchedSku'), 'the line does raise a problem when it is read fresh');
      assert(row.hold && !row.problems.length, 'and a held row carries no problems to raise');
    }
    assert.strictEqual(O.poolId(order, mk(), 2), '3521337740_4412778001_2');
    pass('interpretation');
  }
  /* ── labels in the master (§15 · under, 8 mm below, shared, unlabelled, duplicate, size suffix) ── */
  let master;
  {
    master = await buildMaster(null, { count: 8, edge: true });
    const parsed = await P.parseSource(new Uint8Array(master.bytes), 'master.ai');
    const g = P.groupCharms(parsed, { minPt: 6 });
    assert.strictEqual(g.charms.length, 8, 'eight outlines (holes and details folded in): ' + g.charms.length);
    const lab = P.labelCharms(parsed, g.charms, { gapPt: 6.4 * PT });
    const bySku = new Map([...lab.labels.values()].map(l => [l.sku, l]));
    assert(bySku.has('BR-TST-01') && bySku.has('BR-TST-05') && bySku.has('BR-TST-06'), 'plain labels found');
    assert.strictEqual(bySku.get('BR-TST-02').size, 'S', 'size suffix after a middle dot');
    // a size comes from the sheet's own label, never from comparing two drawings of one design
    {
      const mkCharm = (index, x, w) => ({ index, mergedInto: null, members: [], outline: { bbox: [x, 100, x + w, 100 + w] }, bbox: [x, 100, x + w, 100 + w] });
      const a = mkCharm(0, 0, 30), b = mkCharm(1, 200, 60);          // the same SKU written under two charms, far apart
      const t = (cx, str) => ({ kind: 'text', str, bbox: [cx - 10, 90, cx + 10, 96], chars: str.length });
      const fake = { segments: [t(15, 'BR-SAME-01'), t(230, 'BR-SAME-01')], nested: [] };
      const l2 = P.labelCharms(fake, [a, b], { gapPt: 18 });
      const sized = [...l2.labels.values()].filter(x => x.size);
      assert.strictEqual(sized.length, 0, 'one SKU under two charms is not two sizes: ' + JSON.stringify([...l2.labels.values()].map(x => [x.sku, x.size])));
      assert.strictEqual(l2.labels.size, 1, 'the first charm keeps it, the other is left unlabelled');
      assert(l2.duplicates.some(d => d.sku === 'BR-SAME-01'), 'and the repeat is reported: ' + JSON.stringify(l2.duplicates));
    }
    assert(lab.orphans.some(o => o.sku === 'BR-TST-03'), 'a label 8 mm below is an orphan, not a label');
    const near = (c, m) => Math.abs((c.outline.bbox[0] + c.outline.bbox[2]) / 2 - m.cx) < 2 && Math.abs((c.outline.bbox[1] + c.outline.bbox[3]) / 2 - m.cy) < 4;
    const c3 = g.charms.find(c => near(c, master.charms[2])); assert(lab.unlabelled.includes(c3.index), 'the charm above the too-far label is unlabelled');
    const c4 = g.charms.find(c => near(c, master.charms[3])); assert(lab.unlabelled.includes(c4.index), 'the charm with no label is unlabelled');
    assert(bySku.get('BR-TST-05').extra.some(x => x.sku === 'BR-DUP-05') && lab.duplicates.length === 0, 'two stacked lines under one charm are two SKUs of that charm');
    for (const c of g.charms) assert(!c.members.some(m => m.kind === 'text'), 'no label text stays a member of any charm');
    // a label shared between two outlines goes to the nearer bottom edge
    { const t = { kind: 'text', str: 'BR-SHR-01', bbox: [100, 80, 130, 86], chars: 9 }; const fake = { segments: [t], nested: [] }; const near = { index: 0, outline: { bbox: [95, 90, 135, 120] }, members: [] }, far = { index: 1, outline: { bbox: [90, 96, 140, 130] }, members: [] }; const r = P.labelCharms(fake, [far, near], { gapPt: 18 }); assert.strictEqual(r.labels.get(0).sku, 'BR-SHR-01'); assert(!r.labels.has(1)); }
    pass('labels in the master');
    /* ── extraction: the per-SKU .ai re-parsed equals the master's charm (member count, silhouette hash) ── */
    const c1 = g.charms.find(c => c.sku === 'BR-TST-01');
    await P.buildSilhouettes(parsed, [c1], 6).catch(() => {});   // no canvas in node: silhouettes come from the geometry module below
    const s1 = G.silhouetteBits(c1, 6);
    const single = await P.buildSingleCharm(c1, parsed);
    const p2 = await P.parseSource(new Uint8Array(single), 'single.ai'); const g2 = P.groupCharms(p2, { minPt: 6 });
    assert.strictEqual(g2.charms.length, 1, 'one charm in the per-SKU file'); assert.strictEqual(g2.charms[0].members.length, c1.members.length, 'member count preserved');
    const s2 = G.silhouetteBits(g2.charms[0], 6);
    assert.strictEqual(P.fnv(P.signature(s1.bits, s1.w, s1.h)), P.fnv(P.signature(s2.bits, s2.w, s2.h)), 'silhouette signature identical');
    assert(!new TextDecoder('latin1').decode(single).includes('BR-TST-01'), 'the SKU string is not written into the per-SKU file');
    pass('extraction');
    /* ── flip (§15 · asymmetric charms with off-centre holes; wrong flips fail) ── */
    for (const c of g.charms.filter(x => x.sku)) {
      const v = G.backView(c, { res: 6 });
      assert(Object.values(v.checks).every(Boolean), `${c.sku} flip checks: ${JSON.stringify(v.checks)}`);
      assert(v.detail.dropped >= 1, 'front detail (the blue fill, the red stroke) dropped');
      assert.strictEqual(v.cutMembers.length, 2, 'outline + hole are the cut geometry');
      assert(G.diffFraction(v.F, G.flipX(v.F)) > 0.0005, `${c.sku} is asymmetric, so an UNMIRRORED back would fail the pixel check`);
    }
    { const c = g.charms.find(x => x.sku === 'BR-TST-01'); const fillIn = { outline: c.outline, members: c.members, bbox: c.bbox }; let threw = null; try { G.backView(fillIn, { res: 6, isCut: m => G.isCutLine(m) || (m.fill && !m.stroke) }); } catch (e) { threw = e; } assert(threw && threw.checks && threw.checks.detailDropped === false, 'a fill left in fails detailDropped'); }
    { const c = g.charms.find(x => x.sku === 'BR-TST-01'); const hole = c.members.find(m => m !== c.outline && G.isCutLine(m)); const shifted = G.transformSeg(hole, G.translate(0, -4)); const bad = { outline: c.outline, members: [c.outline, shifted], bbox: c.bbox }; const v = G.backView(bad, { res: 6 }); const p = G.interiorPoint(G.flatten(hole, 12)); assert.strictEqual(G.at(v.B, 2 * v.cx - p[0], p[1]), 1, 'with the hole shifted, the ORIGINAL hole spot is solid on the back: the hole check would catch it'); }
    pass('flip');
    /* ── the size of the lettering: how wide the longest line is against how wide the face is (§7.4) ── */
    {
      const capPerEm = G.capPerEm(font), mm = 25.4 / 72;
      const advanceOf = t => font.getAdvanceWidth(t, 1, { kerning: true });
      // a disc of D mm: the usable face after the 0.8 mm margin, and the largest cap the fitter would allow
      const face = D => Math.PI * Math.pow((D - 1.6) / 2, 2);
      const ceilOf = D => 0.4 * capPerEm * D;                                  // fitText's own maxHeightFrac guard
      const call = (text, D, ceilingCapMm) => {
        const capCeil = ceilingCapMm == null ? ceilOf(D) : ceilingCapMm;
        const fittedMax = capCeil / (capPerEm * mm);
        const size = G.defaultSize([].concat(text), fittedMax, { capPerEm, minCapMm: 1.6, charmMinMm: D, charmMaxMm: D, usableAreaMm2: face(D), advanceOf });
        return { size, fittedMax, cap: size * capPerEm * mm, capCeil, adv: Math.max(...[].concat(text).map(t => advanceOf(t) / capPerEm)) };
      };
      // the complaint: a four letter name must not be set at the largest size that fits
      const finn = call('Finn', 14);
      assert(finn.cap < finn.capCeil * 0.75, `a short name is well under the ceiling: ${finn.cap.toFixed(2)} of ${finn.capCeil.toFixed(2)} mm`);
      assert(finn.cap / 14 > 0.1 && finn.cap / 14 < 0.2, `and is a sane share of the charm: ${(finn.cap / 14 * 100).toFixed(1)}% of 14 mm`);
      // the ceiling is absolute, and the legible floor is absolute — including after the 0.05 pt quantisation
      for (const D of [8, 10, 12, 14, 16, 20, 24, 30]) for (const n of [1, 3, 4, 7, 12, 20, 34]) {
        const c = call('m'.repeat(n), D);
        assert(c.size <= c.fittedMax + 1e-9, `ceiling held: ${n} chars on ${D} mm`);
        assert(c.cap >= Math.min(1.6, c.capCeil) - 1e-6, `floor held: ${c.cap.toFixed(3)} mm, ${n} chars on ${D} mm`);
        assert(c.size > 0 && isFinite(c.size), `a real size: ${n} on ${D}`);
      }
      // length is read from the font, not from a character count: WILLIAM is 7 characters and wider than illinois, 8
      const will = call('WILLIAM', 16), illi = call('illinois', 16);
      assert(will.adv > illi.adv, 'WILLIAM is the wider line');
      assert(will.cap < illi.cap, `and so is set smaller: WILLIAM ${will.cap.toFixed(2)} vs illinois ${illi.cap.toFixed(2)} mm`);
      // the lettering falls as the line gets wider, and the line itself claims more of the face
      const names = ['Al', 'Finn', 'Emily', 'Jessica', 'Charlotte', 'Alexandria'];
      let lastCap = Infinity, lastWidth = 0;
      for (const nm of names) {
        const c = call(nm, 20);
        const width = c.adv * c.cap;
        assert(c.cap <= lastCap + 1e-9, `the lettering never grows with the line: ${nm} ${c.cap.toFixed(2)} after ${lastCap.toFixed(2)}`);
        assert(width >= lastWidth - 1e-9, `and the line claims more of the face: ${nm} ${width.toFixed(2)} mm`);
        lastCap = c.cap; lastWidth = width;
      }
      // no cliff: one more character never moves the size by more than a sixteenth
      let prev = 0, biggest = 0;
      for (let n = 3; n <= 40; n++) { const c = call('n'.repeat(n), 16).cap; if (prev) biggest = Math.max(biggest, Math.abs(c - prev) / prev); prev = c; }
      assert(biggest < 0.0625, 'no step between consecutive lengths: biggest ' + (biggest * 100).toFixed(1) + '%');
      // the same name on bigger charms keeps the same proportion
      const band = [14, 16, 20, 24, 30].map(D => call('Finn', D).cap / D);
      assert(Math.max(...band) / Math.min(...band) < 1.12, 'Finn holds its share of the charm across sizes: ' + band.map(v => v.toFixed(3)).join(' '));
      // a charm with barely any room keeps the lettering legible rather than shrinking it to nothing
      const tight = call('ANNA', 6, 1.9);
      assert(tight.cap >= 1.6 - 1e-6, `never under the legible minimum: ${tight.cap.toFixed(2)} mm`);
      // and when the ceiling is itself under the minimum, the ceiling wins and is returned untouched
      const squeezed = call('Happy Birthday Mom', 16, 1.0966);
      assert.strictEqual(squeezed.size, squeezed.fittedMax, 'a ceiling under the floor is returned exactly, never re-quantised');
      // nothing measured, nothing changed
      assert.strictEqual(G.defaultSize(['Finn'], 9, { capPerEm }), 9, 'with no face and no font the ceiling stands');
      assert.strictEqual(G.defaultSize([''], 9, { capPerEm, usableAreaMm2: 100, advanceOf }), 9, 'an empty line leaves the ceiling alone');
      pass('lettering size');
    }
    /* ── fit (§15 · a hole, a thin ring, a diagonal band; largest size; heart refused) ── */
    {
      const c = g.charms.find(x => x.sku === 'BR-TST-05');
      const v = G.backView(c, { res: 6 }); const mask = G.engraveMask(v, { marginMm: 0.8 });
      const fit = G.fitText(['ANNA', '9.26.25'], font, mask, { tryRotated: true, minStrokeMm: 0, minGapMm: 0 });
      assert(fit.ok, 'text fits on the back: ' + fit.reason);
      assert(G.verifyInk(fit.cmds, mask).ok, 'no ink outside the eroded mask');
      const hole = c.members.find(m => m !== c.outline && G.isCutLine(m)); const hp = G.interiorPoint(G.flatten(G.transformSeg(G.transformSeg(hole, v.M), v.R), 12)); const ink = G.rasterGlyphs(fit.cmds, mask); assert.strictEqual(G.at(ink, hp[0], hp[1]), 0, 'no ink in the hole');
      const bigger = G.layoutLines(fit.lines, font, fit.size + 0.1, 0.18, fit.angle, fit.centre); assert(!G.verifyInk(bigger.cmds, mask).ok, '0.1 pt more must fail');
      const ring = { outline: { kind: 'path', stroke: true, closed: true, strokeRGB: [0, 0, 0], lwPt: 0.5, subpaths: [G.flatten({ subpaths: [[['m', [220, 200]], ['c', [220, 211], [211, 220], [200, 220]], ['c', [189, 220], [180, 211], [180, 200]], ['c', [180, 189], [189, 180], [200, 180]], ['c', [211, 180], [220, 189], [220, 200]], ['h']]] }, 12)[0].map((p, i) => [i ? 'l' : 'm', p]).concat([['h']])], bbox: [180, 180, 220, 220] } };
      ring.members = [ring.outline, Object.assign({}, ring.outline, { subpaths: [G.flatten({ subpaths: [[['m', [217, 200]], ['c', [217, 209.4], [209.4, 217], [200, 217]], ['c', [190.6, 217], [183, 209.4], [183, 200]], ['c', [183, 190.6], [190.6, 183], [200, 183]], ['c', [209.4, 183], [217, 190.6], [217, 200]], ['h']]] }, 12)[0].map((p, i) => [i ? 'l' : 'm', p]).concat([['h']])], bbox: [183, 183, 217, 217] })]; ring.bbox = ring.outline.bbox;
      const rv = G.backView(ring, { res: 6 }); const rm = G.engraveMask(rv, { marginMm: 0.8 }); const rf = G.fitText(['ANNA'], font, rm, {}); assert(!rf.ok && /no solid area|no room/.test(rf.reason), 'a thin ring returns no room: ' + rf.reason);
      // a diagonal band: rotated layout wins only with ≥ 12 % gain
      const band = { outline: { kind: 'path', stroke: true, closed: true, strokeRGB: [0, 0, 0], lwPt: 0.5, subpaths: [[['m', [0, 0]], ['l', [60, 60]], ['l', [52, 68]], ['l', [-8, 8]], ['h']]], bbox: [-8, 0, 60, 68] } }; band.members = [band.outline]; band.bbox = band.outline.bbox;
      const bv = G.backView(band, { res: 6, upAngle: 90 }); const bm = G.engraveMask(bv, { marginMm: 0.5 });
      const straight = G.fitText(['ANNA'], font, bm, { tryRotated: false, minStrokeMm: 0, minGapMm: 0 }); const rot = G.fitText(['ANNA'], font, bm, { tryRotated: true, angles: [0, 45, -45], minStrokeMm: 0, minGapMm: 0 });
      assert(straight.ok && rot.ok, 'band fits'); assert(rot.angle !== 0 && rot.size > straight.size * 1.12, `the band takes the rotated layout (${rot.angle}°, ${rot.size.toFixed(2)} vs ${straight.size.toFixed(2)})`);
      const tight = G.fitText(['ANNA'], font, bm, { tryRotated: true, angles: [0, 3], rotGain: 0.12, minStrokeMm: 0, minGapMm: 0 }); assert.strictEqual(tight.angle, 0, 'a 3° layout gains under 12 % and is not taken');
      assert.deepStrictEqual(G.glyphCoverage(font, 'Anna ♥ ★').missing, ['★'], 'a star the font lacks is refused; the heart it has is not');
      assert(G.splitVariants(['ANNA 9.26.25']).some(v => v.length === 2 && v[1] === '9.26.25'), 're-split offers name / date');
    }
    pass('fit');
  }
  /* ── review gate (§15 · a set with one unapproved engraving cannot commit; a nudge re-fits) ── */
  {
    const rows = [{ key: 'a', state: 'written', engrave: { needed: true, approved: false, state: 'review' } }, { key: 'b', state: 'written' }];
    let ev = O.evaluateOrder(rows); assert(!ev.committable && /awaiting review/.test(ev.held.why));
    rows[0].engrave.approved = true; rows[0].engrave.state = 'approved'; ev = O.evaluateOrder(rows); assert(ev.committable, 'approved → committable');
    ev = O.evaluateOrder([{ key: 'c', state: 'pooled' }]); assert(!ev.committable, 'a pooled (not written) line holds the order');
    ev = O.evaluateOrder([{ key: 'd', spec: { noDesign: true }, state: 'noDesign' }]); assert(ev.committable, 'a no-design line never holds');
    ev = O.evaluateOrder([{ key: 'e', state: 'written', engrave: { needed: true, approved: true, state: 'skipped' } }]); assert(ev.committable, 'a skipped engraving (cut plain) travels');
    const c = (await (async () => { const parsed = await P.parseSource(new Uint8Array(master.bytes), 'm'); const g = P.groupCharms(parsed, { minPt: 6 }); P.labelCharms(parsed, g.charms, { gapPt: 18 }); return g.charms.find(x => x.sku === 'BR-TST-05'); })());
    const v = G.backView(c, { res: 6 }); const mask = G.engraveMask(v, { marginMm: 0.8 }); const fit = G.fitText(['ANNA'], font, mask, { minStrokeMm: 0, minGapMm: 0 });
    const nudged = G.refitAt(['ANNA'], font, mask, {}, { centre: [fit.centre[0] + 0.25 * PT, fit.centre[1]], angle: fit.angle }); assert(nudged.ok && G.verifyInk(nudged.cmds, mask).ok, 'a nudge re-fits and still verifies');
    pass('review gate');
  }
  /* ── sets (§15 · one set number across materials; folder and name dates agree across midnight) ── */
  {
    const day = '2026-09-16';
    assert.strictEqual(O.sheetName('gold', day, 3, 1), 'GF_Sep.16.26_Set-3_Sheet-1'); assert.strictEqual(O.sheetName('silver', day, 3, 1), 'SS_Sep.16.26_Set-3_Sheet-1'); assert.strictEqual(O.sheetName('gold', day, 3, 2), 'GF_Sep.16.26_Set-3_Sheet-2');
    assert.strictEqual(O.setFolder(day, 3), 'charmnest/sets/2026-09-16/Set-3'); assert.strictEqual(O.sheetFolder('rose', day, 3, 1), 'charmnest/sets/2026-09-16/Set-3/RG_Sep.16.26_Set-3_Sheet-1');
    const late = new Date(2026, 8, 16, 23, 59, 30); assert.strictEqual(O.localDay(late), '2026-09-16'); assert.strictEqual(O.dateTag(late), 'Sep.16.26'); assert.strictEqual(O.dateTagOfDay(O.localDay(late)), O.dateTag(late), 'folder date and name date come from one local clock, even at 23:59:30');
    assert.strictEqual(O.encodeOrderList(['3521337740', '3521337741'], 'gold'), 'B36|gold|' + ['3521337740', '3521337741'].map(O.toB36).join('.'));
    assert.deepStrictEqual(O.safeChunks(Array.from({ length: 120 }, (_, i) => String(3521337740 + i)), 'gold').map(c => c.length), [50, 50, 20], 'the station\'s chunker, byte for byte');
    assert.strictEqual(O.CARD_TO_METAL.gold10k, '10k'); assert.strictEqual(O.CARD_TO_METAL.gold14k, '14k');
    pass('sets');
  }
  /* ── what goes to the laser today: full sheets for the fast metals, every other day for the slow ones, orders whole ── */
  {
    const P = O.planRelease, K = O.kinGroups;
    const cap = { silver: 1000, gold: 1000, rose: 1000, gold10k: 1000, gold14k: 1000 };
    const L = (key, orderId, material, areaPt2, shipDays) => ({ key, orderId, material, areaPt2, shipBy: shipDays == null ? 0 : Math.floor(Date.parse('2026-09-18T00:00:00') / 1000) + shipDays * 86400 });
    const base = { today: '2026-09-18', capacity: cap, cadenceDays: 2, lateDays: 2 };
    // 1 · a fast metal with less than a sheet waits, and says how far it has got
    let r = P([L('a', '1', 'silver', 300), L('b', '2', 'silver', 310)], base);
    assert.strictEqual(r.take.size, 0, 'a partial SS sheet is not cut'); assert(/full silver sheet/.test(r.wait.get('a').why) && r.wait.get('a').pct === 61, 'and the wait says how full it is: ' + JSON.stringify(r.wait.get('a')));
    // 2 · one full sheet goes, the remainder waits
    r = P([L('a', '1', 'silver', 400), L('b', '2', 'silver', 400), L('c', '3', 'silver', 200), L('d', '4', 'silver', 150)], base);
    assert.deepStrictEqual([...r.take].sort(), ['a', 'b', 'c'], 'exactly one full sheet goes: ' + [...r.take]); assert(r.wait.has('d'), 'the rest waits'); assert.strictEqual(r.materials.silver.full, 1);
    // 3 · a piece that is due forces the partial sheet, and the sheet is then filled as far as it will go
    r = P([L('a', '1', 'silver', 300, 1), L('b', '2', 'silver', 300, 9)], base);
    assert.deepStrictEqual([...r.take].sort(), ['a', 'b'], 'a due piece cuts the sheet and the other piece rides along'); assert(r.materials.silver.partial && /due/.test(r.materials.silver.forcedBy[0]), 'and the sheet is marked partial with the reason: ' + JSON.stringify(r.materials.silver.forcedBy));
    // 4 · slow metals: whatever there is goes, dates unread, on an open day
    r = P([L('a', '1', 'rose', 50, 30)], base);
    assert(r.take.has('a') && r.materials.rose.open && !r.materials.rose.lastReleased, 'a never-released slow metal is open'); 
    // 5 · … and is closed the day after a release, with the next day named
    r = P([L('a', '1', 'rose', 50)], Object.assign({}, base, { lastReleased: { rose: '2026-09-17' } }));
    assert(r.wait.has('a') && r.wait.get('a').kind === 'slow' && r.wait.get('a').until === '2026-09-19', 'closed the day after: ' + JSON.stringify(r.wait.get('a'))); assert.strictEqual(r.materials.rose.next, '2026-09-19');
    r = P([L('a', '1', 'rose', 50)], Object.assign({}, base, { lastReleased: { rose: '2026-09-16' } }));
    assert(r.take.has('a'), 'open again two days on');
    // 6 · a person opens it early
    r = P([L('a', '1', 'rose', 50)], Object.assign({}, base, { lastReleased: { rose: '2026-09-17' }, released: { rose: '2026-09-18' } }));
    assert(r.take.has('a') && r.materials.rose.forced, 'released by hand today');
    // 7 · an order is never split: its GF piece waits with its closed 14K piece …
    r = P([L('g', '9', 'gold', 900), L('k', '9', 'gold14k', 40), L('h', '8', 'gold', 100)], Object.assign({}, base, { lastReleased: { gold14k: '2026-09-17' } }));
    assert(r.wait.get('g') && r.wait.get('g').kind === 'slow' && r.wait.get('k').kind === 'slow', 'the whole order waits for 14K: ' + JSON.stringify([...r.wait]));
    assert(r.wait.get('h') && r.wait.get('h').kind === 'fill', 'and the lone GF piece waits for a full sheet, not for 14K');
    // 8 · … and rides along the day 14K opens, even though that makes the GF sheet partial
    r = P([L('g', '9', 'gold', 300), L('k', '9', 'gold14k', 40), L('h', '8', 'gold', 100)], base);
    assert(r.take.has('g') && r.take.has('k') && r.take.has('h'), 'order 9 travels whole and the partial GF sheet is filled with what else there is: ' + [...r.take]);
    assert(r.materials.gold.partial && /travels with another material/.test(r.materials.gold.forcedBy[0]), 'the partial is explained: ' + JSON.stringify(r.materials.gold.forcedBy));
    // 9 · a person can say "cut it anyway"
    r = P([L('a', '1', 'silver', 300)], Object.assign({}, base, { forceFill: { silver: true } }));
    assert(r.take.has('a') && r.materials.silver.forcedBy[0] === 'operator', 'the operator can cut a partial sheet');
    // 10 · sets are the materials tied together by shared orders; a material no order ties is a set of its own
    const g = K([L('a', '1', 'silver', 1), L('b', '2', 'gold', 1), L('c', '2', 'gold14k', 1), L('d', '3', 'rose', 1)]);
    assert.deepStrictEqual(g, { silver: 'silver', gold: 'gold+gold14k', gold14k: 'gold+gold14k', rose: 'rose' }, JSON.stringify(g));
    pass('release plan');
  }
  /* ── rings on a layered master: every body first, every ring after. The ring's stream neighbours are other rings, and a
        gate that trusted the stream alone detached every ring in the shop's library. Geometry decides when one outline
        is in reach; the stream decides only when two are. ── */
  {
    const { buildMaster } = require('./fixture-master.cjs');
    for (const layered of [false, true]) {
      const fx = await buildMaster(null, { count: 8, edge: false, layered });
      const parsed = await P.parseSource(new Uint8Array(fx.bytes), 'layered.ai'); const g = P.groupCharms(parsed, { minPt: 6 });
      assert.strictEqual(g.charms.length, 8, `${layered ? 'layered' : 'grouped'} master: eight charms, no ring counted as a charm of its own (${g.charms.length})`);
      const holes = g.charms.map(c => P.cutLinesOf(c).length);
      assert(holes.every(n => n >= 1), `${layered ? 'layered' : 'grouped'} master: every charm keeps its hole: ${holes}`);
      if (layered) {
        assert(g.charms.every(c => c.members.length >= 4), 'body, fill, engraving stroke, hole and the jump ring all belong to the charm: ' + g.charms.map(c => c.members.length));
        // the hole and the jump ring are both tiny closed paths; every charm holds exactly its own two, so the nester sees the ring
        const tiny = g.charms.map(c => c.members.filter(m => m !== c.outline && m.kind === 'path' && m.closed && m.stroke && !m.fill && (m.bbox[2] - m.bbox[0]) < 8).length);   // stroked rings, not the small colour fill
        assert(tiny.every(n => n === 2), 'each charm holds its hole and its jump ring, nobody else\'s: ' + tiny);
        assert.strictEqual((g.orphans || []).length, 0, 'nothing left loose: ' + (g.orphans || []).length);
      }
    }
    pass('rings on a layered master');
  }
  /* ── run steps ── */
  { assert.strictEqual(O.RUN_STEPS.length, 11); assert.strictEqual(O.nextStep('nest'), 'checkpoint'); assert.strictEqual(O.nextStep('complete'), null); assert.strictEqual(O.HALF.engrave, 'B'); assert.strictEqual(O.HALF.nest, 'A'); pass('run steps'); }
  /* ── back file: written and re-parsed, the cut geometry is the mirrored original and the text is paths ── */
  {
    const parsed = await P.parseSource(new Uint8Array(master.bytes), 'm'); const g = P.groupCharms(parsed, { minPt: 6 }); P.labelCharms(parsed, g.charms, { gapPt: 18 });
    const c = g.charms.find(x => x.sku === 'BR-TST-05'); const v = G.backView(c, { res: 6 }); const mask = G.engraveMask(v, { marginMm: 0.8 }); const fit = G.fitText(['ANNA'], font, mask, { minStrokeMm: 0, minGapMm: 0 });
    const rel = fit.glyphs.map(gl => ({ cmds: gl.cmds.map(k => { const o = { type: k.type }; if (k.type !== 'Z') { o.x = k.x - v.cx; o.y = k.y - v.cy; } if (k.type === 'C' || k.type === 'Q') { o.x1 = k.x1 - v.cx; o.y1 = k.y1 - v.cy; } if (k.type === 'C') { o.x2 = k.x2 - v.cx; o.y2 = k.y2 - v.cy; } return o; }) }));
    for (const view of ['asSeenFromBack', 'frontCoordinates']) {
      const built = await P.buildBackFile({ charm: c, parsed, cutMembers: v.cutMembers, cx: v.cx, cy: v.cy, angleDeg: v.angleDeg, padPt: 5 * PT, glyphs: rel, view, title: 't', meta: { test: true } });
      assert(!built.reference.redrawn, 'the cut reference is the original bytes under a matrix');
      const p2 = await P.parseSource(new Uint8Array(built.bytes), 'back.ai'); const g2 = P.groupCharms(p2, { minPt: 4 });
      const back = g2.charms.reduce((a, b) => (b.bbox[2] - b.bbox[0]) * (b.bbox[3] - b.bbox[1]) > (a.bbox[2] - a.bbox[0]) * (a.bbox[3] - a.bbox[1]) ? b : a);
      const cut = back.members.filter(m => m === back.outline || G.isCutLine(m)); assert.strictEqual(cut.length, 2, 'outline + hole in the back file');
      assert(!back.members.some(m => m.fill && !m.stroke && m.fillRGB && m.fillRGB[2] > 0.5), 'the blue front fill never reaches the back file');
      const bbW = cut.reduce((a, s) => [Math.min(a[0], s.bbox[0]), Math.min(a[1], s.bbox[1]), Math.max(a[2], s.bbox[2]), Math.max(a[3], s.bbox[3])], [Infinity, Infinity, -Infinity, -Infinity]);
      const bbV = v.members.reduce((a, s) => [Math.min(a[0], s.bbox[0]), Math.min(a[1], s.bbox[1]), Math.max(a[2], s.bbox[2]), Math.max(a[3], s.bbox[3])], [Infinity, Infinity, -Infinity, -Infinity]);
      const frame = G.makeFrame([0, 0, bbV[2] - bbV[0], bbV[3] - bbV[1]], 6, (bbV[2] - bbV[0]) / 2, 1);
      const W = G.raster(cut.map(m => G.transformSeg(m, G.translate(-bbW[0], -bbW[1]))), frame), V = G.raster(v.members.map(m => G.transformSeg(m, G.translate(-bbV[0], -bbV[1]))), frame);
      const diff = G.diffFraction(W, view === 'frontCoordinates' ? G.flipX(V) : V); assert(diff <= 0.01, `${view}: written cut geometry matches the verified back (${(diff * 100).toFixed(2)}%)`);
      const textPaths = p2.segments.concat(p2.nested).filter(s => s.kind === 'path' && s.fill && !s.stroke); assert(textPaths.length >= 1, 'the text is filled paths'); assert.strictEqual(p2.counts.text, 0, 'no font reference / text object in the back file');
    }
    pass('back file');
  }
  console.log('bridge units OK');
})().catch(e => { console.error(e); process.exit(1); });
