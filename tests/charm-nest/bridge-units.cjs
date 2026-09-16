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
const font = opentype.loadSync(path.join(root, 'netlify/functions/fonts/OpenSans-Regular.ttf'));   // stand-in for Myriad Pro in the tests
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
    assert(lab.orphans.some(o => o.sku === 'BR-TST-03'), 'a label 8 mm below is an orphan, not a label');
    const near = (c, m) => Math.abs((c.outline.bbox[0] + c.outline.bbox[2]) / 2 - m.cx) < 2 && Math.abs((c.outline.bbox[1] + c.outline.bbox[3]) / 2 - m.cy) < 4;
    const c3 = g.charms.find(c => near(c, master.charms[2])); assert(lab.unlabelled.includes(c3.index), 'the charm above the too-far label is unlabelled');
    const c4 = g.charms.find(c => near(c, master.charms[3])); assert(lab.unlabelled.includes(c4.index), 'the charm with no label is unlabelled');
    assert(lab.duplicates.some(d => d.sku === 'BR-DUP-05' || d.also === 'BR-DUP-05'), 'two labels under one charm are reported as a duplicate');
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
      assert.deepStrictEqual(G.glyphCoverage(font, 'Anna ♥').missing, ['♥'], 'a heart the font lacks is refused');
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
