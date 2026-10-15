// SIZEFIT (Paul, 15 Oct 2026): "The system needs ... a new check that checks the size of the charm chosen by the user and automatically adapts the charm size from the repo to match the buyer chosen size/diameter."
// The page side, on the REAL drawing of a round disc (a 12 mm disc with a hoop: discAi() of the real-drawing world, master design BFLOWER_9901): orders that choose 10 mm, 12 mm, 14 mm ("+ engraving") and an
// impossible 40 mm go through the real intake and the real pool; the charm each piece gets must be the repo's disc DRAWN AT THE SIZE CHOSEN, the repo's own record must stay as it is, a record kept from a saved sheet
// must be drawn the same way again, and nothing may be held that should not be.
//   node tests/charm-nest/size-fit-pool.cjs      (browser: run through /tmp/cn-run.sh 240)
'use strict';
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const R = require('./_engraveRealWorld.cjs'), Base = require('./_orderFactsWorld.cjs');
let n = 0; const ok = (c, why) => { assert(c, why); n++; };
const near = (a, b, tol, why) => ok(Math.abs(a - b) <= tol, `${why}: ${a} vs ${b}`);
const V = (name, value) => ({ name, value });
const mk = (rid, size, name) => ({ rid, sku: 'BFLOWER_9901', listing: '18' + rid.slice(-6), title: 'Birthflower Charm Only Disc Necklace', buyer: 'Name One', placed: { day: 0, h: 10, m: 10 }, ship: 3, variations: [V(name || 'Size of Necklace Charm', size)], personalization: [] });
const CASES = { s10: mk('4291000001', '10mm + engraving'), s12: mk('4291000002', '12mm'), s14: mk('4291000003', '14mm'), s40: mk('4291000004', '40mm'), n16: mk('4291000005', '16mm', 'Size of the Necklace Charm (NO Huggie option)') };
(async () => {
  const w = await R.world({ viewport: { width: 1440, height: 900 } });
  if (!w) { console.log('size fit pool: no playwright-core here, skipped'); return; }
  const page = w.page;
  try {
    // the record the real indexer writes for this drawing: the OUTER box (the 34 pt disc and its margin, the jump ring on top) and the silhouette's area
    { const st = w.srv.st, cur = st.list('Charm_Master_Index').find(e => e.sku === 'BFLOWER_9901'); st.put('Charm_Master_Index', 'BFLOWER_9901', Object.assign({}, cur, { widthPt: 37, heightPt: 42.2, areaPt2: 973.8 })); await page.evaluate(() => Master.load(true)); }
    const out = await page.evaluate(async ({ cases, orderSrc }) => {
      const orderOf = eval('(' + orderSrc + ')'), now = Date.now(), res = { rows: {} };
      await Orders.loadMaps(true);
      const base = Master.entryFor('BFLOWER_9901'); res.baseBefore = JSON.stringify({ aiPath: base.aiPath, widthPt: base.widthPt, heightPt: base.heightPt, charmHash: base.charmHash, sizes: base.sizes || null });
      for (const [name, fx] of Object.entries(cases)) {
        const order = orderOf(fx, now);
        for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); row.__name = name; }
      }
      Orders.interpretAll();
      await Pool.addAll({ runId: 'run-sizefit', lines: {} });
      for (const row of Orders.rows()) {
        if (!row.__name) continue;
        const sp = row.spec || {}, pr = (row.poolIds || []).map(id => B.pool.rows.get(id)).filter(Boolean)[0] || null, src = pr && pr.aiPath ? B.pool.sources.get(pr.aiPath) : null, c = src && src.charms && src.charms[0];
        res.rows[row.__name] = { size: sp.size, fit: sp.sizeFit ? { action: sp.sizeFit.action, scale: sp.sizeFit.scale || null, haveMm: sp.sizeFit.haveMm || null } : null, problems: (row.problems || []).map(p => p.kind), state: row.state, reason: row.reason || null, pooled: !!pr,
          poolSize: pr && pr.size, poolPath: pr && pr.aiPath, wMm: c ? (c.outline.bbox[2] - c.outline.bbox[0]) * 25.4 / 72 : null, hMm: c ? (c.outline.bbox[3] - c.outline.bbox[1]) * 25.4 / 72 : null, holes: c ? CharmNestPDF.cutLinesOf(c).length : null, hash: pr && pr.charmHash, srcName: src && src.name, srcCloudPath: src && src.cloud && src.cloud.path };
      }
      // the sources one per size drawn: 10 mm, 12 mm (as the repo draws it), 14 mm
      res.sources = [...B.pool.sources.keys()].filter(k => /BFLOWER/.test(k));
      const e = Master.entryFor('BFLOWER_9901');
      res.baseAfter = JSON.stringify({ aiPath: e.aiPath, widthPt: e.widthPt, heightPt: e.heightPt, charmHash: e.charmHash, sizes: e.sizes || null });
      // the page's two lookups of the design for a size
      const g10 = Pool.sizeEntry(e, '10MM'), g12 = Pool.sizeEntry(e, '12MM'), g14 = Pool.sizeEntry(e, '14MM'), g40 = Pool.sizeEntry(e, '40MM'), gM = Pool.sizeEntry(e, 'M'), gN = Pool.sizeEntry(e, null);
      res.g = { g10: g10 && { path: g10.aiPath, scaled: !!g10.scaled, w: g10.widthPt, h: g10.heightPt, a: g10.areaPt2, base: g10.base === e }, g12: g12 === e, g14: g14 === e, g40: g40 === e, gM: gM === e, gN: gN === e, again: Pool.sizeEntry(e, '10MM') === g10 };
      res.key = { scaled: Pool.sizeKeyOf(e, g10.aiPath), plain: Pool.sizeKeyOf(e, e.aiPath) };
      // a record kept from a saved sheet is drawn the same way again (the Library's "edit this back" route reads it as a made-up entry: its path and the file's link)
      const rec = await Pool.masterCharm({ sku: 'BFLOWER_9901', aiPath: g10.aiPath, aiUrl: e.aiUrl, upAngle: 90 }, null), rc = rec.charms[0];
      res.restored = { wMm: (rc.outline.bbox[2] - rc.outline.bbox[0]) * 25.4 / 72, holes: CharmNestPDF.cutLinesOf(rc).length };
      // a design that holds sizes (12 and 14 mm drawn) and is asked for 10 mm: scaled from the nearest, 13 mm: from a neighbour, the sizes it holds: as they are
      const sized = Object.assign({}, e, { aiPath: null, sizes: { '12MM': Object.assign({}, e, { aiPath: e.aiPath }), '14MM': Object.assign({}, e, { widthPt: 14 * 72 / 25.4, heightPt: 14 * 72 / 25.4 * 1.153, areaPt2: Math.PI * Math.pow(14 * 72 / 25.4, 2) / 4 * 1.03, aiPath: e.aiPath }) } });
      const s10 = Pool.sizeEntry(sized, '10MM'), s12 = Pool.sizeEntry(sized, '12MM'), s11 = Pool.sizeEntry(sized, '11MM'), s40 = Pool.sizeEntry(sized, '40MM');
      res.sized = { s10: s10 && { scaled: !!s10.scaled, scale: s10.scale }, s12: s12 === sized.sizes['12MM'], s11: s11 && { scaled: !!s11.scaled, scale: s11.scale }, s40: s40 };
      res.errors = (window.__errs || []).slice();
      return res;
    }, { cases: CASES, orderSrc: Base.orderOf.toString() });

    const r = out.rows, MM = 25.4 / 72;
    // the intake: the size the buyer chose is read from any wording of the option, and the check is part of the line
    ok(r.s10.size === '10MM' && r.s10.fit.action === 'scale' && Math.abs(r.s10.fit.scale - 10 / ((37 - 2.6) * MM)) < 1e-3, 'the 10 mm line carries the index estimate of the scale (10 / 12.14): ' + JSON.stringify(r.s10.fit));
    ok(r.s12.size === '12MM' && r.s12.fit.action === 'same', 'the 12 mm line is the repo disc as it is (same): ' + JSON.stringify(r.s12.fit));
    ok(r.s14.size === '14MM' && r.s14.fit.action === 'scale' && r.s14.fit.scale > 1.15 && r.s14.fit.scale < 1.16, '14 mm is scaled up: ' + JSON.stringify(r.s14.fit));
    ok(r.n16.size === '16MM' && r.n16.fit.action === 'scale', 'an option named "Size of the Necklace Charm (NO Huggie option)" is the size too');
    ok(r.s40.fit.action === 'ask' && r.s40.problems.includes('missingSize') && !r.s40.pooled, 'an impossible 40 mm waits for a person and is not pooled: ' + JSON.stringify({ p: r.s40.problems, pooled: r.s40.pooled }));
    for (const k of ['s10', 's12', 's14', 'n16']) ok(!r[k].problems.includes('missingSize') && !r[k].problems.includes('needsMapping') && r[k].pooled && r[k].poolSize === r[k].size, `${k}: nothing held, pooled with the size chosen: ` + JSON.stringify(r[k]));
    // the charm each piece gets is the disc at the size chosen (the outline's own width: the hoop and the margin do not count), its hole kept
    near(r.s10.wMm, 10, 0.03, 'the 10 mm piece is 10 mm across, measured on its own outline'); near(r.s12.wMm, 12, 0.03, 'the 12 mm piece is 12 mm across (as the repo draws it)'); near(r.s14.wMm, 14, 0.03, 'the 14 mm piece is 14 mm across'); near(r.n16.wMm, 16, 0.03, 'the 16 mm piece is 16 mm across');
    near(r.s10.hMm / r.s10.wMm, r.s12.hMm / r.s12.wMm, 0.005, 'the shape is the same drawing (width : height)');
    for (const k of ['s10', 's12', 's14', 'n16']) ok(r[k].holes === r.s12.holes && r[k].holes >= 1, `${k}: its hanging hole is still there (${r[k].holes})`);
    ok(new Set([r.s10.hash, r.s12.hash, r.s14.hash, r.n16.hash]).size === 4, 'four sizes are four traced charms (four hashes): ' + [r.s10.hash, r.s12.hash, r.s14.hash].join(','));
    ok(/#fit-10MM@0\.82/.test(r.s10.poolPath) && !/#fit/.test(r.s12.poolPath) && /#fit-14MM@1\.15/.test(r.s14.poolPath), 'the pool row names the file it was drawn from: ' + [r.s10.poolPath, r.s12.poolPath, r.s14.poolPath].join(' | '));
    ok(out.sources.length === 4, 'one source per size drawn (10, 12, 14, 16): ' + out.sources.join(' | '));
    ok(out.baseBefore === out.baseAfter, 'the repo record is not changed by any of it');
    // the lookups
    ok(out.g.g10 && out.g.g10.scaled && /#fit-10MM@/.test(out.g.g10.path) && out.g.g10.base === true, 'sizeEntry(10MM) is a design of its own, scaled from the entry\'s own drawing: ' + JSON.stringify(out.g.g10));
    near(out.g.g10.w, (37 - 2.6) * 0.8240 + 2.6, 0.3, 'its planned width is the drawing scaled plus the same margin');
    ok(out.g.g12 && out.g.g14 === false && out.g.g40 && out.g.gM && out.g.gN, '12 mm is the entry itself; 14 is scaled; an impossible size and a letter size are left as they were');
    ok(out.g.again, 'the same design object is handed out again (cached), so a list of 400 lines does no work twice');
    ok(out.key.scaled === '10MM' && out.key.plain === null, 'sizeKeyOf names the size a scaled file was made for: ' + JSON.stringify(out.key));
    near(out.restored.wMm, 10, 0.03, 'a record kept from a saved sheet is drawn at 10 mm again'); ok(out.restored.holes >= 1, 'and keeps its hole');
    ok(out.sized.s10 && out.sized.s10.scaled && Math.abs(out.sized.s10.scale - 10 / ((37 - 2.6) * MM)) < 1e-3, 'a design that holds 12 and 14 mm: 10 mm is scaled from the nearest: ' + JSON.stringify(out.sized.s10));
    ok(out.sized.s12 === true, 'a size it holds is that size as it is'); ok(out.sized.s11 && out.sized.s11.scaled, '11 mm is scaled too'); ok(out.sized.s40 === null, 'an impossible size of a sized design is nothing: the line is held');
    ok(!w.errors.length, 'no page error: ' + w.errors.join(' | '));
    console.log('size fit pool OK: ' + n + ' checks');
  } finally { await w.close(); }
})().catch(e => { console.error(e); process.exit(1); });
