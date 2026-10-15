// SIZEFIT hardening (Paul, 15 Oct 2026), on the REAL drawing of a round disc with a hoop (BFLOWER_9901, as size-fit-pool.cjs): what a hostile shop could still get wrong once the disc is drawn at the buyer's size.
//   · a saved sheet whose source is a scaled disc is restored into its card AT THE SIZE IT WAS CUT (it came back at the library's size, its placement lost)
//   · an impossible size holds the line with a size a person can pick, and picking it releases the line
//   · an earring pair, a quantity of 3 and a counted necklace of discs: every piece the same diameter, the Right ear mirrored from the scaled Left
//   · the .ai a sheet is written to holds each scaled piece at its size (the laser reads this file)
//   node tests/charm-nest/size-fit-pool-hard.cjs      (browser: run through /tmp/cn-run.sh 240)
'use strict';
const path = require('path'), assert = require('assert/strict');
const R = require('./_engraveRealWorld.cjs'), Base = require('./_orderFactsWorld.cjs');
let n = 0; const ok = (c, why) => { assert(c, why); n++; };
const near = (a, b, tol, why) => ok(Math.abs(a - b) <= tol, `${why}: ${a} vs ${b}`);
const V = (name, value) => ({ name, value });
const mk = (rid, o) => Object.assign({ rid, sku: 'BFLOWER_9901', listing: '18' + rid.slice(-6), title: 'Birthflower Charm Only Disc Necklace', buyer: 'Name One', placed: { day: 0, h: 10, m: 10 }, ship: 3, variations: [], personalization: [] }, o);
const CASES = {
  a12: mk('4296000001', { variations: [V('Size of Necklace Charm', '12mm')] }),
  a14: mk('4296000002', { variations: [V('Size of Necklace Charm', '14mm')] }),
  a10: mk('4296000003', { variations: [V('Size of Necklace Charm', '10mm + engraving')] }),
  s40: mk('4296000004', { variations: [V('Size of Necklace Charm', '40mm')] }),
  ear: mk('4296000005', { title: 'Birthflower Stud Earrings in 14k Gold Fill', variations: [V('Style', 'Stud earrings'), V('Size', '10mm')] }),
  q3: mk('4296000006', { qty: 3, variations: [V('Size of Necklace Charm', '10mm')] }),
  d3: mk('4296000007', { title: 'Initial Disc Necklace, Personalized Letter Discs', variations: [V('Number of Discs', '3 Discs'), V('Size of Necklace Charm', '10mm')], personalization: ['Tag 1: A, Tag 2: B, Tag 3: C'] }),
  comma: mk('4296000008', { variations: [V('Size of Necklace Charm', '1,2 cm')] }),
  bare: mk('4296000009', { variations: [V('Disc size (mm)', '16')] }),
  hoop: mk('4296000010', { title: 'Birthflower Huggie Hoop Earrings', variations: [V('METAL CHOICE', 'Gold'), V('HOOP SIZE', '8.5mm')] })
};
(async () => {
  const w = await R.world({ viewport: { width: 1440, height: 900 } });
  if (!w) { console.log('size fit pool hard: no playwright-core here, skipped'); return; }
  const page = w.page;
  try {
    w.srv.st.put('Charm_Option_Map', '18' + CASES.ear.rid.slice(-6), { listingId: '18' + CASES.ear.rid.slice(-6), map: { style: { 'stud earrings': { field: 'form', value: 'earrings', by: 'T', at: 1 } } } });
    { const st = w.srv.st, cur = st.list('Charm_Master_Index').find(e => e.sku === 'BFLOWER_9901'); st.put('Charm_Master_Index', 'BFLOWER_9901', Object.assign({}, cur, { widthPt: 37, heightPt: 42.2, areaPt2: 973.8 })); await page.evaluate(() => Master.load(true)); }
    const out = await page.evaluate(async ({ cases, orderSrc }) => {
      const orderOf = eval('(' + orderSrc + ')'), now = Date.now(), res = { rows: {} };
      await Orders.loadMaps(true);
      for (const [name, fx] of Object.entries(cases)) {
        const order = orderOf(fx, now);
        for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null, __name: name }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
      }
      Orders.interpretAll();
      await Pool.addAll({ runId: 'run-sizefit-hard', lines: {} });
      const mm = c => (c.outline.bbox[2] - c.outline.bbox[0]) * 25.4 / 72;
      const rowOf = name => Orders.rows().find(r => r.__name === name);
      for (const name of Object.keys(cases)) {
        const row = rowOf(name), sp = row.spec || {};
        res.rows[name] = { size: sp.size, fit: sp.sizeFit && sp.sizeFit.action, state: row.state, problems: (row.problems || []).map(p => ({ kind: p.kind, available: p.available || null })),
          pieces: (row.poolIds || []).map(id => { const c = Pool.charmOf(id), pr = B.pool.rows.get(id); return c ? { side: pr && pr.side, mirror: !!c.mirror, mm: mm(c), holes: CharmNestPDF.cutLinesOf(c).length } : null; }) };
      }
      // the .ai a sheet is written to, read back: every scaled piece (the pair, the three discs of a necklace, the quantity of 3) at its size
      { const ids = [].concat(...['ear', 'q3', 'd3', 'a10', 'a14'].map(nm => rowOf(nm).poolIds)), charms = ids.map(id => Pool.charmOf(id)).filter(Boolean), srcs = new Map();
        for (const c of charms) { const s = S.poolSources[c.sourceId]; if (s) srcs.set(c.sourceId, s.parsed); }
        const bytes = await CharmNestPDF.buildSheet({ sheet: { wPt: 60 * charms.length + 80, hPt: 140 }, sources: srcs, placements: charms.map((c, i) => ({ charm: c, cxPt: 40 + 60 * i, cyPt: 60, angle: 0, scale: 1 })) });
        const parsed = await CharmNestPDF.parseSource(bytes, 'sheet.ai'), g = CharmNestPDF.groupCharms(parsed, { minPt: 6 });
        res.sheet = { want: charms.map(mm), got: g.charms.map(c => (c.outline ? c.outline.bbox[2] - c.outline.bbox[0] : c.bbox[2] - c.bbox[0]) * 25.4 / 72) }; }
      // an impossible size is released by a size a person can pick: what the Review card offers (problem.available) set as the line's size
      { const row = rowOf('s40'), p = (row.problems || []).find(x => x.kind === 'missingSize'), pick = p && p.available && p.available[0];
        res.release = { pick: pick || null };
        if (pick) { row.sizeOverride = pick; row.problems = []; row.state = 'pulled'; row.reason = null; Orders.interpretAll(); await Pool.addAll({ runId: 'run-sizefit-hard', lines: {} });
          const r2 = rowOf('s40'); res.release.after = { state: r2.state, problems: (r2.problems || []).map(x => x.kind), mm: (r2.poolIds || []).map(id => Pool.charmOf(id)).filter(Boolean).map(mm) }; } }
      // a saved sheet whose source is a scaled disc, restored into its card: the disc comes back at the size it was cut, its placement kept
      res.restore = {};
      for (const name of ['a12', 'a14', 'a10']) {
        const row = rowOf(name), id = row.poolIds[0], c = Pool.charmOf(id), src = [...B.pool.sources.values()].find(s => s.id === c.sourceId);
        const d = { id: 'sheet-' + name, metal: 'gold', sources: [{ id: src.id, name: src.name, hash: src.hash, bytes: src.bytes.length, path: src.cloud.path, url: src.cloud.url, pool: true, sku: 'BFLOWER_9901' }], placements: [{ id: c.id, hash: c.hash, source: src.name, index: 0, cxPt: 100, cyPt: 100, angle: 0 }], freePt2: 1000, usablePt2: 2000, pocket: 0, folder: 'f' };
        const before = new Set(S.sources.map(s => s.id));
        try { await restoreSheet(d); } catch (e) { res.restore[name] = { error: String(e && e.message) }; continue; }
        const ch = S.sources.filter(s => !before.has(s.id)).flatMap(s => s.charms), pg = pagesOf('gold').find(p => p.sheetId === d.id);
        res.restore[name] = { saved: mm(c), restored: ch.map(mm), pinned: pg ? pg.placements.length : null, hashEq: ch.map(x => x.hash === c.hash) };
      }
      // the scaled design is a copy of the entry: a facing a person sets on the entry afterwards (Master.patch changes the entry in place) is the facing of the scaled design too
      { const e = Master.entryFor('BFLOWER_9901'), was = e.facing; try { e.facing = 'R'; const g1 = Pool.sizeEntry(e, '10MM'); const f1 = g1.facing; e.facing = 'L'; const g2 = Pool.sizeEntry(e, '10MM'); e.facing = null; const g3 = Pool.sizeEntry(e, '10MM'); res.facing = [f1, g2.facing, g3.facing || null]; } finally { e.facing = was; } }
      // a size a person typed into a saved answer ("10 mm", "10mm", "1 cm") is drawn like the "10MM" the intake reads: the file key names the size by its number, never by the words typed
      { const e = Master.entryFor('BFLOWER_9901'), out = {}; for (const raw of ['10 mm', '10mm', '1 cm', '10.0MM']) { const g = Pool.sizeEntry(e, raw), src = await Pool.masterCharm(e, raw); out[raw] = { path: g && g.aiPath, key: g && Pool.sizeKeyOf(e, g.aiPath), mm: mm(src.charms[0]) }; } res.typed = out; }
      // upkeep: a scaled design a live line needs is kept, and one nobody needs any more (its order gone, its pieces committed) is let go: the key of a scaled file is the one the sources are named by
      { const s14 = Pool.charmOf(rowOf('a14').poolIds[0]).sourceId, s10 = Pool.charmOf(rowOf('a10').poolIds[0]).sourceId, kept = () => Object.keys(Session.poolSourcesInUse());
        const before = kept(); res.upkeep = { before: [before.includes(s14), before.includes(s10)] };
        const row = rowOf('a14'); row.state = 'gone'; for (const id of row.poolIds) { const pr = B.pool.rows.get(id); if (pr) pr.state = 'committed'; }
        S.unassigned = (S.unassigned || []).filter(c => c.sourceId !== s14); for (const pg of allSheets()) pg.charms = pg.charms.filter(c => c.sourceId !== s14);   // (its piece is off every sheet: cut and put away)
        const after = kept(); res.upkeep.after = [after.includes(s14), after.includes(s10)]; }
      res.errors = (window.__errs || []).slice();
      return res;
    }, { cases: CASES, orderSrc: Base.orderOf.toString() });

    const r = out.rows;
    // the readings the shop would trip on
    ok(r.comma.size === '12MM' && r.comma.fit === 'same' && r.comma.pieces[0] && Math.abs(r.comma.pieces[0].mm - 12) < 0.05, 'a buyer in Europe who writes "1,2 cm" gets the 12 mm disc (it was cut 20 mm): ' + JSON.stringify(r.comma));
    ok(r.bare.size === '16MM' && r.bare.pieces[0] && Math.abs(r.bare.pieces[0].mm - 16) < 0.05 && !r.bare.problems.length, '"Disc size (mm)": 16 is a 16 mm disc: ' + JSON.stringify(r.bare));
    // the HOOP's size (HOOP SIZE: 8.5mm, the shop's option on every huggie listing) is read as the size but never the diameter of the charm: the disc is drawn as the library holds it, 12 mm
    ok(r.hoop.size === '8.5MM' && r.hoop.fit === 'unchecked' && r.hoop.pieces.length >= 1 && r.hoop.pieces.every(p => p && Math.abs(p.mm - 12) < 0.05), 'a hoop size does not resize the charm: ' + JSON.stringify(r.hoop));
    // every piece the same diameter
    ok(r.ear.pieces.length === 2 && r.ear.pieces.map(p => p.side).join() === 'L,R' && r.ear.pieces[1].mirror, 'the earring pair is a Left and a mirrored Right: ' + JSON.stringify(r.ear.pieces));
    for (const nm of ['ear', 'q3', 'd3', 'a10']) for (const p of r[nm].pieces) { near(p.mm, 10, 0.03, `${nm}: every piece is 10 mm across`); ok(p.holes >= 1, `${nm}: its hole is kept`); }
    ok(r.q3.pieces.length === 3 && r.d3.pieces.length === 3, 'a quantity of 3 and a necklace of 3 discs are three pieces each');
    near(out.sheet.got.length, out.sheet.want.length, 0, 'every piece is written to the sheet');
    { const a = out.sheet.want.slice().sort((x, y) => x - y), b = out.sheet.got.slice().sort((x, y) => x - y); a.forEach((x, i) => near(b[i], x, 0.05, `the .ai holds a piece at ${x.toFixed(2)} mm`)); }   // (read back in the order the file lists them)
    // an impossible size is released by the size on offer
    ok(r.s40.state === 'held' && r.s40.problems.length === 1 && r.s40.problems[0].kind === 'missingSize', 'the 40 mm line is held: ' + JSON.stringify(r.s40));
    ok(r.s40.problems[0].available && r.s40.problems[0].available.length === 1 && /^\d+MM$/.test(r.s40.problems[0].available[0]), 'it offers the size the disc is drawn at (the Review card\'s list is not empty): ' + JSON.stringify(r.s40.problems[0].available));
    ok(out.release.after && out.release.after.state === 'pooled' && !out.release.after.problems.length && Math.abs(out.release.after.mm[0] - 12) < 0.05, 'the size picked releases the line, drawn as it is: ' + JSON.stringify(out.release));
    // a sheet saved with a scaled source comes back at its size, pinned
    for (const [nm, mm] of [['a12', 12], ['a14', 14], ['a10', 10]]) {
      const x = out.restore[nm]; ok(x && !x.error, `${nm}: restore ran: ` + JSON.stringify(x));
      near(x.restored[0], mm, 0.05, `${nm}: the restored disc is ${mm} mm`); ok(x.pinned === 1 && x.hashEq[0], `${nm}: its placement is kept: ` + JSON.stringify(x));
    }
    for (const raw of ['10 mm', '10mm', '1 cm', '10.0MM']) { const t = out.typed[raw]; ok(t && /#fit-10MM@0\.82\d*$/.test(t.path) && t.key === '10MM', `"${raw}" is the file key of the 10 MM design: ` + JSON.stringify(t)); near(t.mm, 10, 0.03, `"${raw}" draws a 10 mm disc`); }
    ok(out.upkeep.before.join() === 'true,true', 'a scaled design in use is kept by the upkeep: ' + JSON.stringify(out.upkeep));
    ok(out.upkeep.after.join() === 'false,true', 'one nobody needs any more is let go, the shared one is kept: ' + JSON.stringify(out.upkeep));
    ok(out.facing.join() === 'R,L,', 'a facing set on the entry after the scaled design was made reaches it: ' + JSON.stringify(out.facing));
    ok(!w.errors.length, 'no page error: ' + w.errors.join(' | '));
  } finally { await w.close(); }
  // the back words are fitted on the SCALED charm: the same three lines on the 12 mm disc and on the 10 mm disc, each fitted inside its own disc and the lettering smaller by the same ratio
  const NOTE = 'Typewriter font\n\nName One\nName Two\nDate One';
  for (const [nm, rid, size] of [['szA', '4297000001', '12mm + engraving'], ['szB', '4297000002', '10mm + engraving']]) { R.ORDERS[nm] = mk(rid, { variations: [V('Size of Necklace Charm', size), V('Metal choice', 'Gold')], personalization: [NOTE], title: 'Birthflower Necklace Disc, Birth Month Flower Charm Necklace in 14k Gold Fill', ship: 5 }); R.EXPECT_JOBS[nm] = 1; }
  const w2 = await R.world({ viewport: { width: 1440, height: 900 } });
  try {
    { const st = w2.srv.st, cur = st.list('Charm_Master_Index').find(e => e.sku === 'BFLOWER_9901'); st.put('Charm_Master_Index', 'BFLOWER_9901', Object.assign({}, cur, { widthPt: 37, heightPt: 42.2, areaPt2: 973.8 })); await w2.page.evaluate(() => Master.load(true)); }
    await R.build(w2, ['szA', 'szB']);
    const fits = await w2.page.evaluate(() => { const out = {}; for (const j of Engrave.items().values()) { const id = (j.copies || j.row.poolIds || [])[0], c = Pool.charmOf(id), f = j.fit || {}; out[j.row.order.receiptId] = { state: j.state, size: f.size, centre: f.centre, glyphs: f.glyphs && f.glyphs.length, box: c && c.outline.bbox.slice() }; } return out; });
    const a = fits['4297000001'], b = fits['4297000002'];
    ok(a && b && a.state === 'review' && b.state === 'review' && a.glyphs === 3 && b.glyphs === 3, 'both discs\' words are fitted: ' + JSON.stringify([a && a.state, b && b.state]));
    for (const f of [a, b]) ok(f.centre[0] > f.box[0] && f.centre[0] < f.box[2] && f.centre[1] > f.box[1] && f.centre[1] < f.box[3], 'the words sit inside the disc they are fitted on: ' + JSON.stringify(f));
    near(b.size / a.size, 10 / 12, 0.1, 'the lettering on the 10 mm disc is smaller than on the 12 mm disc by the ratio of the discs');
    ok(!w2.errors.length, 'no page error (engraving): ' + w2.errors.join(' | '));
  } finally { await w2.close(); }
  console.log('size fit pool hard OK: ' + n + ' checks');
})().catch(e => { console.error(e); process.exit(1); });
