// ENGLIVE (engrave-ui-1012 round 2): the REAL-DRAWING test world for every engraving-UI worker.
//
// Why it exists: round 1 passed every offline test on a fake site with NO drawings, fonts or Engrave jobs, then failed Paul live (no charm in the thumbnail boxes, the work area ignoring the picked font).
// This world makes the real thing, offline: the real sorter page (charm-nest-1.html) in headless Chromium over the repo's fake site (bridge-server.cjs: the real charmNest* handlers over an in-memory Firestore),
// a MASTER LIBRARY holding the REAL charm drawings of tests/charm-nest/fixtures/*.ai (COMET_9427, SCHRODINGER_CAT_1/_2, TENNIS_BALL_HUGGIE_, TINY_INITIAL_TAG_1), served like Storage serves them, and orders that
// go through the REAL intake (Orders.interpretAll), the REAL pool (Pool.addAll: the real charm of each piece, the right earring mirrored), the REAL classifier path (Engrave.classify: the line's setup gives each face its
// words, a plain back is read by the fake site's stub: no paid reading), and the REAL fit (Engrave.fitAll: the page's fit worker, a real installed font from vendor/fonts via opentype). So each Engrave job has a real
// job.view / job.mask / job.fit.glyphs / job.fontKey, the work area draws the real charm with its cut-outs and the engraving on it, and `Engrave.renderBack/renderFront` work as they do for Paul. Nothing is written anywhere
// real, no Etsy, no paid AI. Every name is made up ("Name One").
//
//   const R = require('./_engraveRealWorld.cjs');
//   const w = await R.world({ viewport: { width: 1440, height: 900 }, shots: '/some/dir' });      // null when there is no playwright-core
//   const made = await R.build(w);                          // all six orders (or R.build(w, ['back','both']))  -> { back: { rid, key, line, jobs: [{key, face, slot, state, text, fontKey}], ... } }
//   await R.openOrderWindow(w, 'both');                      // the order window of that order, settled (cards drawn, their thumbnails painted)
//   await R.openEngraveDetail(w, 'both', { face: 'back' });  // the Engrave tab's detail (card + panel + work area) on that piece's face job, settled
//   await R.shot(w, 'LIVE-detail-both');                     // PNG into w.shots (default /mnt/project-files/plans/engrave-ui-1012/round2/images/)
//   await w.close();
//
// Run the demo (builds the world, opens each view, saves screenshots):   PW_DIR=/opt/node22/lib/node_modules/playwright/node_modules node tests/charm-nest/_engraveRealWorld.cjs [shotsDir]
// Orders (R.ORDERS):  back   = necklace, back only (COMET_9427 drawing, font Stylish, personalisation "Name One")
//                     front  = necklace, front only (SCHRODINGER CAT 1, Charm Type "Tag1 (front)", font Vibur)
//                     both   = necklace, front + back (SCHRODINGER CAT 2, "Engraving Side: Both", font Ace, front "W" back "Name One")
//                     tag    = mini tag, front + back (the real mini-tag drawing as the master design 5X8.5, listing 4458930445 "Tag1 (front+back)", font Ace)
//                     discs  = necklace of 3 discs, one job per disc D1..D3 (COMET_9427: A, B, C)
//                     earrings = a stud earring PAIR (COMET_9427): Left + mirrored Right, NEVER engraved: no job, no card
const path = require('path'), fs = require('fs');
const root = path.join(__dirname, '../..');
const Base = require('./_orderFactsWorld.cjs');
const FIX = path.join(__dirname, 'fixtures');
const DEFAULT_SHOTS = '/mnt/project-files/plans/engrave-ui-1012/round2/images';

/** A ROUND DISC with a hoop and engraved art on its front, made here (no fixture drawing is a disc): a PDF-compatible .ai laid out as the masters are read (layers CUT / ENGRAVE / HATCH), a 34 pt disc (12 mm), a cyan jump ring
 *  that overlaps its top (the page welds it into the cut line), a made-up flower in red engraving strokes with a small blue hatched centre. Nothing in it is a customer's. */
function discAi() {
  const f = n => (Math.round(n * 1000) / 1000).toString();
  const circle = (cx, cy, r) => { const k = 0.5522847498 * r; return `${f(cx + r)} ${f(cy)} m ${f(cx + r)} ${f(cy + k)} ${f(cx + k)} ${f(cy + r)} ${f(cx)} ${f(cy + r)} c ${f(cx - k)} ${f(cy + r)} ${f(cx - r)} ${f(cy + k)} ${f(cx - r)} ${f(cy)} c ${f(cx - r)} ${f(cy - k)} ${f(cx - k)} ${f(cy - r)} ${f(cx)} ${f(cy - r)} c ${f(cx + k)} ${f(cy - r)} ${f(cx + r)} ${f(cy - k)} ${f(cx + r)} ${f(cy)} c h`; };
  const ellipse = (cx, cy, rx, ry, deg) => { const a = deg * Math.PI / 180, c = Math.cos(a), s = Math.sin(a), k = 0.5522847498, P = (x, y) => [cx + x * c - y * s, cy + x * s + y * c];
    const seg = [[[rx, ry * k], [rx * k, ry], [0, ry]], [[-rx * k, ry], [-rx, ry * k], [-rx, 0]], [[-rx, -ry * k], [-rx * k, -ry], [0, -ry]], [[rx * k, -ry], [rx, -ry * k], [rx, 0]]];
    let o = `${P(rx, 0).map(f).join(' ')} m `; for (const g of seg) o += g.map(q => P(q[0], q[1]).map(f).join(' ')).join(' ') + ' c '; return o + 'h'; };
  const CX = 30, CY = 28, R = 17, HY = CY + R + 1.8, HO = 3.4, HI = 1.7;
  // the flower: 8 petals round a centre, two rings, a stem and two leaves
  let art = ''; for (let i = 0; i < 8; i++) art += ellipse(CX + 4.3 * Math.cos(i * Math.PI / 4), CY + 3 + 4.3 * Math.sin(i * Math.PI / 4), 2.7, 4.6, i * 45 - 90) + ' S\n';
  art += circle(CX, CY + 3, 6.2) + ' S\n' + circle(CX, CY + 3, 2.6) + ' S\n';
  art += `${f(CX)} ${f(CY - 5)} m ${f(CX - 1.5)} ${f(CY - 9)} ${f(CX + 1.5)} ${f(CY - 12)} ${f(CX)} ${f(CY - 14)} c S\n` + ellipse(CX - 5, CY - 9, 1.6, 4.6, 55) + ' S\n' + ellipse(CX + 5, CY - 10, 1.6, 4.6, -55) + ' S\n';
  const content = [
    '/OC /ocEng BDC', 'q 1 0.114 0.145 RG 0.249 w', art.trim(), 'Q', 'EMC',
    '/OC /ocHatch BDC', 'q 0 0 1 rg', circle(CX, CY + 3, 1.7) + ' f', 'Q', 'EMC',
    '/OC /ocCut BDC', `q 0 0 0 RG 0.283 w ${circle(CX, CY, R)} S Q`, `q 0.31 0.82 0.851 RG 0.283 w ${circle(CX, HY, HO)} ${circle(CX, HY, HI)} S Q`, 'EMC'
  ].join('\n') + '\n';
  const objs = [
    '<< /Type /Pages /Kids [ 4 0 R ] /Count 1 >>',
    '<< /Type /Catalog /Pages 1 0 R /OCProperties << /OCGs [ 5 0 R 6 0 R 7 0 R ] /D << /Order [ 5 0 R 6 0 R 7 0 R ] /ON [ 5 0 R 6 0 R 7 0 R ] /BaseState /ON >> >> >>',
    '<< /Producer (ENGLIVE test drawing) /Title (BFLOWER_DISC_TEST) >>',
    '<< /Type /Page /Parent 1 0 R /Resources << /Properties << /ocEng 5 0 R /ocHatch 6 0 R /ocCut 7 0 R >> >> /MediaBox [ 0 0 60 60 ] /Contents 8 0 R >>',
    '<< /Type /OCG /Name (ENGRAVE) >>', '<< /Type /OCG /Name (HATCH) >>', '<< /Type /OCG /Name (CUT) >>',
    `<< /Length ${Buffer.byteLength(content)} >>\nstream\n${content}endstream`
  ];
  let out = '%PDF-1.7\n'; const at = [];
  objs.forEach((o, i) => { at.push(Buffer.byteLength(out)); out += `${i + 1} 0 obj\n${o}\nendobj\n`; });
  const x0 = Buffer.byteLength(out);
  out += `xref\n0 ${objs.length + 1}\n0000000000 65535 f \n` + at.map(n => String(n).padStart(10, '0') + ' 00000 n \n').join('') + `trailer\n<< /Size ${objs.length + 1} /Root 2 0 R /Info 3 0 R >>\nstartxref\n${x0}\n%%EOF\n`;
  return Buffer.from(out, 'latin1');
}

/** the real drawings: the master index entry (a real field set as the index writes it) and the .ai file it points at */
const DESIGNS = {
  'COMET_9427': { file: 'COMET_9427.ai', from: 'comet-master-entries.json' },
  'SCHRODINGER CAT 1': { file: 'SCHRODINGER_CAT_1.ai', from: 'schro-master-entries.json' },
  'SCHRODINGER CAT 2': { file: 'SCHRODINGER_CAT_2.ai', from: 'schro-master-entries.json' },
  'TENNIS BALL (HUGGIE)': { file: 'TENNIS_BALL_HUGGIE_.ai', from: 'tennis-master-entries.json' },
  // the Mini Tag listing's "Tag1" shape is the master design "5X8.5" (charm-nest-orders.js TAG_SHAPES); the real mini-tag drawing stands in for it
  // a round disc with a hoop and engraved front art, drawn by discAi() above (no fixture is a disc): the made-up birth-flower disc of the held order
  'BFLOWER_9901': { file: 'BFLOWER_9901.ai', make: discAi, entry: { sku: 'BFLOWER_9901', widthPt: 34, heightPt: 39.2, areaPt2: 935, members: 16, holes: 1, engravable: true, engravableBy: 'index', upAngle: 90, upSource: 'drawn', charmHash: '0afbab0c', backKeepOut: [] } },
  '5X8.5': { file: 'TINY_INITIAL_TAG_1.ai', entry: { sku: '5X8.5', widthPt: 45.02, heightPt: 16.673, areaPt2: 627.28, members: 3, holes: 1, engravable: true, engravableBy: 'index', upAngle: 90, upSource: 'drawn', charmHash: '34ad8c23', backKeepOut: [] } }
};
/** what a person once answered under Options for these made-up listings (Charm_Option_Map, listing id -> { option name: { value: answer } }): the engraving-side words and the earring form need no shape */
const OPTION_MAPS = {
  '1890000102': { 'engraving side': { front: { field: 'ignore', value: null } } },
  '1890000103': { 'engraving side': { both: { field: 'ignore', value: null } } },
  '1890000106': { style: { 'stud earrings': { field: 'form', value: 'earrings' } } }
};
const V = (name, value) => ({ name, value });
const TAG_TITLE = 'Mini Tag Initial Charm Add On Charm Personalized Letter Charm Minimalist Tiny Pendant Gift for Her Custom Initial Charm';
const SHIP_DAYS = 5;
/** the orders; rid / listing are made up and unique; buyers are made up */
const ORDERS = {
  back: { rid: '4290000101', sku: 'COMET_9427', listing: '1890000101', title: 'Comet Necklace, Celestial Charm Necklace in 14k Gold Fill', buyer: 'Name One', placed: { day: 0, h: 9, m: 10 }, ship: SHIP_DAYS, variations: [V('Metal choice', 'Gold'), V('Font Choice', 'Stylish')], personalization: ['Name One'] },
  front: { rid: '4290000102', sku: 'SCHRODINGER CAT 1', listing: '1890000102', title: 'Cat Necklace, Whimsical Cat Charm Gift', buyer: 'Name Two', placed: { day: 0, h: 9, m: 20 }, ship: SHIP_DAYS, variations: [V('Metal choice', 'Gold'), V('Engraving Side', 'Front'), V('Font Choice', 'Vibur')], personalization: ['Name One'] },
  both: { rid: '4290000103', sku: 'SCHRODINGER CAT 2', listing: '1890000103', title: 'Cat Necklace, Whimsical Cat Charm, engraved on both sides', buyer: 'Name Three', placed: { day: 0, h: 9, m: 30 }, ship: SHIP_DAYS, variations: [V('Metal choice', 'Gold'), V('Engraving Side', 'Both'), V('Font Choice', 'Ace')], personalization: ['front - W\nback - Name One'] },
  tag: { rid: '4290000104', sku: 'MINI_0445', listing: '4458930445', title: TAG_TITLE, buyer: 'Name Four', placed: { day: 0, h: 9, m: 40 }, ship: SHIP_DAYS, variations: [V('Metal Choice', 'Gold Filled'), V('Charm Type', 'Tag1 (front+back)'), V('Font Choice', 'Ace')], personalization: ['front - W\nback - 1. 27. 24'] },
  discs: { rid: '4290000105', sku: 'COMET_9427', listing: '1890000105', title: 'Initial Disc Necklace, Personalized Letter Discs', buyer: 'Name Five', placed: { day: 0, h: 9, m: 50 }, ship: SHIP_DAYS, variations: [V('Metal choice', 'Gold'), V('Number of Discs', '3 Discs')], personalization: ['Tag 1: A, Tag 2: B, Tag 3: C'] },
  earrings: { rid: '4290000106', sku: 'COMET_9427', listing: '1890000106', title: 'Comet Stud Earrings, Celestial Earrings in 14k Gold Fill', buyer: 'Name Six', placed: { day: 0, h: 10, m: 0 }, ship: SHIP_DAYS, variations: [V('Metal choice', 'Gold'), V('Style', 'Stud earrings')], personalization: [] }
};
// REFERENCE orders for engrave-realtime.cjs (the same charm as `back`, one thing different each: what the page's own pipeline draws for that word / font is the yardstick a staged edit is measured against). Not in NAMES: build them by name.
ORDERS.refAnna = Object.assign({}, ORDERS.back, { rid: '4290000111', listing: '1890000111', buyer: 'Name Seven', personalization: ['Anna'] });
ORDERS.refMarck = Object.assign({}, ORDERS.back, { rid: '4290000112', listing: '1890000112', buyer: 'Name Eight', variations: [V('Metal choice', 'Gold'), V('Font Choice', 'Pristina')] });
// PRENEST (engrave-prenest-1013): a round DISC necklace with its own front art (a flower), engraved on the BACK only, as the live Birth Flower order 4200828980 was: the listing's option says "Gold - 16" - 2 sides" (BFSIDES: the design's art is the front, the
//   buyer's words are the back's), the back words are three short lines and the font is a typed word in the note ("Typewriter font" = Crimson Text), NOT a Font Choice option. The intake reads those words and that font for free (no paid reading), so the
//   card of a line with no Engrave job still knows them, as the live card did.
//   heldDisc = the live case of 13 Oct: the order is ON HOLD (row state "pulled" with a person's hold on it), was NEVER pooled or nested (no pool item, no run, no sheet) and so has no Engrave job. build() seeds it AFTER the pool
//              and the classifier, so nothing in the world ever pools it: it stays held, in no run, with no job (until the page itself makes one, which is the point of the test).
//   discTwin = the SAME disc, words and note on a normal order that goes through the real pool, the sheet and the fit: the page's own drawing of it (job.view / fit) is the yardstick the held card is measured against.
const DISC_NOTE = 'Typewriter font\n\nName One\nName Two\nDate One';
ORDERS.heldDisc = { rid: '4290000121', sku: 'BFLOWER_9901', listing: '1890000121', title: 'Birthflower Necklace Disc, Birth Month Flower Charm Necklace in 14k Gold Fill', buyer: 'Name Nine', placed: { day: 0, h: 10, m: 10 }, ship: SHIP_DAYS, variations: [V('Size', 'Gold - 16" - 2 sides')], personalization: [DISC_NOTE], held: true };
ORDERS.discTwin = Object.assign({}, ORDERS.heldDisc, { rid: '4290000122', listing: '1890000122', buyer: 'Name Ten', held: false });
const NAMES = ['back', 'front', 'both', 'tag', 'discs', 'earrings'];
const EXPECT_JOBS = { back: 1, front: 1, both: 2, tag: 2, discs: 3, earrings: 0, refAnna: 1, refMarck: 1, heldDisc: 0, discTwin: 1 };   // how many Engrave jobs each order has once it is built (build waits for them)
const keyOf = n => `${ORDERS[n].rid}_${ORDERS[n].rid}1`;

/** the master library: put every real design's index entry and file into the fake site's Firestore / Storage (before the page reads the master) */
function seedMaster(st, skus) {
  for (const sku of skus || Object.keys(DESIGNS)) {
    const d = DESIGNS[sku]; if (!d) continue;
    let entry = d.entry; if (!entry) { const j = JSON.parse(fs.readFileSync(path.join(FIX, d.from), 'utf8')); entry = j.entries.find(e => e.sku === sku); }
    if (!entry) throw new Error('no index entry for ' + sku);
    const aiPath = 'charmnest/master/' + d.file;
    st.put('Charm_Master_Index', sku, Object.assign({}, entry, { sku, aiPath, thumbPath: null, blocked: null, sizes: null, conflict: null }));
    st.blobs.set(aiPath, { buf: d.make ? d.make() : fs.readFileSync(path.join(FIX, d.file)), generation: 1, meta: { contentType: 'application/illustrator', metadata: { firebaseStorageDownloadTokens: 't' } } });
  }
}

/** a person's saved answers under Options for the made-up listings */
function seedOptionMaps(st) { for (const [lid, map] of Object.entries(OPTION_MAPS)) { const m = {}; for (const [n, vals] of Object.entries(map)) { m[n] = {}; for (const [v, a] of Object.entries(vals)) m[n][v] = Object.assign({ by: 'Test', at: 1 }, a); } st.put('Charm_Option_Map', lid, { listingId: lid, map: m }); } }

/** The world: the real page over the fake site with the master library seeded. `opts`: { viewport, shots, scale }. null when playwright-core is missing. */
async function world(opts = {}) {
  const w = await Base.world(Object.assign({ viewport: { width: 1440, height: 900 } }, opts));
  if (!w) return null;
  seedMaster(w.srv.st); seedOptionMaps(w.srv.st);
  await w.page.evaluate(async () => { await Master.load(true); await Orders.loadMaps(true); if (window.Engrave) await Engrave.loadFonts(); });
  w.shots = opts.shots || process.env.SHOTS || DEFAULT_SHOTS; w.made = {};
  return w;
}

/** Orders in, through the real intake, pool, classifier and fit. Resolves { name: { rid, key, line, jobs: [{ key, face, slot, state, text, fontKey, fitted }], never } }. `names` default: all. */
async function build(w, names) {
  names = names || NAMES;
  const page = w.page;
  await page.evaluate(async ({ fixtures, orderSrc }) => {
    const orderOf = eval('(' + orderSrc + ')'), now = Date.now();
    await Orders.loadMaps(true);
    for (const fx of fixtures) {
      if (fx.held) continue;   // (a held order is seeded after the pool and the classifier, below: nothing here may pool it)
      const order = orderOf(fx, now);
      for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
    }
    Orders.interpretAll(); CN.setMode('orders'); Orders.render();
    // the REAL pool: each piece gets the real charm of its design (an earring pair: Left and the mirrored Right, a counted line: one piece per disc)
    await Pool.addAll({ runId: 'run-real', lines: {} });
    // the pieces sit on a sheet that has a name (the fit needs one: Engrave.canFit); where does not matter here
    let n = 0;
    for (const sh of allSheets()) if (sh.charms && sh.charms.length) { n++; sh.sheetId = sh.sheetId || 'real-sheet-' + n; sh.fileBase = sh.fileBase || 'gold-real-' + n; sh.placements = sh.charms.map((c, i) => ({ id: c.id, cxPt: 40 + 70 * i, cyPt: 60, angle: 0 })); }
    // the real classifier path: faced lines take each face's words from the line's setup (no reading), a plain back is read by the fake site's stub (offline)
    for (const row of Orders.rows()) if (['pooled', 'written'].includes(row.state) && row.spec && (row.spec.engraveCandidate || row.spec.engraving)) { try { await Engrave.classify(row); } catch (e) { console.warn('classify', e && e.message); } }
  }, { fixtures: names.map(n => ORDERS[n]), orderSrc: Base.orderOf.toString() });
  // the real fit, in the font the order names (the page's fit worker; a busy machine takes a while, so wait for it: up to 10 minutes)
  const t0 = Date.now();
  for (;;) {
    // (a busy machine can be slow to make a job: every order must have the jobs it should have, and none may still be at work; an order that has not got its jobs yet is classified again)
    const open = await page.evaluate(async ({ want }) => {
      let lacking = 0;
      for (const [rid, n] of want) {
        const rows = Orders.rows().filter(r => r.order.receiptId === rid), have = () => [...Engrave.items().values()].filter(j => rows.includes(j.row) || (j.row && j.row.parentRow && rows.includes(j.row.parentRow))).length;
        if (have() < n) {
          // (an order that is not pooled yet, or has no setup, is read and pooled once more; at most twice: it has been seen to miss its jobs now and then on a busy machine)
          if (rows.some(r => !['pooled', 'written'].includes(r.state) || !r.spec) && (window.__rtRetry = (window.__rtRetry || 0) + 1) <= 2) { try { Orders.interpretAll(); await Pool.addAll({ runId: 'run-real', lines: {} }); } catch (e) { console.warn('re-pool', e && e.message); } }
          for (const row of rows) if (['pooled', 'written'].includes(row.state) && row.spec && (row.spec.engraveCandidate || row.spec.engraving)) { try { await Engrave.classify(row); } catch (e) { console.warn('classify', e && e.message); } }
          if (have() < n) lacking++;
        }
      }
      try { await Engrave.fitAll(); } catch (e) { console.warn('fitAll', e && e.message); }
      // (a machine under heavy load can outrun the fit worker's own 2-minute limit: that is not the engraving's fault; the page's own Retry is pressed for it, at most 3 times a job)
      let retried = 0; for (const j of Engrave.items().values()) if (j.state === 'blocked' && /took too long/.test(j.reason || '') && (j.__retry = (j.__retry || 0) + 1) <= 3) { retried++; try { Engrave.fitJob(j); } catch (e) { console.warn('retry fit', e && e.message); } }
      return retried + lacking + [...Engrave.items().values()].filter(j => ['classify', 'reclassify', 'ready', 'fitting'].includes(j.state)).length;
    }, { want: names.map(n => [ORDERS[n].rid, EXPECT_JOBS[n] == null ? 1 : EXPECT_JOBS[n]]) });
    if (!open || Date.now() - t0 > 600000) break;
    await new Promise(r => setTimeout(r, 400));
  }
  // a HELD order (ORDERS.x.held): on hold, never pooled, in no run, on no sheet: a pulled line with a person's hold on it, exactly the shape of the live one (the pool and the classifier above never saw it)
  const held = names.filter(n => ORDERS[n].held);
  if (held.length) await page.evaluate(async ({ fixtures, orderSrc }) => {
    const orderOf = eval('(' + orderSrc + ')'), now = Date.now();
    for (const fx of fixtures) {
      const order = orderOf(fx, now);
      for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); if (B.orders.byKey.get(key)) continue; const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
    }
    Orders.interpretAll();
    for (const fx of fixtures) for (const row of Orders.rows().filter(r => r.order.receiptId === fx.rid)) { row.state = 'pulled'; row.hold = 'held by Test'; row.reason = null; }
    Orders.render();
  }, { fixtures: held.map(n => ORDERS[n]), orderSrc: Base.orderOf.toString() });
  const out = await page.evaluate(names => {
    const res = {};
    for (const [n, rid] of names) {
      const rows = Orders.rows().filter(r => r.order.receiptId === rid);
      const jobs = [...Engrave.items().values()].filter(j => rows.includes(j.row) || (j.row && j.row.parentRow && rows.includes(j.row.parentRow)));
      res[n] = { rid, key: rows[0] && rows[0].key, state: rows[0] && rows[0].state, why: (rows[0] && (rows[0].reason || (rows[0].problems || [])[0] || (rows[0].spec && rows[0].spec.hold))) ? String(rows[0].reason || (rows[0].problems || [])[0] || '').slice(0, 120) : '', poolIds: rows[0] && rows[0].poolIds, never: !!(rows[0] && rows[0].spec && rows[0].spec.engraveNever), pieceCount: rows[0] && rows[0].spec && rows[0].spec.pieceCount,
        mode: rows[0] && rows[0].spec && rows[0].spec.engraving && rows[0].spec.engraving.mode || null,
        jobs: jobs.map(j => ({ key: j.key, face: j.face === 'front' ? 'front' : 'back', slot: j.slot || null, state: j.state, text: j.text, fontKey: j.fontKey || null, fitted: !!(j.fit && j.fit.glyphs && j.fit.glyphs.length || j.fit && j.fit.cmds), hasMask: !!j.mask, hasView: !!j.view, reason: j.reason || null, questions: (j.questions || []).slice(0, 2) })) };
    }
    return res;
  }, names.map(n => [n, ORDERS[n].rid]));
  Object.assign(w.made, out);
  return out;
}

const settle = (page, ms = 250) => page.evaluate(async ms => { await new Promise(r => setTimeout(r, ms)); await new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r))); }, ms);

/** The order window of an order, with its engraving cards drawn. */
async function openOrderWindow(w, name) {
  const key = w.made[name] ? w.made[name].key : keyOf(name);
  await w.page.evaluate(async k => { CN.setMode('orders'); await OrderWin.open(k); }, key);
  await w.page.waitForSelector('#orderWin', { state: 'attached' });
  await w.page.waitForFunction(() => { const h = document.getElementById('owEng'); return !h || h.hidden || !!h.querySelector('.engCard') || !!h.children.length; }, null, { timeout: 15000 }).catch(() => {});
  await settle(w.page, 600);
  return key;
}

/** The Engrave tab's detail (card + options panel + work area) on a piece's face job. `o`: { face: 'front'|'back' (default: the first job's face), slot ('L'|'R'|'D1'..) }. Resolves the job key (null when the order has no job). */
async function openEngraveDetail(w, name, o = {}) {
  const key = w.made[name] ? w.made[name].key : keyOf(name);
  const jkey = await w.page.evaluate(({ key, face, slot }) => {
    const row = B.orders.byKey.get(key); if (!row) return null;
    const jobs = Engrave.jobsOf(row).filter(j => !slot || (j.slot || null) === slot);
    const j = jobs.find(x => !face || (x.face === 'front' ? 'front' : 'back') === face) || jobs[0]; if (!j) return null;
    try { if (window.OrderWin && OrderWin.close) OrderWin.close(); } catch (_) {}
    CN.setMode('engrave'); Engrave.restoreView({ tab: 'place', list: false, chosen: true, focus: j.key }); Engrave.render();
    return j.key;
  }, { key, face: o.face || null, slot: o.slot || null });
  if (!jkey) return null;
  await w.page.waitForSelector('#egQueue .rvItem[data-kind=placement]', { timeout: 15000 }).catch(() => {});
  await settle(w.page, 700);
  return jkey;
}

async function shot(w, name, o = {}) {
  fs.mkdirSync(w.shots, { recursive: true });
  const file = path.join(w.shots, name.replace(/\.png$/i, '') + '.png');
  await w.page.screenshot(Object.assign({ path: file }, o.clip ? { clip: o.clip } : {}));
  return file;
}

module.exports = { world, build, openOrderWindow, openEngraveDetail, shot, settle, seedMaster, ORDERS, NAMES, EXPECT_JOBS, DESIGNS, keyOf, root };

// the demo
if (require.main === module) (async () => {
  const w = await world({ shots: process.argv[2] || undefined });
  if (!w) { console.log('no playwright-core'); return; }
  try {
    const made = await build(w);
    console.log(JSON.stringify(made, null, 1));
    for (const n of NAMES) {
      if (!made[n].jobs.length) { console.log(n, 'has no engraving job' + (made[n].never ? ' (an earring: never engraved)' : '')); }
      await openOrderWindow(w, n); console.log('order window', n, await shot(w, 'LIVE-order-' + n));
      if (made[n].jobs.length) { await openEngraveDetail(w, n); console.log('detail', n, await shot(w, 'LIVE-detail-' + n)); }
    }
    console.log('page errors:', w.errors);
  } finally { await w.close(); }
})().catch(e => { console.error(e); process.exit(1); });
