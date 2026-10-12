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

/** the real drawings: the master index entry (a real field set as the index writes it) and the .ai file it points at */
const DESIGNS = {
  'COMET_9427': { file: 'COMET_9427.ai', from: 'comet-master-entries.json' },
  'SCHRODINGER CAT 1': { file: 'SCHRODINGER_CAT_1.ai', from: 'schro-master-entries.json' },
  'SCHRODINGER CAT 2': { file: 'SCHRODINGER_CAT_2.ai', from: 'schro-master-entries.json' },
  'TENNIS BALL (HUGGIE)': { file: 'TENNIS_BALL_HUGGIE_.ai', from: 'tennis-master-entries.json' },
  // the Mini Tag listing's "Tag1" shape is the master design "5X8.5" (charm-nest-orders.js TAG_SHAPES); the real mini-tag drawing stands in for it
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
const NAMES = Object.keys(ORDERS);
const keyOf = n => `${ORDERS[n].rid}_${ORDERS[n].rid}1`;

/** the master library: put every real design's index entry and file into the fake site's Firestore / Storage (before the page reads the master) */
function seedMaster(st, skus) {
  for (const sku of skus || Object.keys(DESIGNS)) {
    const d = DESIGNS[sku]; if (!d) continue;
    let entry = d.entry; if (!entry) { const j = JSON.parse(fs.readFileSync(path.join(FIX, d.from), 'utf8')); entry = j.entries.find(e => e.sku === sku); }
    if (!entry) throw new Error('no index entry for ' + sku);
    const aiPath = 'charmnest/master/' + d.file;
    st.put('Charm_Master_Index', sku, Object.assign({}, entry, { sku, aiPath, thumbPath: null, blocked: null, sizes: null, conflict: null }));
    st.blobs.set(aiPath, { buf: fs.readFileSync(path.join(FIX, d.file)), generation: 1, meta: { contentType: 'application/illustrator', metadata: { firebaseStorageDownloadTokens: 't' } } });
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
  // the real fit, in the font the order names (the page's fit worker; a busy machine takes a while, so wait for it: up to 4 minutes)
  const t0 = Date.now();
  for (;;) {
    const open = await page.evaluate(async () => { try { await Engrave.fitAll(); } catch (e) { console.warn('fitAll', e && e.message); } return [...Engrave.items().values()].filter(j => ['classify', 'reclassify', 'ready', 'fitting'].includes(j.state)).length; });
    if (!open || Date.now() - t0 > 240000) break;
    await new Promise(r => setTimeout(r, 400));
  }
  const out = await page.evaluate(names => {
    const res = {};
    for (const [n, rid] of names) {
      const rows = Orders.rows().filter(r => r.order.receiptId === rid);
      const jobs = [...Engrave.items().values()].filter(j => rows.includes(j.row) || (j.row && j.row.parentRow && rows.includes(j.row.parentRow)));
      res[n] = { rid, key: rows[0] && rows[0].key, state: rows[0] && rows[0].state, poolIds: rows[0] && rows[0].poolIds, never: !!(rows[0] && rows[0].spec && rows[0].spec.engraveNever), pieceCount: rows[0] && rows[0].spec && rows[0].spec.pieceCount,
        mode: rows[0] && rows[0].spec && rows[0].spec.engraving && rows[0].spec.engraving.mode || null,
        jobs: jobs.map(j => ({ key: j.key, face: j.face === 'front' ? 'front' : 'back', slot: j.slot || null, state: j.state, text: j.text, fontKey: j.fontKey || null, fitted: !!(j.fit && j.fit.glyphs && j.fit.glyphs.length || j.fit && j.fit.cmds), hasMask: !!j.mask, hasView: !!j.view, reason: j.reason || null })) };
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

module.exports = { world, build, openOrderWindow, openEngraveDetail, shot, settle, seedMaster, ORDERS, NAMES, DESIGNS, keyOf, root };

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
