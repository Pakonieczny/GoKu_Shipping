/* DESIGNS (Paul, 28 Oct 2026: "this repository must grow with every new update or change to any charm using the charm editor"): the design library on the real sorter page.
   The repo's fake site (_engraveRealWorld.cjs: the real intake, pool and fit; the real charmNestLibrary handler over the in-memory Firestore and Storage; offline, made-up names)
   with a calendar charm ordered front + back. The Engraving window's "Edit charm" → a grid line hidden with the eye → Save:
     1  the page loads charm-nest-designs.js (before the bridge) and nothing on it errs
     2  the piece's save (Charm_Pool_Art) is also version 1 of its design in the library (Charm_Designs/pc-<poolId>): source piece, its SKU, its order, who, its size its
        cut line's, its faces whole (the hidden line not in what is made), its hash the cloud's canonical(); its picture a real PNG drawn on the page's canvas, in Storage
     3  the page's index has it at once (no list call), and its search finds it instantly; a second Save is version 2 of the same design
   PW_DIR=<playwright node_modules> CHROMIUM=<chrome> node tests/charm-nest/designs-page.cjs   (through scratchpad/int/slot.sh) */
'use strict';
const assert = require('node:assert/strict'), path = require('node:path'), crypto = require('node:crypto');
const ROOT = path.join(__dirname, '../..');
let n = 0; const ok = (c, m) => { assert.ok(c, m); n++; console.log('ok  ' + m); };
const settleP = page => page.evaluate(() => new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r))));

(async () => {
  // the calendar (as the shop draws its masters), made in node
  const hadWindow = 'window' in global, hadSelf = 'self' in global;
  global.window = global; global.self = global;
  global.PDFLib = global.PDFLib || require(path.join(ROOT, 'vendor/pdf-lib-1.17.1.min.js'));
  require(path.join(ROOT, 'charm-nest-pdf.js'));
  global.CharmNestGeom = global.CharmNestGeom || require(path.join(ROOT, 'charm-nest-geom.js'));
  const cal = await require('./_artCalendar.cjs')(ROOT);
  if (!hadWindow) delete global.window; if (!hadSelf) delete global.self;
  const D = require(path.join(ROOT, 'charm-nest-designs.js'));
  const R = require('./_engraveRealWorld.cjs'), c = cal.charm;
  R.DESIGNS.CALENDAR = { file: 'CALENDAR.ai', make: () => Buffer.from(cal.bytes), entry: { sku: 'CALENDAR', widthPt: +(c.bbox[2] - c.bbox[0]).toFixed(3), heightPt: +(c.bbox[3] - c.bbox[1]).toFixed(3), areaPt2: 4200, members: c.members.length, holes: 0, engravable: true, engravableBy: 'index', upAngle: 90, upSource: 'drawn', charmHash: 'calhash01', backKeepOut: [] } };
  R.ORDERS.cal = Object.assign({}, R.ORDERS.both, { rid: '4290000131', sku: 'CALENDAR', title: 'Calendar Necklace, Personalized Month Calendar Charm', buyer: 'Name Eleven', personalization: ['front - W\nback - Name One'] });
  R.EXPECT_JOBS.cal = 2;
  const w = await R.world({});
  if (!w) { console.log('SKIP no playwright-core'); return; }
  const page = w.page, st = w.srv.st, Q = '#egQueue .rvItem[data-kind=placement]';
  try {
    const html = require('node:fs').readFileSync(path.join(ROOT, 'charm-nest-1.html'), 'utf8');
    ok(/<script src="charm-nest-designs\.js\?v=[^"]+"><\/script>/.test(html) && html.indexOf('charm-nest-designs.js') < html.indexOf('charm-nest-bridge.js?v=') && await page.evaluate(() => typeof CharmNestDesigns.put === 'function' && typeof CharmNestDesigns.search === 'function'), '1 the page loads the design library (with a cache token, before the bridge)');
    const made = await R.build(w, ['cal']), PID = made.cal.poolIds[0], ID = 'pc-' + PID;
    ok(made.cal.jobs.length === 2, '1 the world: the calendar pooled, its front and back fitted');
    const listCalls = () => st.calls.filter(x => x.name === 'charmNestLibrary' && x.op === 'designList').length, l0 = listCalls();

    /* ── Edit charm → hide a grid line → Save ── */
    const edit = async () => {
      await R.openEngraveDetail(w, 'cal', { face: 'front' }); await R.settle(page, 800);
      await page.click(Q + ' .backHost .egCharmEdit');
      await page.waitForSelector('.ce-root .ce-items path', { timeout: 60000 }); await settleP(page);
      const line = await page.evaluate(() => { const it = CharmEditor.current.items().find(i => i.role === 'line' && i.source === 'master' && !i.hidden); return it ? String(it.id) : null; });
      await page.click(`.ce-root .ce-layer[data-id="${line}"] [data-lact="eye"]`); await settleP(page);
      await page.click('.ce-root [data-act="save"]');
      return line;
    };
    const line = await edit();
    for (let t0 = Date.now(); Date.now() - t0 < 90000 && !st.doc('Charm_Designs', ID); ) await new Promise(r => setTimeout(r, 200));
    const head = st.doc('Charm_Designs', ID), v1 = st.doc('Charm_Designs/' + ID + '/versions', '1'), art = st.doc('Charm_Pool_Art', PID + '~front');
    ok(art && art.removed.includes(line), '2 the piece\'s own save happened (Charm_Pool_Art: the hidden line removed)');
    ok(head && head.version === 1 && head.source.kind === 'piece' && head.source.poolId === PID && head.source.sku === 'CALENDAR' && head.orderId === made.cal.rid && head.by === 'Test Operator' && head.name === 'CALENDAR',
      '2 ... and version 1 of its design in the library: source piece, its SKU, its order, who: ' + JSON.stringify(head && { id: ID, v: head.version, source: head.source, orderId: head.orderId, by: head.by }));
    const design = { name: v1.name, faces: D.decodeFaces(v1.faces), widthMm: v1.widthMm, heightMm: v1.heightMm }, size = D.sizeOf(design.faces), L = D.leaves(design.faces);
    ok(v1.faces.front && v1.faces.back && Math.abs(size.widthMm - head.widthMm) < 1e-9 && Math.abs(size.heightMm - head.heightMm) < 1e-9 && head.widthMm > 5, `2 its faces whole (front and back), its size its cut line's: ${head.widthMm} × ${head.heightMm} mm`);
    ok(v1.faces.front.items.some(i => i.id === line && i.hidden) && !L.front.some(l => l.id === line), '2 the hidden line is kept as hidden, and is not in what is made');
    ok(head.hash === crypto.createHash('sha256').update(D.canonical(design)).digest('hex'), '2 its hash is canonical()\'s of what was kept (D6)');
    const blob = head.thumbPath && st.blobs.get(head.thumbPath), png = blob && blob.buf, pw = png && png.readUInt32BE(16), ph = png && png.readUInt32BE(20);
    if (process.env.SHOTS && png) { require('node:fs').mkdirSync(process.env.SHOTS, { recursive: true }); require('node:fs').writeFileSync(path.join(process.env.SHOTS, 'design-thumb.png'), png); }
    ok(png && png.readUInt32BE(0) === 0x89504e47 && pw > 32 && pw <= 192 && ph > 32 && ph <= 192 && head.thumbPath === 'charmnest/designs/thumbs/' + head.hash + '.png' && /^https:/.test(head.thumbUrl), `2 its picture: a PNG drawn on the page (${pw} × ${ph} px, ${png && png.length} bytes) in Storage, named by the hash`);

    const work = await page.evaluate(async id => {
      const d = await CharmNestDesigns.get(id, 1), D = CharmNestDesigns, ops = D.leaves(d.faces).front.reduce((k, l) => k + l.subpaths.reduce((a, sp) => a + sp.length, 0), 0), runs = [];
      for (let i = 0; i < 3; i++) { const t0 = performance.now(); D.clean(Object.assign({}, d), { stored: false }); const t1 = performance.now(), stats = {}; await D.thumbPng(d.faces, { stats }); runs.push([t1 - t0, stats.longest]); }
      return { ops, check: Math.min(...runs.map(r => r[0])), picture: Math.min(...runs.map(r => r[1])) };
    }, ID);
    ok(work.check + work.picture < 50, `2 the page's own work for a save (its check and its picture's longest slice) is ${work.check.toFixed(1)} + ${work.picture.toFixed(1)} ms of main thread for the calendar (${work.ops} path operators on its front), in a task of its own after the press`);

    const heavy = await page.evaluate(async () => {
      const D = CharmNestDesigns, W = 20 * 72 / 25.4, H = 15 * 72 / 25.4, rect = [['m', [0, 0]], ['l', [W, 0]], ['l', [W, H]], ['l', [0, H]], ['h']];
      const level = n => Array.from({ length: n }, (_, i) => { const x = 2 + (i % 50) * 1.1, y = 2 + Math.floor(i / 50) * 0.75 % 38; return [['m', [x, y]], ['c', [x + 0.3, y], [x + 0.6, y + 0.2], [x + 0.6, y + 0.5]], ['c', [x + 0.6, y + 0.7], [x + 0.2, y + 0.7], [x, y + 0.5]], ['c', [x - 0.1, y + 0.3], [x - 0.1, y + 0.1], [x, y]], ['h']]; });
      const make = n => ({ name: 'Traced', widthMm: 20, heightMm: 15, source: { kind: 'new' }, faces: { front: { face: 'front', items: [{ id: 'o', kind: 'path', role: 'cut', subpaths: [rect] }].concat([0.2, 0.5].map((g, k) => ({ id: 'lv' + k, kind: 'path', role: 'art', fill: [g, g, g], subpaths: level(n) }))) } } });
      const at = (fn) => { const r = []; for (let i = 0; i < 3; i++) { const t0 = performance.now(); fn(); r.push(performance.now() - t0); } return Math.min(...r); };
      const slice = async faces => { const r = []; for (let i = 0; i < 3; i++) { const stats = {}; await D.thumbPng(faces, { stats }); r.push(stats.longest); } return Math.min(...r); };
      const limit = make(600), photo = make(4000);   // (2 levels × 600 subpaths × 5 operators = 6,000: the most the page checks itself; 40,000: a traced photograph, checked by the cloud only)
      return { check: at(() => D.clean(limit, { stored: false })), pic: await slice(limit.faces), photoPic: await slice(photo.faces) };
    });
    ok(heavy.check + heavy.pic < 50 && heavy.photoPic < 50, `2 at the most the page checks itself (6,000 operators): ${heavy.check.toFixed(1)} ms + its picture ${heavy.pic.toFixed(1)} ms; a traced photograph's picture (40,000 operators) drawn in slices, the longest ${heavy.photoPic.toFixed(1)} ms`);

    /* ── the page's index ── */
    await page.waitForFunction(id => CharmNestDesigns.rows().some(r => r.id === id), ID, { timeout: 20000 });
    const ix = await page.evaluate(id => { const t0 = performance.now(), hits = CharmNestDesigns.search('calendar'), t1 = performance.now(), byOrder = CharmNestDesigns.search(CharmNestDesigns.rows()[0].orderId); return { first: CharmNestDesigns.rows()[0].id, hit: hits.map(h => h.id), ms: t1 - t0, byOrder: byOrder.map(h => h.id), thumb: CharmNestDesigns.rows()[0].thumbUrl }; }, ID);
    ok(ix.first === ID && ix.hit[0] === ID && ix.byOrder[0] === ID && ix.thumb === head.thumbUrl && listCalls() === l0, `3 the page's index has it at once, with its picture (no list call); search finds it by SKU and by order in ${ix.ms.toFixed(2)} ms`);
    await new Promise(r => setTimeout(r, 300));
    await edit();
    for (let t0 = Date.now(); Date.now() - t0 < 90000 && (st.doc('Charm_Designs', ID) || {}).version !== 2; ) await new Promise(r => setTimeout(r, 200));
    const head2 = st.doc('Charm_Designs', ID);
    ok(head2.version === 2 && st.doc('Charm_Designs/' + ID + '/versions', '1').hash === head.hash && head2.hash !== head.hash && head2.createdBy === 'Test Operator', '3 a second Save is version 2 of the same design (version 1 kept as it was)');
    ok(w.errors.length === 0, '1 no page error' + (w.errors.length ? ': ' + JSON.stringify(w.errors).slice(0, 300) : ''));
    console.log(`PASS ${n} checks: DESIGNS on the page — the Charm Editor's Save is a version of the piece's design, with its picture, found at once (${ix.ms.toFixed(2)} ms)`);
  } finally { await w.close(); }
})().catch(e => { console.error(e); process.exit(1); });
