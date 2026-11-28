'use strict';
/* CARDNAMES / FRONTPIC (Paul, 28 Nov 2026): every picture of a sheet must show exactly what the downloaded .ai shows. The .ai puts each SAVED front engraving ON its charm
   (charm-nest-export-ui.js build, charm-nest-export.js planFronts); a back goes above the sheet, never on it. The Library picture of GF_Nov.24.26_Set-1_Sheet-1 (d-2c660d6bd1c6bd63)
   had no "Waldo" on 4244028423_5307575879_1 (SKINNY_0191), GF_Nov.23.26_Set-1_Sheet-2 no "MSW": that picture is the cloud's preview.png (_charmNestFinishRender.js previewPng), which drew
   each charm with CharmNestPDF.drawCharm only. The page's own clean picture (charm-nest-1.html paintPreview, previewPng, Recall.repicture) had the same gap.
   The charm is a real master drawing (fixtures/TINY_BAR_7427.ai, a bar the shop engraves on its FRONT); every name is made up. Node only, no network.
     A  the cloud's picture: a saved front's words are inked on its charm where its record lays them (and nowhere else), absent without the record; a pending, an invalidated, an unsaved
        (no file), an artOnly front and a BACK are not drawn; newer pending / invalidated records beside the saved one change nothing
     B  fonts missing: the picture is still made (the charm without the words, byte for byte the picture without the record) and why is said once; previewFonts never throws
     C  the sheet writer hands the record's backPool to both pictures (the S6 write and the picture-only redraw) and reads their fonts first
     D  the page's clean picture (paintPreview, run in a vm on the same canvas) draws the very same pixels as the cloud's; its previewPng waits for the fonts; a font not loaded yet is
        asked for, the picture is still drawn, and the live Library pictures are drawn again when it lands
   node tests/charm-nest/rt1128-cardnames.cjs */
process.env.CHARM_NEST_CANVAS = process.env.CHARM_NEST_CANVAS || 'js';
const fs = require('fs'), path = require('path'), vm = require('vm');
const ROOT = path.join(__dirname, '..', '..');
let pass = 0, fail = 0; const ok = (c, m, d) => { if (c) { pass++; console.log('  ok  ' + m); } else { fail++; console.log('  FAIL ' + m + (d === undefined ? '' : ' :: ' + JSON.stringify(d).slice(0, 400))); } };
const Pdf = require(path.join(ROOT, 'netlify/functions/_charmNestPdf.js')); Pdf.withCanvas({ backend: 'js' });
const P = Pdf.CharmNestPDF, Geom = Pdf.Geom, Canvas = require(path.join(ROOT, 'netlify/functions/_charmNestCanvas.js'));
const R = require(path.join(ROOT, 'netlify/functions/_charmNestFinishRender.js'));
const Fonts = require(path.join(ROOT, 'netlify/functions/_charmEngraveFonts.js'));
const Backs = require(path.join(ROOT, 'charm-nest-backs.js'));
const O = require(path.join(ROOT, 'charm-nest-orders.js'));
const MM = 72 / 25.4, ST = { wPt: 100 * MM, hPt: 50 * MM }, T = Date.UTC(2026, 10, 24, 15, 0, 0);

(async () => {
  // ── the charm: TINY BAR 7427 as the page and the cloud make it (parse, group, trace) ──
  const parsed = await P.parseSource(new Uint8Array(fs.readFileSync(path.join(__dirname, 'fixtures/TINY_BAR_7427.ai'))), 'TINY_BAR_7427.ai');
  const gr = P.groupCharms(parsed, { minPt: 6 }); await P.buildSilhouettes(parsed, gr.charms, 6);
  const POOL = '4299000001_5399000001_1';
  const charm = Object.assign(gr.charms[0], { id: 'c-' + POOL, poolId: POOL, name: 'TINY BAR_7427', sku: 'TINY BAR_7427' });
  ok(!!charm.centerPt && !!charm.outline && charm.members.length > 0, 'the real master is a charm (outline, members, centre)');
  const bb = charm.outline.bbox, mid = [(bb[0] + bb[2]) / 2, (bb[1] + bb[3]) / 2];
  const place = { id: charm.id, cxPt: 50 * MM, cyPt: 25 * MM, angle: 30, scale: 0.971 };
  // the saved front (its file written), a pending one, an invalidated one, and others that are not on the sheet's picture
  const file = n => ({ ai: { path: `charmnest/backs/${POOL}/${n}.ai`, url: 'https://example.invalid/' + n + '.ai' }, png: { path: `charmnest/backs/${POOL}/${n}.png`, url: 'https://example.invalid/' + n + '.png' } });
  const saved = { poolId: POOL, face: 'front', order: '4299000001', sku: 'TINY BAR_7427', copy: 1, text: 'Waldo', lines: ['Waldo'], sizePt: 7, capMm: 1.7, weight: 'Regular', font: 'Source Sans 3',
    lineGap: 0.216, centre: [mid[0] - 4, mid[1]], angle: 0, upAngle: 90, approvedAt: T, approvedBy: 'Name One', outputs: file('front') };
  const pending = Object.assign({}, saved, { text: 'MSW', lines: ['MSW'], centre: [mid[0] + 12, mid[1]], approvedAt: T + 60000, pending: true, outputs: file('front-2') });
  const invalid = Object.assign({}, saved, { text: 'Gone', lines: ['Gone'], centre: [mid[0] + 10, mid[1] + 1], approvedAt: T + 120000, invalidated: true, outputs: file('front-3') });
  const noFile = Object.assign({}, saved, { text: 'Unsaved', lines: ['Unsaved'], approvedAt: T + 1000 }); delete noFile.outputs;
  const artOnly = Object.assign({}, saved, { text: 'Art', lines: ['Art'], artOnly: true });
  const back = Object.assign({}, saved, { text: 'Back words', lines: ['Back words'], outputs: file('back') }); delete back.face;
  const sheet = pool => ({ id: 'd-test-cardnames', metal: 'gold', charms: [charm], placements: [Object.assign({}, place)], backPool: pool });

  // where the words go: the record's glyphs (the window's engraveOn with the font the record names), through the charm's placement, in picture pixels
  const font = Fonts.load().Regular, k = 1026 / ST.wPt;
  const geo = Backs.engraveOn(saved, charm, { font, geom: Geom });
  ok(!!(geo && geo.glyphs && geo.glyphs.length), 'the record\'s words lay out on the charm (CharmNestBacks.engraveOn)');
  const a = place.angle * Math.PI / 180, ca = Math.cos(a), sa = Math.sin(a), s = place.scale;
  const toPx = (x, y) => { const u = (x - charm.centerPt[0]) * k * s, v = (charm.centerPt[1] - y) * k * s; return [place.cxPt * k + u * ca - v * sa, place.cyPt * k + u * sa + v * ca]; };
  let box = null; for (const g of geo.glyphs) for (const c of g.cmds) if (c.type !== 'Z') { const p = toPx(c.x, c.y); box = box ? [Math.min(box[0], p[0]), Math.min(box[1], p[1]), Math.max(box[2], p[0]), Math.max(box[3], p[1])] : [p[0], p[1], p[0], p[1]]; }
  const blue = png => {   // the words' ink (CharmNestBacks.INK, #1a5fc4) inside the words' box, and anywhere else
    const img = Canvas.decodePng(png), W = img.width, px = img.pixels; let inside = 0, outside = 0;
    for (let y = 0; y < img.height; y++) for (let x = 0; x < W; x++) { const i = (y * W + x) * 4, r = px[i], g = px[i + 1], b = px[i + 2]; if (!(b > r + 60 && b > g + 40)) continue;
      if (x >= box[0] - 3 && x <= box[2] + 3 && y >= box[1] - 3 && y <= box[3] + 3) inside++; else outside++; }
    return { inside, outside };
  };
  const pic = pool => R.previewPng(sheet(pool), ST, null);

  console.log('A · the cloud\'s picture (_charmNestFinishRender.js previewPng)');
  const plain = pic([]), withFront = pic([saved]);
  ok(Buffer.isBuffer(plain) && Buffer.isBuffer(withFront), 'both pictures are made');
  const inkPlain = blue(plain), inkFront = blue(withFront);
  ok(inkFront.inside > 60, 'the saved front\'s words are inked on its charm, where its record lays them', inkFront);
  ok(inkFront.outside === 0, 'and nowhere else on the sheet', inkFront);
  ok(inkPlain.inside === 0 && inkPlain.outside === 0, 'without the record the charm has no words', inkPlain);
  for (const [name, rec] of [['a pending front', pending], ['an invalidated front', invalid], ['a front whose file is not saved', noFile], ['an artOnly front (its parts are in the drawing)', artOnly], ['a BACK (above the sheet in the .ai, never on it)', back]]) {
    const png = pic([rec]), ink = blue(png);
    ok(ink.inside === 0 && ink.outside === 0 && Buffer.compare(png, plain) === 0, name + ' is not drawn: the picture is the one without words', ink);
  }
  ok(Buffer.compare(pic([invalid, saved, pending, back]), withFront) === 0, 'newer pending and invalidated fronts and a back beside the saved front change nothing: byte for byte the saved front\'s picture');
  const moved = Object.assign({}, saved, { centre: [mid[0] + 9, mid[1]] }), inkMoved = blue(pic([moved]));
  ok(inkMoved.inside < inkFront.inside && inkMoved.outside > 0, 'the words follow the record\'s centre (moved 9 pt along the bar, most of the ink leaves the old place)', inkMoved);

  console.log('B · fonts missing');
  const realSetOf = Fonts.setOf, warn = console.warn, said = [];
  console.warn = (...m) => { said.push(m.join(' ')); };
  try {
    Fonts.setOf = () => { throw new Error('Source Sans 3 is not available (vendor/fonts/SourceSans3-Regular.otf: ENOENT, test)'); };
    const p1 = pic([saved]), p2 = pic([saved]);
    ok(Buffer.isBuffer(p1) && Buffer.compare(p1, plain) === 0, 'Source Sans 3 unreadable: the picture is still made, the charm without the words');
    let threw = null; try { await R.previewFonts([saved]); await R.previewFonts(sheet([saved])); } catch (e) { threw = e; }
    ok(!threw, 'previewFonts never throws', threw && threw.message);
    const why = said.filter(t => /Source Sans 3 is not available/.test(t));
    ok(why.length === 1 && Buffer.compare(p2, plain) === 0, 'why is said once (two pictures, two font reads)', said);
    said.length = 0;
    Fonts.setOf = () => null;
    const playwrite = Object.assign({}, saved, { font: 'Playwrite US Trad', fontKey: (O.ENGRAVING_FONTS.find(f => f.id !== O.DEFAULT_FONT) || {}).id || 'playwrite-us-trad' });
    const q1 = pic([playwrite]), q2 = pic([playwrite]);
    ok(Buffer.isBuffer(q1) && Buffer.compare(q1, plain) === 0 && Buffer.compare(q2, plain) === 0, 'the record\'s own font unreadable: the picture is still made, the charm without the words');
    ok(said.filter(t => /could not be read/.test(t)).length === 1, 'and why is said once', said);
  } finally { Fonts.setOf = realSetOf; console.warn = warn; }
  ok(Buffer.compare(pic([saved]), withFront) === 0, 'with the fonts back the words are drawn again');

  console.log('C · the sheet writer hands the record\'s backPool to both pictures');
  const fin = fs.readFileSync(path.join(ROOT, 'netlify/functions/_charmNestFinish.js'), 'utf8');
  const calls = fin.match(/X\.Render\.previewPng\([^\n]*\n?/g) || [];
  ok(calls.length === 2 && calls.every(c => /backPool \}\)/.test(c)), 'both previewPng calls carry backPool', calls.map(c => c.slice(0, 120)));
  ok(/const backPool = rec\.backPool \|\| \[\];[^\n]*\n\s*await X\.Render\.previewFonts\(backPool\);\n\s*const png = X\.Render\.previewPng\(/.test(fin), 'the picture-only redraw (SHIPPEDSTAYS) takes the record\'s backPool and reads its fonts first');
  ok(/const backPool = \(X\.rec \|\| sh\)\.backPool \|\| \[\];[^\n]*\n\s*await X\.Render\.previewFonts\(backPool\);\n\s*const png = X\.Render\.previewPng\(/.test(fin), 'the S6 picture takes the record\'s backPool (sheetOf\'s copy holds none) and reads its fonts first');

  console.log('D · the page\'s clean picture (charm-nest-1.html paintPreview) draws the same');
  const html = fs.readFileSync(path.join(ROOT, 'charm-nest-1.html'), 'utf8');
  const from = html.indexOf('function shippedOf(sh)'), to = html.indexOf('/* ═══ 12b · a card turned over');
  ok(from > 0 && to > from, 'paintPreview is found in the page');
  const asked = [];
  const E = { fonts: Fonts.load(), fontSetFor: id => (id === O.DEFAULT_FONT ? Fonts.load() : Fonts.setOf(id)), loadFonts: async () => { asked.push('source-sans-3'); }, loadFontSet: async id => { asked.push(id); }, ensureScriptFonts: async () => [] };
  const ctx = vm.createContext({ console, Math, Map, Set, JSON, Promise, Date, String, Number, Array, Object, Error, setTimeout, clearTimeout, devicePixelRatio: 1,
    S: { settings: { insetPt: 1.42 }, library: { rows: [] } }, stockFor: () => ST, CharmNestPDF: P, CharmNestGeom: Geom, CharmNestOrders: O, Engrave: E });
  ctx.window = ctx; ctx.self = ctx;
  let page = null;
  try { vm.runInContext(fs.readFileSync(path.join(ROOT, 'charm-nest-backs.js'), 'utf8'), ctx, { filename: 'charm-nest-backs.js' });   // (as the page loads it: its CharmNestGeom is the page's)
    vm.runInContext(html.slice(from, to) + '\nthis.__paint = paintPreview; this.__front = typeof FrontPic === "undefined" ? null : FrontPic;', ctx, { filename: 'charm-nest-1.html (paintPreview)' }); page = ctx; } catch (e) { ok(false, 'the page\'s picture code runs in node', e.message); }
  const pagePic = pool => { const cv = new Canvas.NodeCanvas(1026, Math.round(1026 * ST.hPt / ST.wPt)); ctx.__paint(cv, sheet(pool), true); return cv.toBuffer(); };
  if (page) {
    const pFront = pagePic([saved]), pPlain = pagePic([]);
    ok(blue(pFront).inside > 60, 'the page\'s files picture inks the saved front\'s words on its charm', blue(pFront));
    ok(Buffer.compare(pFront, withFront) === 0, 'pixel for pixel the cloud\'s picture (so the Library shows one picture whichever drew it)');
    ok(Buffer.compare(pPlain, plain) === 0, 'and without a front the two pictures are as before, the same');
    ok(Buffer.compare(pagePic([invalid, saved, pending, noFile, artOnly, back]), withFront) === 0, 'pending, invalidated, unsaved, artOnly fronts and backs are left out on the page too');
    // a font not loaded yet: the picture is drawn without the words, the font is asked for once, and the live pictures' signature moves when it lands
    const F = ctx.__front, before = F ? F.landed() : 0;
    E.fonts = {};
    const pNoFont = pagePic([saved]); pagePic([saved]);
    ok(Buffer.compare(pNoFont, plain) === 0, 'a font not loaded yet: the page\'s picture is still drawn, the charm without the words');
    await new Promise(r => setTimeout(r, 20));
    ok(asked.filter(x => x === 'source-sans-3').length === 1 && !!F && F.landed() === before + 1, 'the font is asked for once and its landing is counted (the live pictures are drawn again)', { asked, landed: F && F.landed(), before });
    E.fonts = Fonts.load();
  }
  ok(/async function previewPng\(sh\) \{\s*try \{ await FrontPic\.ready\(sh\);/.test(html), 'the page\'s previewPng (finishNest, Recall.repicture) waits for the fronts\' fonts before it draws');
  ok(/\[\[\.\.\.FrontPic\.of\(sh\)\.keys\(\)\], FrontPic\.landed\(\)\]/.test(html), 'the live Library pictures are drawn again when a front is saved or its font lands');

  console.log(`\nrt1128-cardnames: ${pass} passed, ${fail} failed`); process.exit(fail ? 1 : 0);
})().catch(e => { console.log('FAIL rt1128-cardnames: ' + ((e && e.stack) || e)); process.exit(1); });
