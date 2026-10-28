// AN IMAGE ON A CHARM (Design & Engrave, item 11; Paul, 28 Oct 2026: "I also need the ability for users to import images into this environment, and to manipulate the image and place it on a
// charm. All images must be converted to a vector format while keeping the quality of the image and the colour should be a greyscale using the black as base").
// charm-nest-ce-image.js, the Charm Editor's importer for PNG / JPG / WebP, and the greys kept to the laser's file (artGrey: charm-nest-art-edit.js, charmNestLibrary.js artPut, charm-nest-pdf.js).
//   A  (node) the pure parts: a turn and a flip carry the crop with them; the prep (auto-levels, gamma, invert, median, unsharp, the line-art threshold under uneven light); the suggested
//      level count; the box inside an outline that keeps off a hole; the trace laid out as one group of greys; and the record: encode → apply keeps a grey part grey (fillRGB, no hatchBlue),
//      artOps writes its grey, decode and itemsOf give it back, while a hatched part with no mark is blue exactly as before
//   B  the real sorter page over the repo's fake site (_engraveRealWorld.cjs, offline, made-up names) with the calendar charm (_artCalendar.cjs): Edit charm; a PNG made in the page DROPPED on the
//      work area opens the Image step (split preview: original | greyscale | vector); its suggestion, levels, tone, turn, flip, crop, background removal, line art and the "more" filters,
//      each change measured in main-thread ms; Place → one group of grey paths fitted inside the outline; moved, sized and turned as any group; Re-trace (the Object menu) with 2 levels where it
//      stands, one undo step; a second PNG PASTED with a day square selected goes into that square; Save → the Charm_Pool_Art record keeps every grey (artGrey) through the real artPut; the
//      sheet's file (CharmNestPDF.buildSheet) fills each grey as its own colour while the month's letters stay blue; opened again, the greys come back grey.
//   The trace is the real one (charm-nest-ce-trace.js, CharmNestTrace): which tone takes which band is its choice (multi-level Otsu), so the greys are checked as k/N, black the base.
//   PW_DIR=<playwright node_modules> CHROMIUM=<chrome> node tests/charm-nest/ce-image.cjs      (CEI_ONLY=A or B; SHOTS=<dir>)
'use strict';
const path = require('node:path');
const ROOT = path.join(__dirname, '../..');
const RESULTS = [];
const check = (id, ok, evidence) => { RESULTS.push({ id, ok: !!ok }); console.log(`${ok ? 'PASS' : 'FAIL'}  ${id}  ${ok ? '' : evidence || ''}`.trimEnd()); };
const ONLY = process.env.CEI_ONLY || '';
const PT = 72 / 25.4;

async function nodeKit() {
  const hadWindow = 'window' in global, hadSelf = 'self' in global;
  global.window = global; global.self = global;
  global.PDFLib = global.PDFLib || require(path.join(ROOT, 'vendor/pdf-lib-1.17.1.min.js'));
  require(path.join(ROOT, 'charm-nest-pdf.js'));
  global.CharmNestGeom = global.CharmNestGeom || require(path.join(ROOT, 'charm-nest-geom.js'));
  const cal = await require('./_artCalendar.cjs')(ROOT);
  const kit = { P: global.CharmNestPDF, G: global.CharmNestGeom, L: global.PDFLib, AE: require(path.join(ROOT, 'charm-nest-art-edit.js')), CE: require(path.join(ROOT, 'charm-nest-charm-editor.js')),
    IM: require(path.join(ROOT, 'charm-nest-ce-image.js')), T: require(path.join(ROOT, 'charm-nest-ce-trace.js')), cal };
  if (!hadWindow) delete global.window; if (!hadSelf) delete global.self;
  return kit;
}

/* ═══ A · the pure parts ═══ */
async function partA(kit) {
  const { IM, AE, CE, G, P, T, cal } = kit, M = CE.model;
  const kOfN = (gs, N) => gs.length > 0 && gs.every(g => Math.abs(g * N - Math.round(g * N)) < 0.002) && new Set(gs.map(g => Math.round(g * N))).size === gs.length;
  // turns and flips: the crop follows the picture; four turns are none; a flip then a turn is the turn then the other flip
  const o0 = IM.norm({ crop: [0.1, 0.2, 0.5, 0.9] });
  const t4 = [1, 2, 3, 4].reduce(o => IM.turn(o, 1), o0), tl = IM.turn(IM.turn(o0, 1), -1), fh = IM.turn(IM.flip(o0, true), 1), hf = IM.flip(IM.turn(o0, 1), false);
  const same = (a, b) => a.rot === b.rot && a.flipH === b.flipH && a.flipV === b.flipV && JSON.stringify(a.crop.map(v => +v.toFixed(9))) === JSON.stringify(b.crop.map(v => +v.toFixed(9)));
  // and the pixel a crop corner names is the same pixel after a turn (the shown matrix maps it back to the source)
  const corner = o => { const [sw, sh] = IM.shownSize(200, 100, o), m = IM.shownMatrix(200, 100, o), inv = [m[3], -m[1], -m[2], m[0]].map(v => v / (m[0] * m[3] - m[1] * m[2])); const x = o.crop[0] * sw - m[4], y = o.crop[1] * sh - m[5]; return [inv[0] * x + inv[2] * y, inv[1] * x + inv[3] * y].map(v => +v.toFixed(6)); };
  const c0 = corner(o0), c1 = corner(IM.turn(o0, 1));
  check('A1 a quarter turn carries the crop with the picture (four are none, left undoes right), a flip before a turn is the other flip after it, and the crop still names the same pixels',
    same(t4, o0) && same(tl, o0) && same(fh, hf) && IM.turn(o0, 1).rot === 90 && JSON.stringify(c0) === JSON.stringify([20, 20]) && Math.abs(c1[0] - 20) < 1e-6 && Math.abs(c1[1] - 90) < 1e-6,
    JSON.stringify({ t4, fh, hf, c0, c1 }));

  // the prep: a dull ramp (100..156) stretched by auto-levels to black..white; gamma lifts the middle; invert turns it over
  const img = (w, h, f) => { const d = new Uint8ClampedArray(w * h * 4); for (let y = 0; y < h; y++) for (let x = 0; x < w; x++) { const v = f(x, y), p = (y * w + x) * 4; d[p] = d[p + 1] = d[p + 2] = v; d[p + 3] = 255; } return d; };
  const ramp = img(256, 4, x => 100 + Math.round(x * 56 / 255));
  const off = IM.prepGrey(ramp, 256, 4, IM.norm({ auto: false })).g, on = IM.prepGrey(ramp, 256, 4, IM.norm({})).g, gam = IM.prepGrey(ramp, 256, 4, IM.norm({ gamma: 2 })).g, invd = IM.prepGrey(ramp, 256, 4, IM.norm({ invert: true })).g;
  check(`A2 auto-levels stretch a dull ramp (${off[0]}..${off[255]}) to ${on[0]}..${on[255]}; gamma 2 lifts its middle (${on[128]} → ${gam[128]}); invert turns it over`,
    off[0] === 100 && off[255] === 156 && on[0] <= 3 && on[255] >= 252 && gam[128] > on[128] + 30 && Math.abs(invd[0] - (255 - on[0])) <= 1 && Math.abs(invd[255] - (255 - on[255])) <= 1, '');
  // the median takes a lone speck out; the unsharp mask steepens an edge
  const speck = img(9, 9, (x, y) => (x === 4 && y === 4 ? 0 : 230)), edge = img(20, 1, x => (x < 10 ? 80 : 180));
  const sp0 = IM.prepGrey(speck, 9, 9, IM.norm({ auto: false })).g[40], sp1 = IM.prepGrey(speck, 9, 9, IM.norm({ auto: false, denoise: true })).g[40];
  const e0 = IM.prepGrey(edge, 20, 1, IM.norm({ auto: false })).g, e1 = IM.prepGrey(edge, 20, 1, IM.norm({ auto: false, sharpen: 0.5 })).g;
  check(`A3 denoise (median 3×3) takes out a lone speck (${sp0} → ${sp1}); sharpen (unsharp mask) steepens an edge (${e0[9]}|${e0[10]} → ${e1[9]}|${e1[10]})`, sp0 === 0 && sp1 === 230 && e1[9] < e0[9] - 10 && e1[10] > e0[10] + 10, '');
  // LINE ART: handwriting photographed under a light falling off from left to right (paper 235 → 120, ink 30 below its paper): a global threshold blackens the dim paper, the local one does not
  const W = 240, H = 80, paper = x => Math.round(235 - 115 * x / W), stroke = (x, y) => (Math.abs(y - 40 - 12 * Math.sin(x / 9)) < 2.2 && x > 10 && x < W - 10);
  const hand = img(W, H, (x, y) => (stroke(x, y) ? paper(x) - 70 : paper(x)));
  const glob = IM.prepGrey(hand, W, H, IM.norm({})).g, loc = IM.prepGrey(hand, W, H, IM.norm({ mode: 'line' })).g;
  let inkTrue = 0, inkLoc = 0, falseGlob = 0, falseLoc = 0;
  for (let y = 0; y < H; y++) for (let x = 0; x < W; x++) { const i = y * W + x; if (stroke(x, y)) { inkTrue++; if (loc[i] === 0) inkLoc++; } else { if (glob[i] < 128) falseGlob++; if (loc[i] === 0) falseLoc++; } }
  check(`A4 line art (local-mean threshold) under uneven light: ${(100 * inkLoc / inkTrue).toFixed(0)}% of the strokes inked, ${falseLoc} paper pixels inked (a global middle threshold inks ${falseGlob})`,
    inkLoc / inkTrue > 0.9 && falseLoc < W * H * 0.01 && falseGlob > W * H * 0.2 && new Set(loc).size === 2, '');
  // the suggestion: line art one level, a flat three-tone drawing three, a photo-like spread five
  const st = d => IM.prepGrey(d, 64, 64, IM.norm({}));
  let seed = 7; const rnd = () => (seed = (seed * 16807) % 2147483647) / 2147483647;
  const s1 = IM.suggestLevels(st(img(64, 64, (x, y) => (Math.abs(x - y) < 3 ? 0 : 255)))), s3 = IM.suggestLevels(st(img(64, 64, x => (x < 16 ? 0 : x < 32 ? 110 : x < 48 ? 190 : 255))));
  const s5 = IM.suggestLevels(st(img(64, 64, (x, y) => Math.max(0, Math.min(255, Math.round(128 + 90 * Math.sin(x / 7) * Math.cos(y / 9) + 40 * (rnd() - 0.5)))))));
  check(`A5 the suggested levels: line art ${s1.levels} (${s1.kind}), a three-tone drawing ${s3.levels} (${s3.kind}), a photo ${s5.levels} (${s5.kind})`, s1.levels === 1 && s3.levels === 3 && s5.levels === 5, JSON.stringify({ s1, s3, s5 }));

  // where it goes: a round charm (r 30 pt) with a hanging hole near its top; the box is square, inside the circle by the margin, and clear of the hole
  const circle = (cx, cy, r, n) => { const p = []; for (let i = 0; i < n; i++) p.push(cx + r * Math.cos(2 * Math.PI * i / n), cy + r * Math.sin(2 * Math.PI * i / n)); return p; };
  const outer = [circle(50, 50, 30, 180)], hole = [circle(50, 26, 3, 48)], m = 0.4 * PT;
  const b = IM.fitInOutline(1, outer, hole, m);
  const corners = b && [[b[0], b[1]], [b[2], b[1]], [b[2], b[3]], [b[0], b[3]]];
  const inCircle = corners && corners.every(([x, y]) => Math.hypot(x - 50, y - 50) <= 30 - m + 1e-6), clearHole = b && b[1] > 29 + m - 1e-6;
  check(`A6 fitted inside the outline: a ${(b[2] - b[0]).toFixed(1)} × ${(b[3] - b[1]).toFixed(1)} pt box in a 60 pt circle, every corner ${m.toFixed(2)} pt inside it and clear of the hanging hole`,
    !!b && Math.abs((b[2] - b[0]) - (b[3] - b[1])) < 1e-6 && inCircle && clearHole && b[2] - b[0] > 30, JSON.stringify(b));

  // the trace laid out as one group: lightest at the bottom, black on top, fills [g, g, g], hatch, artGrey, even-odd; mapped into the box
  const pic = { width: 40, height: 20, data: img(40, 20, x => (x < 10 ? 0 : x < 20 ? 128 : x < 30 ? 200 : 255)) };
  const tr = T.traceSync(pic, IM.traceOpts(IM.norm({ levels: 4 }), 20)), g = IM.buildGroup(tr, [10, 10, 50, 30], { src: 'imgTest', settings: { levels: 4 }, name: 'logo.png' });
  const fl = g.children.map(c => c.fill[0]);
  const gb = M.bboxOf([].concat(...g.children.map(c => c.subpaths)));
  check('A7 the trace becomes ONE group: a path per grey (lightest first, black on top), role hatch, fill [g,g,g], artGrey, even-odd, laid into the box; the group keeps its source and settings (trace)',
    g.kind === 'group' && g.id === 'imgTest' && g.trace.src === 'imgTest' && g.trace.levels === 4 && fl.length === 3 && kOfN(fl, 4) && fl.every((v, i) => !i || v < fl[i - 1]) && g.children.every(c => c.role === 'hatch' && c.artGrey && c.even && c.fill.length === 3 && c.fill[0] === c.fill[2])
      && g.children[2].name === 'Black' && g.trace.box.join() === '10,10,50,30' && Math.abs(gb[0] - 10) < 1e-6 && Math.abs(gb[2] - 40) < 1e-6 && Math.abs(gb[1] - 10) < 1e-6 && Math.abs(gb[3] - 30) < 1e-6,
    JSON.stringify({ fills: g.children.map(c => c.fill), gb, names: g.children.map(c => c.name) }));

  // the record: the group as the editor hands it back, through encode → apply → artOps, on the calendar
  const cb = cal.charm, view = G.backView(cb, { face: 'front', upAngle: 75, res: 6 }), geo = M.createGeo(null);
  const items = AE.itemsOf(cb, view, 'front'), fr = M.bboxOf([].concat(...items.filter(i => i.role === 'cut').map(i => i.subpaths)));
  const cx = (fr[0] + fr[2]) / 2, cy = (fr[1] + fr[3]) / 2;
  const grp = IM.buildGroup(tr, [cx - 15, cy - 7.5, cx + 15, cy + 7.5], { src: 'imgRec', name: 'logo.png' });
  const words = { id: 'w1', kind: 'path', role: 'hatch', source: 'added', fill: true, subpaths: [[['m', [cx - 2, cy - 20]], ['l', [cx + 2, cy - 20]], ['l', [cx + 2, cy - 16]], ['l', [cx - 2, cy - 16]], ['h']]] };
  const out = M.exportItems(M.prepIn(items, ['cut']).concat([grp, words]), geo), diff = M.diffFace(M.exportItems(M.prepIn(items, ['cut']), geo), out);
  const rec = AE.encode(diff, { charm: cb, view, face: 'front', placed: true });
  const greyRec = rec.added.filter(e => e.artGrey), ed = AE.apply(cb, Object.assign({ at: 1, by: 'Test' }, rec), 'front');
  const segs = ed.members.filter(s => s.artGrey), plain = ed.members.find(s => s.artMemberId === 'w1');
  const ops = P.artOps(ed.members, null);
  check(`A8 encode keeps each grey part grey (${greyRec.map(e => e.fill[0]).join(', ')}: artGrey, even-odd) and apply makes it a grey FILL (fillRGB, no blue hatching), which artOps writes as its grey; a hatched part with no mark is blue as before`,
    greyRec.length === 3 && greyRec.every(e => e.role === 'hatch' && e.even && e.fill[0] === e.fill[1] && e.fill[1] === e.fill[2] && e.group === 'imgRec') && segs.length === 3 && segs.every(s => !s.hatchBlue && s.fill && !s.stroke && s.fillRGB[0] === s.fillRGB[2] && s.manufacturingRole === 'hatch' && /f\*$/.test(s.paintOp))
      && greyRec.every(e => { const v = String(Math.round(e.fill[0] * 1000) / 1000); return ops.includes(`${v} ${v} ${v} rg`); }) && !!plain && plain.hatchBlue && !plain.artGrey && rec.added.find(e => e.id === 'w1').fill.join() === '0,0,1' && /q 0 0 1 rg/.test(ops),
    JSON.stringify({ greyRec: greyRec.map(e => [e.id, e.fill, e.artGrey]), segs: segs.map(s => [s.artMemberId, s.fillRGB, s.hatchBlue]), plain: plain && [plain.hatchBlue, plain.fillRGB] }));
  const back = AE.decode(Object.assign({ at: 1 }, rec), cb, view), shown = AE.itemsOf(ed, view, 'front').filter(i => i.artGrey);
  check('A9 opened again the greys come back grey: decode and itemsOf give each part its grey and its mark (and the group\'s id, for Re-trace)',
    back.added.filter(i => i.artGrey).length === 3 && shown.length === 3 && shown.every(i => i.fill[0] === i.fill[2] && i.group === 'imgRec' && i.role === 'hatch'), JSON.stringify(shown.map(i => [i.id, i.fill, i.group])));
  // served: the page loads it after the editor (with a cache token) and the build ships it
  const fs = require('node:fs'), html = fs.readFileSync(path.join(ROOT, 'charm-nest-1.html'), 'utf8'), build = fs.readFileSync(path.join(ROOT, 'scripts/build-public.cjs'), 'utf8');
  const at = html.indexOf('src="charm-nest-ce-image.js?v='), edAt = html.indexOf('src="charm-nest-charm-editor.js?v=');
  check('A10 the page loads charm-nest-ce-image.js after the editor, with a ?v= token, and scripts/build-public.cjs ships it', at > edAt && edAt > 0 && /"charm-nest-ce-image\.js"/.test(build), JSON.stringify({ at, edAt }));
}

/* ═══ B · the real page ═══ */
const settleP = page => page.evaluate(() => new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r))));
const C = (page, fn, arg) => page.evaluate(fn, arg);
async function typeField(page, f, value) { const el = page.locator(`.ce-root [data-f="${f}"]`); await el.click(); await page.keyboard.press('Control+a'); await page.keyboard.type(String(value)); await page.keyboard.press('Enter'); await settleP(page); }
/** Waits until the Image step's preview trace has caught up (nothing running and the newest drawn). */
const previewDone = page => page.waitForFunction(() => { const c = CharmNestCeImage.current(); return c && !c.working() && document.querySelector('.cei [data-wait]').hidden; }, null, { timeout: 30000 });
const vecGreys = page => C(page, () => [...document.querySelectorAll('.cei__vec path')].map(p => p.getAttribute('fill')));
/** 'rgb(a,a,a)' or 'rgb(a, a, a)' → a / 255; and whether greys are the trace's fixed k / N (the stand-in's and CharmNestTrace's alike: which tone takes which band is the trace's choice). */
const g01 = c => +String(c).match(/\d+/)[0] / 255;
const kOfN = (gs, N) => gs.length > 0 && gs.every(g => Math.abs(g * N - Math.round(g * N)) < 0.02) && new Set(gs.map(g => Math.round(g * N))).size === gs.length;
const css = g => `rgb(${Math.round(g * 255)}, ${Math.round(g * 255)}, ${Math.round(g * 255)})`;

async function partB(kit) {
  const R = require('./_engraveRealWorld.cjs');
  const shots = process.env.SHOTS || '/tmp/ce-image-shots';
  const c = kit.cal.charm;
  R.DESIGNS.CALENDAR = { file: 'CALENDAR.ai', make: () => Buffer.from(kit.cal.bytes), entry: { sku: 'CALENDAR', widthPt: +(c.bbox[2] - c.bbox[0]).toFixed(3), heightPt: +(c.bbox[3] - c.bbox[1]).toFixed(3), areaPt2: 4200, members: c.members.length, holes: 0, engravable: true, engravableBy: 'index', updatedAt: 1 } };
  R.ORDERS.cal = Object.assign({}, R.ORDERS.both, { rid: '4290000131', sku: 'CALENDAR', title: 'Calendar Necklace, Personalized Month Calendar Charm', buyer: 'Name Eleven', personalization: ['front - W\nback - Name One'] });
  R.EXPECT_JOBS.cal = 2;
  const w = await R.world({ shots });
  if (!w) { console.log('SKIP no playwright-core'); return; }
  const page = w.page, st = w.srv.st, Q = '#egQueue .rvItem[data-kind=placement]';
  try {
    const made = await R.build(w, ['cal']);
    const PID = made.cal.poolIds[0];
    check('B0 the world: the calendar is pooled and its faces fitted', made.cal.jobs.length === 2, JSON.stringify(made.cal.jobs.map(j => [j.face, j.state])));
    // the employee log (charm-nest-design-log.js, another workstream's): what this page is told, kept to read back
    await page.evaluate(() => { window.__ceiLog = []; const was = window.CNDesignLog; window.CNDesignLog = { event: (k, d) => { window.__ceiLog.push([k, d]); if (was && was.event) try { was.event(k, d); } catch (e) { /* theirs */ } } }; });
    // two pictures made in the page: a logo (black disc, a mid-grey square, a light-grey band, on white) and a small badge
    await page.evaluate(() => {
      const mk = (w, h, draw, name) => new Promise(res => { const cv = document.createElement('canvas'); cv.width = w; cv.height = h; const g = cv.getContext('2d'); g.fillStyle = '#fff'; g.fillRect(0, 0, w, h); draw(g); cv.toBlob(b => res(new File([b], name, { type: 'image/png' })), 'image/png'); });
      window.__cei = {};
      return Promise.all([
        mk(240, 160, g => { g.fillStyle = '#000'; g.beginPath(); g.arc(70, 80, 45, 0, 7); g.fill(); g.fillStyle = 'rgb(128,128,128)'; g.fillRect(140, 40, 70, 70); g.fillStyle = 'rgb(200,200,200)'; g.fillRect(20, 132, 200, 18); }, 'logo.png').then(f => { __cei.logo = f; }),
        mk(60, 60, g => { g.fillStyle = '#000'; g.fillRect(10, 10, 40, 40); g.fillStyle = 'rgb(128,128,128)'; g.fillRect(20, 20, 20, 20); }, 'image.png').then(f => { __cei.badge = f; })
      ]);
    });
    await R.openEngraveDetail(w, 'cal', { face: 'front' }); await R.settle(page, 700);
    await page.click(Q + ' .backHost .egCharmEdit');
    await page.waitForSelector('.ce-root .ce-items path', { timeout: 60000 }); await settleP(page); await settleP(page);
    const head = await page.evaluate(() => { const b = document.querySelector('.ce-root [data-act="import"]'); return { has: !!b && !b.hidden, title: b && b.title, accept: (CharmEditor.importers || []).map(i => i.id + ':' + i.accept.join(' ')), trace: !!(window.CharmNestTrace && CharmNestTrace.posterise && CharmNestTrace.trace) }; });
    const mine = head.accept.find(a => a.startsWith('image:')) || '';
    check(`B1 the editor's Import takes pictures (${mine}: png, jpeg and webp named, never image/svg+xml, which is the drawing importer's), on a page with CharmNestTrace`,
      head.has && /\.png/.test(head.title) && /^image:\.png \.jpg \.jpeg \.webp image\/png image\/jpeg image\/webp$/.test(mine) && !/svg|image\/ /.test(mine + ' ') && head.trace, JSON.stringify(head));

    /* ── DROP: the logo let go on the work area ── */
    const n0 = await page.evaluate(() => CharmEditor.current.items().length);
    await page.evaluate(() => {
      const st = document.querySelector('.ce-root .ce-stage'), r = st.getBoundingClientRect(), dt = new DataTransfer(); dt.items.add(__cei.logo);
      const at = { bubbles: true, cancelable: true, clientX: r.left + r.width / 2, clientY: r.top + r.height / 2, dataTransfer: dt };
      st.dispatchEvent(new DragEvent('dragenter', at)); st.dispatchEvent(new DragEvent('dragover', at)); st.dispatchEvent(new DragEvent('drop', at));
    });
    await page.waitForSelector('.ce-root .cei .cei__card', { timeout: 20000 }); await previewDone(page); await settleP(page);
    const s0 = await page.evaluate(() => {
      const ink = cv => { const d = cv.getContext('2d').getImageData(0, 0, cv.width, cv.height).data; let n = 0; for (let i = 0; i < d.length; i += 4) if (d[i] < 128) n++; return n; };
      const cO = document.querySelector('.cei__orig'), cG = document.querySelector('.cei__grey'), sheet = document.querySelector('.cei__sheet').getBoundingClientRect();
      const labs = [...document.querySelectorAll('.cei__lab')].filter(l => !l.hidden).map(l => l.textContent);
      return { name: document.querySelector('.cei [data-name]').textContent, mode: document.querySelector('.cei__sheet').dataset.mode, labs, inkO: ink(cO), inkG: ink(cG), ar: +(sheet.width / sheet.height).toFixed(2),
        where: document.querySelector('.cei [data-where]').textContent, note: document.querySelector('.cei [data-note]').textContent, sug: document.querySelector('.cei [data-sug]').textContent, items: CharmEditor.current.items().length };
    });
    const g0 = await vecGreys(page);
    await R.shot(w, 'CEI-1-image-step');
    check(`B2 a PNG dropped on the work area opens the Image step on it (${s0.name}): a split preview, ${s0.labs.join(' | ')}, the vector traced in the background (${g0.length} greys: ${g0.join(' ')}), nothing on the charm yet`,
      /^logo\.png · 240 × 160 px$/.test(s0.name) && s0.mode === 'split' && s0.labs.join() === 'Original,Greyscale,Vector' && s0.inkO > 1000 && s0.inkG > 1000 && Math.abs(s0.ar - 1.5) < 0.03
        && g0.length >= 3 && g0.length <= 4 && kOfN(g0.map(g01), 4) && g0[g0.length - 1] === 'rgb(0,0,0)' && s0.items === n0, JSON.stringify(Object.assign(s0, { g0 })));
    check(`B3 the panel says plainly what a photo becomes ("${s0.note.slice(0, 92)}…"), offers the picture's own level count ("${s0.sug.trim()}") and where it will go (${s0.where})`,
      /posterised, stencil-like engraving in 4 greys/.test(s0.note) && /Looks like an illustration: \d greys suggested/.test(s0.sug) && /centred in the charm's outline/.test(s0.where), JSON.stringify(s0));

    /* ── adjusting: each change drawn at once (main-thread ms measured), the vector following ── */
    // the suggestion's Use (when it suggests other than the 4 in hand), else the slider one step down: 3 levels; then back to 4
    const useN = await page.evaluate(() => { const b = document.querySelector('.cei [data-do="usesug"]'); return b ? +b.textContent.replace(/\D/g, '') : 0; });
    if (useN === 3) await page.click('.cei [data-do="usesug"]'); else { await page.focus('.cei [data-k="levels"]'); await page.keyboard.press('ArrowLeft'); }
    await previewDone(page);
    const g3 = await vecGreys(page), lv3 = await page.evaluate(() => document.querySelector('.cei [data-k="levels"]').value);
    await page.focus('.cei [data-k="levels"]'); await page.keyboard.press('ArrowRight'); await previewDone(page);
    const g4 = await vecGreys(page);
    check(`B4 ${useN === 3 ? '"Use 3"' : 'the Levels slider'} sets 3 levels (the vector: ${g3.join(' ')}, thirds), and back to 4 (${g4.length} greys again, quarters): the trace's fixed k/N, black the base`,
      lv3 === '3' && kOfN(g3.map(g01), 3) && g3.includes('rgb(0,0,0)') && g4.length === g0.length && kOfN(g4.map(g01), 4), JSON.stringify({ useN, g3, g4 }));
    await page.focus('.cei [data-k="brightness"]'); for (let i = 0; i < 8; i++) await page.keyboard.press('ArrowRight');
    for (let i = 0; i < 8; i++) await page.keyboard.press('ArrowLeft');
    await page.focus('.cei [data-k="contrast"]'); for (let i = 0; i < 6; i++) await page.keyboard.press('ArrowRight'); for (let i = 0; i < 6; i++) await page.keyboard.press('ArrowLeft');
    await page.click('.cei [data-do="more"]');
    await page.focus('.cei [data-k="gamma"]'); for (let i = 0; i < 6; i++) await page.keyboard.press('ArrowRight'); for (let i = 0; i < 6; i++) await page.keyboard.press('ArrowLeft');
    await page.focus('.cei [data-k="sharpen"]'); for (let i = 0; i < 6; i++) await page.keyboard.press('ArrowRight'); for (let i = 0; i < 6; i++) await page.keyboard.press('ArrowLeft');
    await page.click('.cei [data-tog="denoise"]'); await page.click('.cei [data-tog="denoise"]');
    await previewDone(page);
    const ar0 = await page.evaluate(() => { const r = document.querySelector('.cei__sheet').getBoundingClientRect(); return r.width / r.height; });
    await page.click('.cei [data-do="right"]'); await settleP(page);
    const ar1 = await page.evaluate(() => { const r = document.querySelector('.cei__sheet').getBoundingClientRect(); return r.width / r.height; });
    await page.click('.cei [data-do="left"]'); await page.click('.cei [data-do="flipH"]'); await page.click('.cei [data-do="flipH"]'); await settleP(page);
    // crop: a tenth off the right, where the picture is white
    await page.click('.cei [data-do="crop"]'); await settleP(page);
    { const h = await page.locator('.cei__crop i[data-h="se"]').boundingBox(), sh = await page.locator('.cei__sheet').boundingBox(); await page.mouse.move(h.x + h.width / 2, h.y + h.height / 2); await page.mouse.down(); await page.mouse.move(h.x + h.width / 2 - sh.width * 0.08, h.y + h.height / 2, { steps: 4 }); await page.mouse.up(); }
    await page.click('.cei [data-do="crop"]'); await previewDone(page);
    const cs = await page.evaluate(() => CharmNestCeImage.current().settings());
    const stats = await page.evaluate(() => { const s = CharmNestCeImage.stats(), med = a => { const b = a.slice().sort((x, y) => x - y); return b.length ? b[b.length >> 1] : 0; }; const out = {}; for (const [k, v] of Object.entries(s.byKind)) out[k] = { n: v.length, med: +med(v).toFixed(1), max: +Math.max(...v).toFixed(1) }; return { out, traces: s.previewTraces, last: s.lastTrace }; });
    const tone = ['brightness', 'contrast', 'gamma'].map(k => stats.out[k]), fl = ['sharpen', 'denoise', 'turn', 'flip'].map(k => stats.out[k]);
    console.log('      main-thread ms per change (median / max):', Object.entries(stats.out).map(([k, v]) => `${k} ${v.med}/${v.max} ×${v.n}`).join(' · '), `· ${stats.traces} preview traces, the last ${stats.last && stats.last.ms} ms in the background`);
    // (a slider dragged is painted inside its frame; a click (a turn, a flip, a filter switched) never holds the page 50 ms, on this machine under the other agents' load too)
    check(`B5 a slider dragged paints in its frame: tone (brightness, contrast, gamma: one lookup table) median ${Math.max(...tone.map(t => t.med))} ms, sharpen ${stats.out.sharpen.med} ms (its filter in the worker); a click (turn, flip, denoise) at most ${Math.max(...fl.map(t => t && t.max || 0))} ms; a turn turns the preview (${ar0.toFixed(2)} → ${ar1.toFixed(2)}); the crop kept (${cs.crop && cs.crop.map(v => v.toFixed(2)).join(',')})`,
      tone.every(t => t && t.n >= 12 && t.med < 8) && stats.out.sharpen.med < 8 && fl.every(t => t && t.max < 50) && Math.abs(ar0 * ar1 - 1) < 0.05 && cs.crop && cs.crop[2] < 0.95 && cs.crop[2] > 0.85 && cs.rot === 0 && !cs.flipH && cs.sharpen === 0 && cs.gamma === 1 && !cs.denoise,
      JSON.stringify({ stats, cs, ar0, ar1 }));
    // background: the light band clicked away (white is never engraved), then put back
    await page.click('.cei [data-do="erase"]'); await settleP(page);
    { const sh = await page.locator('.cei__sheet').boundingBox(); await page.mouse.click(sh.x + sh.width * (100 / 240), sh.y + sh.height * (141 / 160)); }
    await page.click('.cei [data-do="erase"]'); await previewDone(page);
    const gE = await vecGreys(page), restoreOn = await page.evaluate(() => !document.querySelector('.cei [data-do="restore"]').disabled);
    await page.click('.cei [data-do="erase"]'); await page.click('.cei [data-do="restore"]'); await page.click('.cei [data-do="erase"]'); await previewDone(page);
    const gR = await vecGreys(page);
    // line art: black only
    await page.click('.cei [data-tmode="line"]'); await previewDone(page);
    const gL = await vecGreys(page), noteL = await page.evaluate(() => document.querySelector('.cei [data-note]').textContent);
    await page.click('.cei [data-tmode="grey"]'); await previewDone(page);
    await R.shot(w, 'CEI-2-adjusted');
    check(`B6 the background clicked away takes the light band off the engraving (${gE.length} greys) and Restore puts it back (${gR.length}); Line art traces black only (${gL.join(' ')})`,
      gE.length === g0.length - 1 && gE.includes('rgb(0,0,0)') && restoreOn && gR.length === g0.length && gL.join() === 'rgb(0,0,0)' && /^Line art:/.test(noteL), JSON.stringify({ gE, gR, gL, noteL }));

    /* ── PLACE ── */
    const thr0 = await page.evaluate(() => CharmNestCeImage.current().thresholds());
    await page.click('.cei [data-do="place"]');
    await page.waitForFunction(() => !document.querySelector('.cei'), null, { timeout: 60000 }); await settleP(page);
    const pl = await page.evaluate(() => {
      const E = CharmEditor.current, its = E.items(), g = its[its.length - 1], cut = its.find(i => i.role === 'cut'), cb = E.box(cut.id), gb = E.box(g.id);
      const fills = [...document.querySelectorAll(`.ce-items path`)].filter(p => (g.children || []).some(c => c.id === p.dataset.id)).map(p => getComputedStyle(p).fill);
      return { n: its.length, kind: g.kind, name: g.name, id: g.id, trace: g.trace, kids: (g.children || []).map(c => ({ fill: c.fill, role: c.role, artGrey: c.artGrey, name: c.name })), sel: E.selection(), cb, gb, fills, hist: E.history().undo,
        stat: CharmNestCeImage.stats().placeMs, layer: (document.querySelector(`.ce-root .ce-layer[data-id="${g.id}"] .ce-layer__name`) || {}).textContent };
    });
    const m = 0.4 * PT - 0.01, inside = pl.gb[0] >= pl.cb[0] + m && pl.gb[1] >= pl.cb[1] + m && pl.gb[2] <= pl.cb[2] - m && pl.gb[3] <= pl.cb[3] - m;
    const centred = Math.abs((pl.trace.box[0] + pl.trace.box[2]) / 2 - (pl.cb[0] + pl.cb[2]) / 2) < 0.5 && Math.abs((pl.trace.box[1] + pl.trace.box[3]) / 2 - (pl.cb[1] + pl.cb[3]) / 2) < 0.5;
    const ev = await page.evaluate(() => (window.__ceiLog || []).filter(e => e[0] === 'imageTrace').map(e => e[1]));
    check(`B7a Place traces with the thresholds the preview showed (CharmNestTrace.posterise: ${thr0 && thr0.join(', ')}) and logs imageTrace for the employee (${ev.length}: ${ev[0] && ev[0].levels} levels, ${ev[0] && ev[0].ops} points, ${ev[0] && ev[0].ms} ms)`,
      Array.isArray(thr0) && thr0.length === 4 && ev.length === 1 && JSON.stringify(ev[0].thresholds) === JSON.stringify(thr0.map(x => Math.round(x * 1000) / 1000)) && ev[0].levels === 4 && ev[0].layers === pl.kids.length && ev[0].into === 'outline',
      JSON.stringify({ thr0, ev }));
    await R.shot(w, 'CEI-3-placed');
    check(`B7 Place: ONE group "${pl.name}" on top, selected (${pl.kids.length} grey paths: ${pl.kids.map(k => k.name).join(', ')}, black on top), drawn in their own greys (${pl.fills.join(' ')}), centred and fitted inside the outline (${((pl.gb[2] - pl.gb[0]) / PT).toFixed(1)} × ${((pl.gb[3] - pl.gb[1]) / PT).toFixed(1)} mm in ${((pl.cb[2] - pl.cb[0]) / PT).toFixed(1)} × ${((pl.cb[3] - pl.cb[1]) / PT).toFixed(1)}), in ${pl.stat} ms`,
      pl.n === n0 + 1 && pl.kind === 'group' && pl.name === 'Image · logo' && pl.sel.join() === pl.id && pl.kids.length === g0.length && kOfN(pl.kids.map(k => k.fill[0]), 4) && pl.kids.every(k => k.role === 'hatch' && k.artGrey && k.fill[0] === k.fill[2])
        && pl.kids.every((k, i) => !i || k.fill[0] < pl.kids[i - 1].fill[0]) && pl.kids[pl.kids.length - 1].fill[0] === 0 && pl.fills.join() === pl.kids.map(k => css(k.fill[0])).join()
        && inside && centred && pl.trace.src === pl.id && pl.trace.levels === 4 && !!pl.layer, JSON.stringify(pl));

    /* ── an ordinary group: moved, sized, turned ── */
    for (let i = 0; i < 4; i++) await page.keyboard.press('ArrowRight');
    await typeField(page, 'w', (((pl.gb[2] - pl.gb[0]) / PT) * 0.8).toFixed(2));
    await typeField(page, 'r', '15');
    await settleP(page);
    // (where the picture's own box is now: its placing box through the map its parts are drawn with, so a re-trace whose ink is smaller is still compared on the same picture)
    const placed = id => page.evaluate(id => { const E = CharmEditor.current, c = E.ctx, g = E.items().find(i => i.id === id); if (!g) return null; const lf = g.children[0], e = c.entry(lf.id), W = c.model.mul(e.P, c.geo.matrix(lf)), b = g.trace.box;
      return { kind: g.kind, box: E.box(id), rot: g.children.map(x => x.rot), kids: g.children.length, fills: g.children.map(x => x.fill[0]), levels: g.trace.levels, hist: E.history().undo, corners: [[b[0], b[1]], [b[2], b[1]], [b[2], b[3]], [b[0], b[3]]].map(p => c.model.ap(W, p[0], p[1])) }; }, id);
    const mv = await placed(pl.id);
    check(`B8 then an ordinary group: nudged, sized to 80 % with W and turned 15° (its parts at ${mv.rot && mv.rot.map(r => +r.toFixed(3)).join(', ')}°), still one group of ${mv.kids}`,
      mv.kind === 'group' && mv.kids === pl.kids.length && mv.rot.every(r => Math.abs(r - 15) < 1e-6), JSON.stringify(mv));

    /* ── RE-TRACE: two levels, where it stands now ── */
    const h0 = await page.evaluate(() => CharmEditor.current.history().undo);
    await page.click('.ce-root [data-drop="xcmd"] .ce-drop__b'); await settleP(page);
    const menu = await page.evaluate(() => { const b = document.querySelector('.ce-root [data-xcmd="imageRetrace"]'); return b ? { on: !b.classList.contains('is-off'), text: b.textContent } : null; });
    await page.click('.ce-root [data-xcmd="imageRetrace"]');
    await page.waitForSelector('.ce-root .cei .cei__card', { timeout: 20000 }); await previewDone(page);
    const rt0 = await page.evaluate(() => ({ title: document.querySelector('.cei__head h3').textContent, levels: CharmNestCeImage.current().settings().levels, crop: CharmNestCeImage.current().settings().crop, where: document.querySelector('.cei [data-where]').textContent }));
    await page.focus('.cei [data-k="levels"]'); await page.keyboard.press('ArrowLeft'); await page.keyboard.press('ArrowLeft'); await previewDone(page);
    await page.click('.cei [data-do="place"]');
    await page.waitForFunction(() => !document.querySelector('.cei'), null, { timeout: 60000 }); await settleP(page);
    const rt = await placed(pl.id), off = rt && Math.max(...rt.corners.map((p, i) => Math.hypot(p[0] - mv.corners[i][0], p[1] - mv.corners[i][1])));
    await R.shot(w, 'CEI-4-retraced');
    check(`B9 Re-trace (the Object menu: "${menu && menu.text}") opens the step on the same picture and settings (${rt0.levels} levels, its crop), traced again with 2 levels: put where it stands (turned ${rt.rot && rt.rot.map(r => +r.toFixed(2)).join(', ')}°, the picture's corners where they were to ${off.toFixed(4)} pt), ONE undo step`,
      !!menu && menu.on && rt0.title === 'Re-trace image' && rt0.levels === 4 && !!rt0.crop && kOfN(rt.fills, 2) && rt.fills.includes(0) && rt.levels === 2 && rt.rot.every(r => Math.abs(r - 15) < 1e-6) && off < 0.01 && rt.hist === h0 + 1,
      JSON.stringify({ menu, rt0, rt, mv }));
    await page.keyboard.press('Control+z'); await settleP(page);
    const un = await page.evaluate(id => { const g = CharmEditor.current.items().find(i => i.id === id); return g && g.children.map(c => c.fill[0]); }, pl.id);
    check(`B10 one ⌘Z takes the re-trace back (its ${pl.kids.length} greys again)`, un && un.join() === pl.kids.map(k => k.fill[0]).join(), JSON.stringify(un));

    /* ── PASTE with a day square selected: the picture goes into that square ── */
    await page.keyboard.press('Escape'); await settleP(page);
    const sq = await page.evaluate(() => { const E = CharmEditor.current, s = E.items().filter(i => i.role === 'line').map(i => ({ id: i.id, b: E.box(i.id) })).sort((a, b) => b.b[1] - a.b[1] || a.b[0] - b.b[0]); return s[0]; });
    await page.evaluate(id => { const c = CharmEditor.current.ctx; if (c) c.select([id]); }, sq.id); await settleP(page);
    const selSq = await page.evaluate(() => CharmEditor.current.selection());
    await page.evaluate(() => { const dt = new DataTransfer(); dt.items.add(__cei.badge); document.body.dispatchEvent(new ClipboardEvent('paste', { clipboardData: dt, bubbles: true, cancelable: true })); });
    await page.waitForSelector('.ce-root .cei .cei__card', { timeout: 20000 }); await previewDone(page);
    const ps = await page.evaluate(() => ({ name: document.querySelector('.cei [data-name]').textContent, where: document.querySelector('.cei [data-where]').textContent, sel: document.querySelector('.cei button[data-into="selection"]').classList.contains('is-on') }));
    await page.click('.cei [data-do="place"]');
    await page.waitForFunction(() => !document.querySelector('.cei'), null, { timeout: 60000 }); await settleP(page);
    const pb = await page.evaluate(() => { const E = CharmEditor.current, its = E.items(), g = its[its.length - 1]; return { kind: g.kind, name: g.name, box: E.box(g.id), kids: g.children.map(c => c.fill[0]) }; });
    check(`B11 a picture PASTED with a day square selected (${ps.name}) goes into that square ("${ps.where}"): its box inside the square's`,
      selSq.join() === sq.id && ps.sel && /centred in the selected part/.test(ps.where) && pb.kind === 'group' && pb.box[0] >= sq.b[0] - 0.01 && pb.box[1] >= sq.b[1] - 0.01 && pb.box[2] <= sq.b[2] + 0.01 && pb.box[3] <= sq.b[3] + 0.01 && pb.kids.length === 2 && kOfN(pb.kids, 4) && pb.kids.includes(0),
      JSON.stringify({ ps, pb, sq }));

    /* ── SAVE: the record keeps every grey, through the real artPut ── */
    await page.click('.ce-root [data-act="save"]');
    for (let t0 = Date.now(); Date.now() - t0 < 60000 && !st.doc('Charm_Pool_Art', PID + '~front'); ) await new Promise(r => setTimeout(r, 200));
    await R.settle(page, 1200);
    const rec = st.doc('Charm_Pool_Art', PID + '~front'), greys = rec ? rec.added.filter(e => e.artGrey) : [];
    check(`B12 Save: the piece's record (Charm_Pool_Art, by ${rec && rec.by}) keeps ${greys.length} grey parts, each artGrey with its grey fill (${[...new Set(greys.map(e => e.fill[0]))].sort().join(', ')}), even-odd, in its image's group; nothing else changed`,
      !!rec && greys.length === pl.kids.length + pb.kids.length && greys.every(e => e.role === 'hatch' && e.even === true && e.fill[0] === e.fill[1] && e.fill[1] === e.fill[2] && !e.stroke && /^img/.test(e.group))
        && greys.map(e => e.fill[0]).sort().join() === pl.kids.map(k => k.fill[0]).concat(pb.kids).sort().join() && !Object.keys(rec.changed || {}).length && !(rec.removed || []).length && rec.added.length === greys.length, JSON.stringify(rec && rec.added.map(e => [e.id, e.fill, e.artGrey, e.group])));

    /* ── the laser's file: each grey its own fill colour; the month's letters still blue ── */
    await R.openEngraveDetail(w, 'cal', { face: 'front' }); await R.settle(page, 900);
    const sheet = await page.evaluate(async id => {
      const ch = Pool.charmOf(id), bytes = await CharmNestPDF.buildSheet({ sheet: { wPt: 120, hPt: 130 }, placements: [{ charm: ch, angle: 0, cxPt: 60, cyPt: 65, scale: 1 }], sources: new Map([[ch.sourceId, (sourceOf(ch.sourceId) || {}).parsed]]), title: 'image' });
      const p = await CharmNestPDF.parseSource(new Uint8Array(bytes), 'sheet.ai');
      const forms = p.segments.filter(s => s.kind === 'xobj'), fills = forms.length ? forms[0].children.filter(k => k.kind === 'path' && k.fill).map(k => k.fillRGB.map(v => +v.toFixed(3)).join()) : [];
      const text = new TextDecoder('latin1').decode(bytes);
      return { fills, greys: ch.members.filter(m => m.artGrey).map(m => [m.fillRGB.join(), !!m.hatchBlue]), hash: ch.hash, n: bytes.length };
    }, PID);
    const count = (list, v) => list.filter(x => x === v).length, want = [...new Set(greys.map(e => e.fill.join()))].sort();
    check(`B13 the sheet's file (CharmNestPDF.buildSheet): the piece's grey parts are written as grey fills (${want.map(v => v + ' ×' + count(sheet.fills, v)).join(', ')}, as in the record), never blue hatching, and MAY's three letters stay blue (×${count(sheet.fills, '0,0,1')})`,
      want.every(v => count(sheet.fills, v) === count(greys.map(e => e.fill.join()), v)) && count(sheet.fills, '0,0,1') === 3 && sheet.fills.length === 3 + greys.length && sheet.greys.length === greys.length && sheet.greys.every(g => !g[1]) && /~ae/.test(sheet.hash),
      JSON.stringify(sheet));

    /* ── opened again: the greys come back grey, and Re-trace still knows their picture ── */
    await page.click(Q + ' .backHost .egCharmEdit');
    await page.waitForSelector('.ce-root .ce-items path', { timeout: 60000 }); await settleP(page);
    const re = await page.evaluate(() => {
      const E = CharmEditor.current, gs = E.items().filter(i => i.artGrey);
      const css = gs.map(i => { const p = document.querySelector(`.ce-items path[data-id="${i.id}"]`); return p ? getComputedStyle(p).fill : ''; });
      const c = E.ctx; if (c) c.select([gs[0].id]);
      return { n: gs.length, fills: gs.map(i => i.fill[0]), css, groups: [...new Set(gs.map(i => i.group))].length, retrace: !!(c && CharmEditor.commands.imageRetrace.enabled(c)) };
    });
    check(`B14 opened again, the ${re.n} grey parts come back grey (drawn ${[...new Set(re.css)].join(' · ')}), each with its image's group, and Re-trace is offered on them`,
      re.n === greys.length && re.groups === 2 && re.css.every((f, i) => f === css(re.fills[i])) && re.fills.some(g => g > 0) && re.retrace, JSON.stringify(re));
    await page.keyboard.press('Escape'); await page.keyboard.press('Escape');
    check('B15 no page error', w.errors.length === 0, JSON.stringify(w.errors).slice(0, 400));
  } finally { await w.close(); }
}

(async () => {
  const kit = await nodeKit();
  if (ONLY !== 'B') await partA(kit);
  if (ONLY !== 'A') await partB(kit);
  const bad = RESULTS.filter(r => !r.ok);
  console.log(`\n${RESULTS.length - bad.length}/${RESULTS.length} checks pass`);
  process.exit(bad.length ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
