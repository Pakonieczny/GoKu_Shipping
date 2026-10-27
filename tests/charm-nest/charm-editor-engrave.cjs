// CHARM EDIT (Paul, 27 Oct 2026: "In the Engraving Modal the User sometimes needs to be able to see the engraving on the front of a given charm. The user also needs full control similar to
// Adobe illustrator ... over each individual layer contained on any charm ... In this case the user needs to be able to add a date inside a given Calendar day square and be able to update the
// Month name located on top of the Calendar charm ... zoom into a given charm while maintaining full vector file details and have very fine and accurate control when adding text").
// The charm editor (charm-nest-charm-editor.js) opened from the Engraving window's "Edit charm" and saved through the art edit (charm-nest-art-edit.js) into the piece's record and its laser files.
//   A  (node) the two modules agree: CharmNestArtEdit.itemsOf opens cleanly in the editor (nothing read as a change, the cut line locked), and encode takes what the editor hands back: a turned
//      and scaled curve lands where the editor drew it (its matrix is made about the TIGHT box of the curve, not the box of its control points), and words in a turned group land where the editor
//      drew them (their outlines come already placed: nothing is put on them twice)
//   B  the real sorter page over the repo's fake site (_engraveRealWorld.cjs: the real intake, pool, classifier and fit; offline, nothing written anywhere real, made-up names) with a calendar charm
//      (_artCalendar.cjs, a per-SKU master) ordered front + back, and another calendar order: the Engraving window's quiet "Edit charm" pill beside the grid's label; the editor opens on the front
//      with every part a layer and the cut line locked; "MAY" swept and deleted, SEPTEMBER typed and centred where it was with the X / Y fields; the wheel zooms into a day square and "25" is set at
//      5 pt and moved into it with the arrow keys; Save → the Charm_Pool_Art record (who, what removed, what added), the piece's charm carries the new parts (a hash of its own, "~ae"), the work
//      area's front art draws them, the card's Front thumbnail is drawn again, the approved front is withdrawn in Engrave.artChanged's words, the back keeps its words and its "Show front" shows
//      the new front, and CharmNestPDF.buildSheet writes the new outlines as blue hatching on that piece while the other calendar's drawing is byte for byte what it was. Cancel writes nothing;
//      the editor opens again on the saved edit, and a second edit keeps the first; a part off the metal is refused in the work area's note, and nothing of that edit (front or back) is saved
//      (27 Oct 2026, Paul: "the thickness of the Red engrave line is too thick, I need ... fine font thickness control over each font") the 25 made 0.04 mm bolder with the Weight field
//      and one day square's red line thinned to 0.05 mm with the Stroke field (drawn at exactly that width): the record keeps the weight with the outlines AT it and the line's width,
//      the sheet's file engraves those very outlines and writes that width, and the editor opened again reads both back
//   PW_DIR=<playwright node_modules> CHROMIUM=<chrome> node tests/charm-nest/charm-editor-engrave.cjs      (CE_ONLY=A or B; SHOTS=<dir> for the screenshots)
'use strict';
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path');
const ROOT = path.join(__dirname, '../..');
const RESULTS = [];
const check = (id, ok, evidence) => { RESULTS.push({ id, ok: !!ok }); console.log(`${ok ? 'PASS' : 'FAIL'}  ${id}  ${ok ? '' : evidence || ''}`.trimEnd()); };
const ONLY = process.env.CE_ONLY || '';
const PT = 72 / 25.4;

/* the node side: pdf-lib, the PDF reader and the calendar (made here as the shop draws its masters) */
async function nodeKit() {
  const hadWindow = 'window' in global, hadSelf = 'self' in global;
  global.window = global; global.self = global;
  global.PDFLib = global.PDFLib || require(path.join(ROOT, 'vendor/pdf-lib-1.17.1.min.js'));
  require(path.join(ROOT, 'charm-nest-pdf.js'));
  global.CharmNestGeom = global.CharmNestGeom || require(path.join(ROOT, 'charm-nest-geom.js'));
  const cal = await require('./_artCalendar.cjs')(ROOT);
  const kit = { P: global.CharmNestPDF, G: global.CharmNestGeom, L: global.PDFLib, AE: require(path.join(ROOT, 'charm-nest-art-edit.js')), CE: require(path.join(ROOT, 'charm-nest-charm-editor.js')), cal };
  if (!hadWindow) delete global.window; if (!hadSelf) delete global.self;   // (playwright must not think it runs in a page)
  return kit;
}

/* ═══ A · the two modules agree ═══ */
async function partA(kit) {
  const { AE, CE, G, cal } = kit, M = CE.model, geo = M.createGeo(null), cb = cal.charm;
  const view = G.backView(cb, { face: 'front', upAngle: 75, res: 6 }), fr = AE.frame(view);
  const items = AE.itemsOf(cb, view, 'front'), base = M.prepIn(items, ['cut']);
  const tight = it => M.bboxOf(it.subpaths), outB = M.exportItems(base, geo);
  const none = M.diffFace(outB, M.exportItems(base, geo)), lockedIds = base.filter(i => i.locked).map(i => i.id);
  check('A1 itemsOf opens cleanly in the editor: every part a path layer with its id and role, its box the box of its own geometry; nothing read as a change; the cut line opens locked (and the lock is the editor\'s, no change)',
    items.length === cb.members.length && base.every((it, i) => it.id === items[i].id && it.kind === 'path' && ['cut', 'line', 'hatch', 'art'].includes(it.role) && JSON.stringify(geo.box(it)) === JSON.stringify(tight(items[i]))) &&
    M.changeCount(none) === 0 && lockedIds.length === 1 && base.find(i => i.locked).role === 'cut' && outB.find(i => i.id === lockedIds[0]).locked === false,
    JSON.stringify({ n: items.length, locked: lockedIds, change: M.changeCount(none) }));

  // a curve whose control points stand well off it (its control box is not its tight box), turned 30° and scaled; and words in a group turned 15° and moved
  const day = geo.box(base.find(i => i.id === AE.memberIds(cb)[cal.day(1, 2)])), cx = (day[0] + day[2]) / 2, cy = (day[1] + day[3]) / 2;
  const arc = { id: 'arc1', kind: 'path', role: 'line', source: 'added', stroke: true, fill: false, lwPt: 0.3, subpaths: [[['m', [cx - 4, cy + 2]], ['c', [cx - 2, cy - 6], [cx + 2, cy - 6], [cx + 4, cy + 2]]]], tx: 0.6, ty: -0.4, sx: 1.3, sy: 0.8, rot: 30 };
  const words = { id: 'w1', kind: 'text', role: 'hatch', source: 'added', text: '25', fontKey: 'x', sizePt: 4, align: 'left', tx: cx - 2.5, ty: cy + 1.4, sx: 1, sy: 1, rot: 0 };
  const grp = { id: 'g1', kind: 'group', role: 'art', source: 'added', tx: 0.4, ty: 0.2, sx: 1, sy: 1, rot: 15, children: [words] };
  const after = base.concat([arc, grp]), out = M.exportItems(after, geo), diff = M.diffFace(outB, out);
  const ctrl = M.bboxOf(arc.subpaths.map(sp => sp.map(o => (o[0] === 'c' ? ['l', o[1]] : o)).concat(sp.filter(o => o[0] === 'c').map(o => ['l', o[2]])))), tb = tight(arc);
  check('A2 the editor hands the words back as outlines already placed in the face and the curve with its own matrix (about the tight box, which its control points\' box is not)',
    diff.added.map(a => a.id).join() === 'arc1,g1' && Array.isArray(out.find(i => i.id === 'arc1').matrix) && out.find(i => i.id === 'g1').children[0].subpaths.length > 0 && Math.abs((ctrl[1] + ctrl[3]) / 2 - (tb[1] + tb[3]) / 2) > 1,
    JSON.stringify({ added: diff.added.map(a => a.id), ctrl, tb }));
  const rec = AE.encode(diff, { charm: cb, view, face: 'front', placed: true }), ed = AE.apply(cb, Object.assign({ at: 1 }, rec), 'front');
  const asItem = segs => segs.flatMap(m => m.subpaths.map(sp => sp.map(o => (o[0] === 'h' ? ['h'] : [o[0]].concat(o.slice(1).map(fr.toItem))))));
  const worst = (a, b) => (a && b ? Math.max(...a.map((v, i) => Math.abs(v - b[i]))) : Infinity);
  const oArc = out.find(i => i.id === 'arc1'), drawnArc = M.bboxOf(M.transformSubpaths(oArc.subpaths, oArc.matrix)), gotArc = M.bboxOf(asItem(ed.members.filter(m => m.artMemberId === 'arc1')));
  const drawnW = M.bboxOf(out.find(i => i.id === 'g1').children[0].subpaths), gotW = M.bboxOf(asItem(ed.members.filter(m => m.artMemberId === 'w1')));
  check('A3 encode → apply puts the turned curve and the words in the turned group on the piece exactly where the editor drew them (to the record\'s 0.001 pt)',
    worst(drawnArc, gotArc) < 0.003 && worst(drawnW, gotW) < 0.003 && ed.members.filter(m => m.artMemberId === 'w1').every(m => m.hatchBlue && m.fill && !m.stroke) && ed.members.find(m => m.artMemberId === 'arc1').stroke,
    JSON.stringify({ arc: worst(drawnArc, gotArc), words: worst(drawnW, gotW) }));
  // (what that agreement is worth: the same curve read by its tx..rot about the box of its control points, as encode read every path before, lands elsewhere)
  const old = AE.encode({ added: [Object.assign({}, oArc, { matrix: undefined })] }, { charm: cb, view, face: 'front' }), oldGot = M.bboxOf(asItem(AE.apply(cb, Object.assign({ at: 2 }, old), 'front').members.filter(m => m.artMemberId === 'arc1')));
  check('A4 (without the editor\'s matrix the turned curve would land ' + worst(drawnArc, oldGot).toFixed(2) + ' pt off)', worst(drawnArc, oldGot) > 0.3, '');
}

/* ═══ B · the Engraving window ═══ */
const settleP = page => page.evaluate(() => new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r))));
async function drag(page, a, b) { await page.mouse.move(a.x, a.y); await page.mouse.down(); await page.mouse.move(b.x, b.y, { steps: 10 }); await page.mouse.up(); await settleP(page); }
async function typeField(page, f, value) { const el = page.locator(`.ce-root [data-f="${f}"]`); await el.click(); await page.keyboard.press('Control+a'); await page.keyboard.type(String(value)); await page.keyboard.press('Enter'); await settleP(page); }
const C = (page, fn, arg) => page.evaluate(fn, arg);
const clientOf = (page, x, y) => C(page, ([x, y]) => window.CharmEditor.current.clientOf(x, y), [x, y]);
const findText = (page, t) => C(page, t => { let f = null; const walk = l => l.forEach(i => { if (i.kind === 'text' && i.text === t) f = i; if (i.children) walk(i.children); }); walk(window.CharmEditor.current.items()); return f; }, t);
const union = bs => bs.reduce((a, b) => [Math.min(a[0], b[0]), Math.min(a[1], b[1]), Math.max(a[2], b[2]), Math.max(a[3], b[3])]);

/** One charm form of a written sheet (its decoded stream and its resources resolved all the way down, object numbers left out): two files' copies of one piece compared (art-edit-output.cjs). */
async function formsOf(L, bytes) {
  const doc = await L.PDFDocument.load(bytes), ctx = doc.context, page = doc.getPage(0), xo = ctx.lookup(page.node.Resources().get(L.PDFName.of('XObject')));
  const deep = (v, seen) => { v = v instanceof L.PDFRef ? ctx.lookup(v) : v; if (v instanceof L.PDFDict) { if (seen.has(v)) return '<loop>'; seen.add(v); return '<<' + v.entries().map(([k, x]) => k.toString() + ' ' + deep(x, seen)).sort().join(' ') + '>>'; } if (v instanceof L.PDFArray) return '[' + v.asArray().map(x => deep(x, seen)).join(' ') + ']'; if (v && v.dict) return 'stream ' + deep(v.dict, seen) + ' ' + Buffer.from(L.decodePDFRawStream(v).decode()).toString('latin1').length; return String(v); };
  const out = {};
  for (const [k, ref] of xo.entries()) { const st = ctx.lookup(ref); out[k.toString().slice(1).replace(/-\d+$/, '')] = { content: Buffer.from(L.decodePDFRawStream(st).decode()).toString('latin1'), res: deep(st.dict.get(L.PDFName.of('Resources')), new Set()) }; }
  return out;
}

async function partB(kit) {
  const R = require('./_engraveRealWorld.cjs');
  const shots = process.env.SHOTS || '/tmp/charm-editor-engrave-shots';
  const c = kit.cal.charm;
  // the calendar in the master library (a per-SKU file, as the Library writes it), ordered front + back ("front - W / back - Name One", font Ace) and once more by another buyer (back only)
  R.DESIGNS.CALENDAR = { file: 'CALENDAR.ai', make: () => Buffer.from(kit.cal.bytes), entry: { sku: 'CALENDAR', widthPt: +(c.bbox[2] - c.bbox[0]).toFixed(3), heightPt: +(c.bbox[3] - c.bbox[1]).toFixed(3), areaPt2: 4200, members: c.members.length, holes: 0, engravable: true, engravableBy: 'index', upAngle: 90, upSource: 'drawn', charmHash: 'calhash01', backKeepOut: [] } };
  R.ORDERS.cal = Object.assign({}, R.ORDERS.both, { rid: '4290000131', sku: 'CALENDAR', title: 'Calendar Necklace, Personalized Month Calendar Charm', buyer: 'Name Eleven', personalization: ['front - W\nback - Name One'] });
  R.ORDERS.cal2 = Object.assign({}, R.ORDERS.back, { rid: '4290000132', sku: 'CALENDAR', listing: '1890000132', title: 'Calendar Necklace, Personalized Month Calendar Charm', buyer: 'Name Twelve', personalization: ['Name Two'] });
  R.EXPECT_JOBS.cal = 2; R.EXPECT_JOBS.cal2 = 1;
  const w = await R.world({ shots });
  if (!w) { console.log('SKIP no playwright-core'); return; }
  const page = w.page, st = w.srv.st, Q = '#egQueue .rvItem[data-kind=placement]';
  const artPuts = () => st.calls.filter(x => x.name === 'charmNestLibrary' && x.op === 'artPut').length;
  try {
    const made = await R.build(w, ['cal', 'cal2']);
    const PID = made.cal.poolIds[0], PID2 = made.cal2.poolIds[0];
    check('B0 the world: the calendar is pooled and its front and back are fitted (and another calendar order)', made.cal.jobs.length === 2 && made.cal.jobs.every(j => j.fitted) && made.cal2.jobs.length === 1, JSON.stringify(made.cal.jobs.map(j => [j.face, j.state])));
    await page.evaluate(() => {
      window.__ce = {
        card: () => document.querySelector('#egQueue .rvItem[data-kind=placement]'),
        cv: () => document.querySelector('#egQueue .rvItem[data-kind=placement] .backHost canvas'),
        job: () => Engrave.items().get(document.querySelector('#egQueue .rvItem[data-kind=placement]').dataset.key),
        thumb: face => { const t = document.querySelector(`#egQueue .rvItem[data-kind=placement] .engCard .engFace[data-face="${face}"] .engThumb canvas`); if (!t) return ''; const d = t.toDataURL(); let h = 0x811c9dc5; for (let i = 0; i < d.length; i++) { h ^= d.charCodeAt(i); h = Math.imul(h, 0x01000193) >>> 0; } return h.toString(16) + ':' + t.width; },
        /** a sheet of these pieces written as the laser gets it (CharmNestPDF.buildSheet), base64 */
        async sheet(ids) {
          const charms = ids.map(id => Pool.charmOf(id)), placements = charms.map((ch, i) => ({ charm: ch, angle: 0, cxPt: 50 + 90 * i, cyPt: 60, scale: 1 }));
          const bytes = await CharmNestPDF.buildSheet({ sheet: { wPt: 200, hPt: 130 }, placements, sources: new Map(charms.map(ch => [ch.sourceId, (sourceOf(ch.sourceId) || {}).parsed])), title: 'charm edit' });
          let s = ''; const u = new Uint8Array(bytes); for (let i = 0; i < u.length; i += 0x8000) s += String.fromCharCode.apply(null, u.subarray(i, i + 0x8000)); return btoa(s);
        },
        /** each piece's form of a written sheet, parsed back: its fills [["r,g,b", bbox]] and its line engravings (red strokes) [bbox] */
        async parts(b64) { const p = await CharmNestPDF.parseSource(Uint8Array.from(atob(b64), x => x.charCodeAt(0)), 'sheet.ai'); return p.segments.filter(s => s.kind === 'xobj').map(f => ({ fills: f.children.filter(k => k.kind === 'path' && k.fill).map(k => [k.fillRGB.join(), k.bbox]), lines: f.children.filter(k => k.kind === 'path' && k.stroke && !k.fill && k.strokeRGB[0] > 0.5 && k.strokeRGB[2] < 0.5).map(k => k.bbox), lws: f.children.filter(k => k.kind === 'path' && k.stroke && !k.fill && k.strokeRGB[0] > 0.5 && k.strokeRGB[2] < 0.5).map(k => k.lwPt) })); }
      };
    });

    /* ── the front approved first: its record and its file, as a person approves it ── */
    const sheets = await page.evaluate(() => { const out = []; for (const s of allSheets()) if (s.charms && s.charms.length) { if (!s.folderPath) s.folderPath = 'charmnest/sheets/' + s.sheetId; out.push({ sheetId: s.sheetId, poolIds: s.charms.map(x => x.poolId) }); } return out; });
    for (const s of sheets) st.put('Charm_Nest_Sheets', s.sheetId, { sheetId: s.sheetId, poolIds: s.poolIds, backPool: [] });
    await R.openEngraveDetail(w, 'cal', { face: 'front' }); await R.settle(page, 700);
    await page.click(Q + ' [data-a="approve"]');
    for (let t0 = Date.now(); Date.now() - t0 < 120000 && !st.doc('Charm_Pool_Back', PID + '~front'); ) await new Promise(r => setTimeout(r, 300));
    await R.openEngraveDetail(w, 'cal', { face: 'front' }); await R.settle(page, 900);
    const a0 = await page.evaluate(() => { const j = __ce.job(); return { face: j.face, state: j.state, approvedAt: j.approvedAt, art: __ce.cv()._frontArt.length }; });
    check('B1 the calendar\'s front is approved (its record saved) and its card shows the month and the day grid in the work area', !!st.doc('Charm_Pool_Back', PID + '~front') && ['approved', 'written'].includes(a0.state) && a0.art === 23, JSON.stringify(a0));

    /* ── the entry: one quiet pill beside the grid's label ── */
    const e0 = await page.evaluate(() => {
      const b = document.querySelector('#egQueue .rvItem[data-kind=placement] .backHost .egCharmEdit'), cv = __ce.cv(), r = b && b.getBoundingClientRect(), c = cv.getBoundingClientRect(), ctl = __ce.card().querySelector('.ctl');
      return { has: !!b, text: b && getComputedStyle(b, '::after').content.replace(/"/g, ''), name: b && b.getAttribute('aria-label'), quiet: (b && b.parentElement.textContent.trim()) || '', icon: !!(b && b.querySelector('svg')), pos: r && [Math.round(r.left - c.left), Math.round(r.top - c.top), Math.round(r.right - c.left), Math.round(r.height)], cw: Math.round(c.width), inToolbar: !!(ctl && ctl.querySelector('.egCharmEdit')), all: document.querySelectorAll('.egCharmEdit').length };
    });
    const entryShot = await R.shot(w, 'CE-1-entry-front');
    check('B2 the Engraving window: one "Edit charm" pill (an icon and two words) in the drawing\'s top-left corner beside the grid\'s label, not in the toolbar below; its words are the style sheet\'s, so the work area\'s own text (its messages) stays empty', e0.has && e0.text === 'Edit charm' && e0.name === 'Edit charm' && e0.quiet === '' && e0.icon && e0.pos[0] > 30 && e0.pos[1] >= 0 && e0.pos[1] < 30 && e0.pos[2] < e0.cw / 2 && e0.pos[3] < 24 && !e0.inToolbar && e0.all === 1, JSON.stringify(e0));
    await R.openEngraveDetail(w, 'cal', { face: 'back' }); await R.settle(page, 700);
    const e1 = await page.evaluate(() => { const h = document.querySelector('#egQueue .rvItem[data-kind=placement] .backHost'), b = h.querySelector('.egCharmEdit'), p = h.querySelector('.egFrontPeek'); const rb = b && b.getBoundingClientRect(), rp = p && p.getBoundingClientRect(); return { has: !!b, peek: !!p, after: !!(rb && rp && rb.left >= rp.right && Math.abs(rb.top - rp.top) < 4) }; });
    await R.shot(w, 'CE-1b-entry-back');
    check('B3 on the back it sits right after "Show front", on the same line', e1.has && e1.peek && e1.after, JSON.stringify(e1));
    const master = await page.evaluate(id => { const ch = Pool.charmOf(id); return { hash: ch.hash, n: ch.members.length }; }, PID2);

    /* ── the editor opens on the front ── */
    await R.openEngraveDetail(w, 'cal', { face: 'front' }); await R.settle(page, 700);
    const thumb0 = await page.evaluate(() => __ce.thumb('front'));
    const backFit0 = await page.evaluate(id => { const b = [...Engrave.items().values()].find(x => x.face !== 'front' && (x.copies || []).includes(id)); return JSON.stringify([b.state, b.fit.size, b.fit.centre, b.fit.angle]); }, PID);
    const sheet0 = await page.evaluate(ids => __ce.sheet(ids), [PID, PID2]);
    await page.click(Q + ' .backHost .egCharmEdit');
    await page.waitForSelector('.ce-root .ce-items path', { timeout: 60000 }); await settleP(page); await settleP(page);
    const o0 = await page.evaluate(() => { const E = CharmEditor.current, its = E.items(); return { face: E.face(), n: its.length, back: E.items('back').length, roles: its.map(i => i.role).join(), locked: its.filter(i => i.locked).map(i => [i.role, i.name]), paths: document.querySelectorAll('.ce-items path').length, title: document.querySelector('.ce-id h2').textContent, sub: document.querySelector('.ce-eyebrow').textContent, fonts: [...document.querySelectorAll('.ce-root .ce-item[data-font]')].map(b => b.textContent), frame: E.frame() }; });
    await R.shot(w, 'CE-2-editor-open');
    check('B4 Edit charm opens the editor on the front: CALENDAR, "order … · piece 1 of 1", every part a layer (outline, 3 month letters, 20 day squares), drawn as SVG paths',
      o0.face === 'front' && o0.n === 24 && o0.title === 'CALENDAR' && o0.sub === `order ${made.cal.rid} · piece 1 of 1` && o0.paths >= 24 && (o0.roles.match(/hatch/g) || []).length === 3 && (o0.roles.match(/line/g) || []).length === 20 && o0.back === 1, JSON.stringify(o0));
    check('B5 the cut line is locked and says why (the piece is on a sheet); the fonts are the engraving drop-down\'s words, the piece\'s own first (Ace)',
      o0.locked.length === 1 && o0.locked[0][0] === 'cut' && /on a sheet/.test(o0.locked[0][1]) && o0.fonts[0] === 'Ace' && ['Stylish', 'Pristina', 'Typewriter', 'Britannic Bold'].every(f => o0.fonts.includes(f)), JSON.stringify(o0.locked) + ' ' + o0.fonts.join(','));

    /* ── the month: swept, deleted, SEPTEMBER typed and centred where MAY was ── */
    const letters = await page.evaluate(() => CharmEditor.current.items().filter(i => i.role === 'hatch').map(i => i.id));
    const mBox = union(await Promise.all(letters.map(id => C(page, id => CharmEditor.current.box(id), id))));
    await page.keyboard.press('m');
    const pad = 0.6 * PT;
    await drag(page, await clientOf(page, mBox[0] - pad, mBox[1] - pad), await clientOf(page, mBox[2] + pad, mBox[3] + pad));
    const swept = await page.evaluate(() => CharmEditor.current.selection());
    await page.keyboard.press('Delete'); await settleP(page);
    await page.keyboard.press('t');
    const at = await clientOf(page, mBox[0], mBox[3]);
    await page.mouse.click(at.x, at.y); await settleP(page);
    await page.keyboard.type('SEPTEMBER'); await page.keyboard.press('Enter'); await settleP(page);
    const sep0 = await findText(page, 'SEPTEMBER');
    const fr = o0.frame, wmm = parseFloat(await page.locator('.ce-root [data-f="w"]').inputValue()), hmm = parseFloat(await page.locator('.ce-root [data-f="h"]').inputValue());
    const mid = [(mBox[0] + mBox[2]) / 2, (mBox[1] + mBox[3]) / 2];
    await typeField(page, 'x', (((mid[0] - fr.x0) / PT) - wmm / 2).toFixed(3));
    await typeField(page, 'y', (((mid[1] - fr.y0) / PT) - hmm / 2).toFixed(3));
    const sepBox = await C(page, id => CharmEditor.current.box(id), sep0.id);
    await R.shot(w, 'CE-3-september');
    check('B6 the marquee sweeps exactly the three letters of MAY and Delete takes them out; SEPTEMBER is typed in Ace (outlines from the page\'s own font) and centred where MAY was with X and Y',
      swept.slice().sort().join() === letters.slice().sort().join() && sep0 && sep0.fontKey === 'jost-ace' && Math.abs((sepBox[0] + sepBox[2]) / 2 - mid[0]) < 0.03 && Math.abs((sepBox[1] + sepBox[3]) / 2 - mid[1]) < 0.03 && (sepBox[2] - sepBox[0]) > 25 && (sepBox[2] - sepBox[0]) < 60,
      JSON.stringify({ swept, letters, sepBox, mid, wmm }));

    /* ── a date in a day square: zoomed in, 5 pt, the arrow keys ── */
    await page.keyboard.press('Escape'); await page.keyboard.press('v'); await settleP(page);
    const squares = await page.evaluate(() => CharmEditor.current.items().filter(i => i.role === 'line').map(i => ({ id: i.id, b: CharmEditor.current.box(i.id) })).sort((a, b) => a.b[1] - b.b[1] || a.b[0] - b.b[0]));
    const sq = squares[2 * 5 + 3].b, sc = await clientOf(page, (sq[0] + sq[2]) / 2, (sq[1] + sq[3]) / 2);
    await page.mouse.move(Math.round(sc.x), Math.round(sc.y));
    for (let i = 0; i < 10; i++) { await page.mouse.wheel(0, -100); await page.waitForTimeout(16); }
    await settleP(page);
    const zoom = await page.evaluate(() => CharmEditor.current.zoom());
    await page.keyboard.press('t');
    const near = await clientOf(page, sq[0] - 1.2 * PT, sq[3] - 0.5 * PT);
    await page.mouse.click(near.x, near.y); await settleP(page);
    await page.keyboard.type('25'); await page.keyboard.press('Enter'); await settleP(page);
    await typeField(page, 'pt', '5');
    let t25 = await findText(page, '25');
    const tb = await C(page, id => CharmEditor.current.box(id), t25.id), mm = v => v / PT;
    const dx = mm((sq[0] + sq[2]) / 2 - (tb[0] + tb[2]) / 2), dy = mm((sq[1] + sq[3]) / 2 - (tb[1] + tb[3]) / 2);
    const keys = (d, pos, neg) => { const big = Math.trunc(d), small = Math.round((d - big) / 0.05); return [...Array(Math.abs(big)).fill('Shift+' + (big > 0 ? pos : neg)), ...Array(Math.abs(small)).fill(small > 0 ? pos : neg)]; };
    const taps = keys(dx, 'ArrowRight', 'ArrowLeft').concat(keys(dy, 'ArrowDown', 'ArrowUp'));
    for (const k of taps) await page.keyboard.press(k);
    await settleP(page);
    t25 = await findText(page, '25');
    const b25 = await C(page, id => CharmEditor.current.box(id), t25.id), svg = await page.evaluate(() => ({ imgs: document.querySelectorAll('.ce-root img, .ce-stage canvas').length, paths: document.querySelectorAll('.ce-items path').length }));
    await R.shot(w, 'CE-4-zoomed-25');
    check(`B7 the wheel zooms into a day square (${Math.round(zoom)}%, still SVG paths, no picture), "25" is set at 5 pt and moved into the square with ${taps.length} arrow taps (Shift 1 mm, 0.05 mm)`,
      zoom > 2000 && svg.imgs === 0 && svg.paths > 20 && t25.sizePt === 5 && b25[0] > sq[0] && b25[1] > sq[1] && b25[2] < sq[2] && b25[3] < sq[3] && Math.abs(mm((b25[0] + b25[2]) / 2 - (sq[0] + sq[2]) / 2)) < 0.03 && taps.length > 3,
      JSON.stringify({ zoom, b25, sq, svg }));
    /* ── WEIGHT AND LINE WIDTH (Paul, 27 Oct 2026: "the thickness of the Red engrave line is too thick, I need the ability to control the thickness of fonts ... I need fine font
       thickness control over each font"): the 25 made 0.04 mm bolder with the Weight field, one day square's red line thinned to 0.05 mm with the Stroke field ── */
    const inkOf = id => page.evaluate(id => { const it = CharmEditor.current.result().faces.front.items.find(i => i.id === id); return CharmEditor.model.inkArea(it.subpaths); }, id);
    await page.keyboard.press('Escape'); await page.keyboard.press('v'); await settleP(page);
    { const b = await C(page, id => CharmEditor.current.box(id), t25.id), p = await clientOf(page, (b[0] + b[2]) / 2, (b[1] + b[3]) / 2); await page.mouse.click(p.x, p.y); await settleP(page); }
    const bar = await page.evaluate(() => { const w = document.querySelector('.ce-root [data-when="weight"]'), kids = [...document.querySelector('.ce-props').children].filter(e => e.getBoundingClientRect().height > 0); return { sel: CharmEditor.current.selection(), shown: !w.hidden, value: w.querySelector('input').value, rows: new Set(kids.map(e => Math.round(e.getBoundingClientRect().top))).size }; });
    const ink25 = await inkOf(t25.id);
    await typeField(page, 'wt', '0.04');
    t25 = await findText(page, '25');
    const ink25w = await inkOf(t25.id);
    await R.shot(w, 'CE-4b-25-weight');
    check(`B7b the 25 in Ace takes the Weight field (on the one-row bar beside its size: ${bar.rows} row) and +0.04 mm makes it bolder (${ink25.toFixed(2)} → ${ink25w.toFixed(2)} pt² of ink)`,
      bar.sel.join() === t25.id && bar.shown && bar.value === '0.00' && bar.rows === 1 && t25.weightMm === 0.04 && ink25w > ink25 * 1.05, JSON.stringify({ bar, ink25, ink25w, w: t25.weightMm }));
    // where the record must put the 25's outlines: the editor's own, taken back to the charm's points through the face's view (as the art edit does)
    const want25 = await page.evaluate(id => { const it = CharmEditor.current.result().faces.front.items.find(i => i.id === id), j = __ce.job(), fr = CharmNestArtEdit.frame(j.view); let b = null; for (const sp of it.subpaths) for (const o of sp) for (const p of o.slice(1)) { const q = fr.toSource(p); b = b ? [Math.min(b[0], q[0]), Math.min(b[1], q[1]), Math.max(b[2], q[0]), Math.max(b[3], q[1])] : [q[0], q[1], q[0], q[1]]; } return b; }, t25.id);
    const res = await page.evaluate(() => CharmEditor.current.result());
    const fc = res.changes.front;
    check('B8 what the editor hands back: the three letters removed, SEPTEMBER and 25 added (as outlines), nothing else changed, the back untouched',
      fc.removed.slice().sort().join() === letters.slice().sort().join() && fc.added.map(a => a.text).sort().join() === '25,SEPTEMBER' && !Object.keys(fc.changed).length && fc.added.every(a => a.subpaths.length > 1) && res.changes.back.added.length === 0,
      JSON.stringify({ removed: fc.removed, added: fc.added.map(a => a.text), changed: Object.keys(fc.changed) }));

    const sqLine = squares[0];   // (the top-left square: its top and left lines are its own, the others it shares with a neighbour drawn over them)
    await page.keyboard.press('Escape'); await page.keyboard.press('Control+0'); await settleP(page);
    { const p = await clientOf(page, sqLine.b[0], (sqLine.b[1] + sqLine.b[3]) / 2); await page.mouse.click(p.x, p.y); await settleP(page); }
    const lw0 = await page.evaluate(() => document.querySelector('.ce-root [data-f="lw"]').value), lwSel = await page.evaluate(() => CharmEditor.current.selection());
    await typeField(page, 'lw', '0.05');
    await page.keyboard.press('Control+2'); await settleP(page);   // (zoomed to the square: the thin line next to its neighbours' own)
    await page.keyboard.press('Escape'); await page.mouse.move(4, 360); await settleP(page);
    const lwNow = await page.evaluate(id => CharmEditor.current.items().find(i => i.id === id).lwPt, sqLine.id);
    const drawnW = await page.evaluate(([id, z]) => [parseFloat(getComputedStyle(document.querySelector(`.ce-items path[data-id="${id}"]`)).strokeWidth), CharmEditor.current.zoom()], [sqLine.id, 0]);
    await R.shot(w, 'CE-4c-grid-line-thinned');
    const fc2 = (await page.evaluate(() => CharmEditor.current.result())).changes.front;
    check(`B8b a day square's red line reads ${lw0} mm in the Stroke field (its own 0.3 pt) and is thinned to 0.05 mm, drawn at exactly that (${drawnW[0].toFixed(2)} px at ${Math.round(drawnW[1])} %); it is now the one part changed`,
      lwSel.join() === sqLine.id && lw0 === '0.106' && Math.abs(lwNow - 0.05 * PT) < 1e-6 && Math.abs(drawnW[0] - lwNow * drawnW[1] / 100 * 96 / 72) < 1e-3 && drawnW[1] > 1000 && Object.keys(fc2.changed).join() === sqLine.id,
      JSON.stringify({ lw0, lwNow, lwSel, drawnW, changed: Object.keys(fc2.changed) }));

    /* ── Save ── */
    const puts0 = artPuts();
    await page.click('.ce-root [data-act="save"]');
    for (let t0 = Date.now(); Date.now() - t0 < 60000 && !st.doc('Charm_Pool_Art', PID + '~front'); ) await new Promise(r => setTimeout(r, 200));
    // the face is fitted again around its new drawing: wait for it (and the card drawn again)
    await page.waitForFunction(id => { const f = [...Engrave.items().values()].find(x => x.face === 'front' && (x.copies || []).includes(id)); return f && f.view && !['ready', 'fitting'].includes(f.state); }, PID, { timeout: 120000 }).catch(() => {});
    await R.openEngraveDetail(w, 'cal', { face: 'front' }); await R.settle(page, 1200);
    // (27 Oct 2026: the wait above can pass on the face's state from BEFORE the save, and the card is then drawn again under the next read: it fails so on main too, 2 runs in 3. Wait for its drawing)
    await page.waitForFunction(() => { const c = document.querySelector('#egQueue .rvItem[data-kind=placement] .backHost canvas'); return !!(c && c._frontArt); }, null, { timeout: 60000 }).catch(() => {});
    const rec = st.doc('Charm_Pool_Art', PID + '~front');
    const addedIds = [sep0.id, t25.id];
    check('B9 the cloud keeps the edit for this piece: who, the three letters removed, SEPTEMBER and 25 added as blue-hatched outlines in the piece\'s own points, one save',
      rec && rec.by === 'Test Operator' && rec.removed.slice().sort().join() === letters.slice().sort().join() && rec.added.map(a => a.id).sort().join() === addedIds.slice().sort().join() &&
      rec.added.every(a => a.role === 'hatch' && a.fill && a.fill.join() === '0,0,1' && /^M/.test(a.d)) && rec.added.find(a => a.id === sep0.id).text === 'SEPTEMBER' && artPuts() === puts0 + 1 && !st.doc('Charm_Pool_Art', PID),
      JSON.stringify(rec && { by: rec.by, removed: rec.removed, added: rec.added.map(a => [a.id, a.role, a.text, a.fill]) }));
    const e25 = rec && rec.added.find(a => a.id === t25.id), got25 = e25 && (() => { const pts = []; for (const sp of kit.AE.fromD(e25.d)) for (const o of sp) for (const p of o.slice(1)) pts.push(p); return [Math.min(...pts.map(p => p[0])), Math.min(...pts.map(p => p[1])), Math.max(...pts.map(p => p[0])), Math.max(...pts.map(p => p[1]))]; })();
    const thinRec = rec && rec.changed && rec.changed[sqLine.id];
    check('B9b …the 25 is kept with its weight (+0.04 mm) and the outlines AT that weight (the editor\'s, in the piece\'s own points, to 0.002 pt); the thinned day square is kept as a change with its width, 0.142 pt (0.05 mm)',
      !!e25 && e25.weightMm === 0.04 && got25.every((v, i) => Math.abs(v - want25[i]) < 0.002) && !!thinRec && thinRec.lwPt === 0.142 && thinRec.role === 'line' && thinRec.stroke && thinRec.stroke.join() === '1,0,0' && Object.keys(rec.changed).length === 1,
      JSON.stringify({ w: e25 && e25.weightMm, got25, want25, thin: thinRec && { lw: thinRec.lwPt, role: thinRec.role, stroke: thinRec.stroke }, changed: rec && Object.keys(rec.changed || {}) }));
    const after = await page.evaluate(([id, id2, added, gone]) => {
      const ch = Pool.charmOf(id), j = __ce.job(), cv = __ce.cv();
      const art = cv._frontArt.map(s => s.original || s), artIds = art.map(m => m && m.artMemberId);
      return { hash: ch.hash, addedOn: added.every(a => ch.members.some(m => m.artMemberId === a && m.artAdded && m.hatchBlue)), goneOff: gone.every(g => !ch.members.some(m => m.artMemberId === g)), master: ch.artFrom && ch.artFrom.hash,
        other: Pool.charmOf(id2).hash, otherClean: Pool.charmOf(id2).members.every(m => !m.artAdded && !m.artRedraw),
        artNew: added.every(a => artIds.includes(a)), artGone: gone.every(g => !artIds.includes(g)), artN: art.length, face: j.face, state: j.state, approvedAt: j.approvedAt, fitted: !!j.fit,
        note: (__ce.card().querySelector('.backHost .egArtNote') || {}).textContent || '', noteClear: (() => { const n = __ce.card().querySelector('.backHost .egArtNote'); return !!n && n.getBoundingClientRect().top >= cv.getBoundingClientRect().bottom; })(), row: (B.pool.rows.get(id) || {}).artEdit };
    }, [PID, PID2, addedIds, letters]);
    const workShot = await R.shot(w, 'CE-5-work-area-after-save');
    check('B10 the piece\'s charm carries the new parts, without MAY, with a hash of its own ("~ae"); the shared master and the other calendar are untouched',
      after.addedOn && after.goneOff && /~ae[0-9a-z]+$/.test(after.hash) && after.master === master.hash && after.other === master.hash && after.otherClean && after.row && after.row.front === rec.at, JSON.stringify(after));
    check('B11 the work area draws the new front art at once: SEPTEMBER and 25 among its art, MAY gone (the face fitted again around the new drawing)', after.artNew && after.artGone && after.artN === 22 && after.face === 'front', JSON.stringify({ artN: after.artN, artNew: after.artNew, artGone: after.artGone, state: after.state }));
    const fb = st.doc('Charm_Pool_Back', PID + '~front');
    check('B12 the approved front is withdrawn in Engrave.artChanged\'s words: its record invalidated, the face waits for Approve again, and the work area\'s note (under the drawing, never over the art) says so',
      fb && fb.invalidated === true && after.approvedAt == null && after.state === 'review' && after.note.includes("This charm's front changed since the words were approved — fit the words again") && /^Front artwork saved for this piece/.test(after.note) && after.noteClear,
      JSON.stringify({ invalidated: fb && fb.invalidated, state: after.state, note: after.note, clear: after.noteClear }));
    const thumb1 = await page.evaluate(() => __ce.thumb('front'));
    check('B13 the card\'s Front thumbnail is drawn again', !!thumb0 && !!thumb1 && thumb0 !== thumb1, thumb0 + ' → ' + thumb1);

    await R.openEngraveDetail(w, 'cal', { face: 'back' }); await R.settle(page, 800);
    const bk = await page.evaluate(([added, gone, id]) => { const cv = __ce.cv(), j = __ce.job(), ids = cv._frontArt.map(s => (s.original || s).artMemberId); return { face: j.face || 'back', fit: JSON.stringify([j.state, j.fit.size, j.fit.centre, j.fit.angle]), newOn: added.every(a => ids.includes(a)), goneOff: gone.every(g => !ids.includes(g)), pill: !!__ce.card().querySelector('.backHost .egCharmEdit') }; }, [addedIds, letters, PID]);
    check('B14 the back keeps its words where they were (its fit untouched), and "Show front" there shows the new front, not MAY', bk.face === 'back' && bk.fit === backFit0 && bk.newOn && bk.goneOff && bk.pill, JSON.stringify(Object.assign(bk, { backFit0 })));
    await R.openEngraveDetail(w, 'cal', { face: 'front' }); await R.settle(page, 800);

    /* ── the laser's file ── */
    const sheet1 = await page.evaluate(ids => __ce.sheet(ids), [PID, PID2]);
    const f0 = await formsOf(kit.L, Buffer.from(sheet0, 'base64')), f1 = await formsOf(kit.L, Buffer.from(sheet1, 'base64'));
    const p0 = await page.evaluate(b => __ce.parts(b), sheet0), p1 = await page.evaluate(b => __ce.parts(b), sheet1);
    const blue = fs => fs.filter(f => f[0] === '0,0,1'), inside = (b, q) => b[0] > q[0] && b[1] > q[1] && b[2] < q[2] && b[3] < q[3];
    const nb = blue(p1[0].fills), wide = nb.find(f => f[1][2] - f[1][0] > 25), small = nb.find(f => f[1][2] - f[1][0] < 8), inSq = small && p1[0].lines.filter(q => inside(small[1], q));
    check('B15 CharmNestPDF.buildSheet: the other calendar\'s drawing is byte for byte what it was (its form\'s stream and resources); the edited piece is written anew',
      f1.Charm1 && f1.Charm1.content === f0.Charm1.content && f1.Charm1.res === f0.Charm1.res && f1.Charm0.content !== f0.Charm0.content, Object.keys(f1).join());
    check('B16 …and it writes the new month and the date as blue hatching: MAY\'s three hatched letters gone, SEPTEMBER across the top and 25 inside one of the 20 day squares (the other calendar keeps its three letters)',
      blue(p0[0].fills).length === 3 && nb.length === 2 && p1[0].fills.every(f => f[0] === '0,0,1') && !!wide && !!small && inSq.length === 1 && p1[0].lines.length === 20 &&
      !p0[0].fills.some(f => nb.some(g => JSON.stringify(g[1]) === JSON.stringify(f[1]))) && JSON.stringify(p1[1]) === JSON.stringify(p0[1]),
      JSON.stringify({ before: blue(p0[0].fills).length, after: nb.map(f => f[1].map(v => +v.toFixed(1))), squares: p1[0].lines.length, inSq: inSq && inSq.length }));
    { // (27 Oct 2026) the file engraves what the editor drew: the 25's outlines at their weight, the thinned square at its width (the sheet sets the charm's middle at 50, 130 - 60, unturned)
      const where = await page.evaluate(([id, sq]) => { const ch = Pool.charmOf(id), m = ch.members.find(x => x.artMemberId === sq); return { c: ch.centerPt, sq: m && m.bbox, lw: m && m.lwPt }; }, [PID, sqLine.id]);
      const on = b => [b[0] + 50 - where.c[0], b[1] + 130 - 60 - where.c[1], b[2] + 50 - where.c[0], b[3] + 130 - 60 - where.c[1]];
      const thinAt = p1[0].lws.map((v, i) => (Math.abs(v - 0.142) < 1e-6 ? i : -1)).filter(i => i >= 0), at25 = on(want25);
      check('B16b …at the weight and the width the editor showed: the 25\'s blue hatching is exactly its outlines at +0.04 mm (box to 0.003 pt), and the thinned square is written at 0.142 pt where it is, the other 19 at their own 0.3 pt (all 20 were 0.3 before)',
        !!small && small[1].every((v, i) => Math.abs(v - at25[i]) < 0.003) && thinAt.length === 1 && p1[0].lines[thinAt[0]].every((v, i) => Math.abs(v - on(where.sq)[i]) < 0.003) && Math.abs(where.lw - 0.142) < 1e-9 &&
        p1[0].lws.filter(v => Math.abs(v - 0.3) < 1e-6).length === 19 && p0[0].lws.every(v => Math.abs(v - 0.3) < 1e-6),
        JSON.stringify({ small: small && small[1], at25, thinAt, lws: p1[0].lws.map(v => +v.toFixed(3)), lws0: p0[0].lws.map(v => +v.toFixed(3)) }));
    }

    /* ── Cancel writes nothing ── */
    const recAt = rec.at, puts1 = artPuts();
    await page.click(Q + ' .backHost .egCharmEdit');
    await page.waitForSelector('.ce-root .ce-items path', { timeout: 60000 }); await settleP(page);
    const re = await page.evaluate(([gone]) => { const its = CharmEditor.current.items(); return { names: its.map(i => i.name || ''), sources: its.filter(i => i.source === 'added').map(i => i.name), gone: gone.every(g => !its.some(i => i.id === g)), n: its.length }; }, [letters]);
    await R.shot(w, 'CE-6-reopened');
    check('B17 the editor opens again on the saved edit: SEPTEMBER and 25 are layers of their own (added here), MAY is not there', re.sources.includes('SEPTEMBER') && re.sources.includes('25') && re.gone && re.n === 23, JSON.stringify(re));
    { // (27 Oct 2026) opened again, the 25 says its weight and the square its width: picked from their rows, the bar reads them
      const pick = async id => { await page.locator(`.ce-root .ce-layer[data-id="${id}"] .ce-layer__name`).scrollIntoViewIfNeeded(); await page.locator(`.ce-root .ce-layer[data-id="${id}"] .ce-layer__name`).click(); await settleP(page); };
      await pick(t25.id);
      const r25 = await page.evaluate(id => ({ it: CharmEditor.current.items().find(i => i.id === id), wt: document.querySelector('.ce-root [data-f="wt"]').value, shown: !document.querySelector('.ce-root [data-when="weight"]').hidden }), t25.id);
      await pick(sqLine.id);
      const rsq = await page.evaluate(id => ({ it: CharmEditor.current.items().find(i => i.id === id), lw: document.querySelector('.ce-root [data-f="lw"]').value }), sqLine.id);
      await page.keyboard.press('Escape'); await settleP(page);
      check('B17b …the 25 comes back as its outlines saying their weight, +0.04 mm in the Weight field (a new weight grows them by the difference), and the thinned square says 0.05 mm in the Stroke field',
        r25.it && r25.it.weightMm === 0.04 && r25.shown && r25.wt === '+0.04' && rsq.it && Math.abs(rsq.it.lwPt - 0.142) < 1e-9 && rsq.lw === '0.05', JSON.stringify({ r25, rsq: { lw: rsq.lw, lwPt: rsq.it && rsq.it.lwPt } }));
    }
    await page.keyboard.press('v');
    const sepId = await page.evaluate(() => CharmEditor.current.items().find(i => i.name === 'SEPTEMBER').id);
    await page.evaluate(id => { const b = CharmEditor.current.box(id), p = CharmEditor.current.clientOf((b[0] + b[2]) / 2, (b[1] + b[3]) / 2); window.__p = p; }, sepId);
    const sp0 = await page.evaluate(() => window.__p);
    await page.mouse.click(sp0.x, sp0.y); await settleP(page);
    await page.keyboard.press('Delete'); await settleP(page);
    await page.click('.ce-root .ce-foot [data-act="cancel"]'); await settleP(page);
    const asked = await page.evaluate(() => !!document.querySelector('.ce-ask'));
    await page.click('.ce-ask [data-ask="yes"]'); await settleP(page); await R.settle(page, 500);
    const c1 = await page.evaluate(id => ({ open: !!document.querySelector('.ce-root'), hash: Pool.charmOf(id).hash }), PID);
    check('B18 Cancel (after deleting SEPTEMBER, and saying yes to "Leave without saving?") writes nothing: no save sent, the record and the piece as they were',
      asked && !c1.open && artPuts() === puts1 && st.doc('Charm_Pool_Art', PID + '~front').at === recAt && c1.hash === after.hash, JSON.stringify({ asked, c1, puts: artPuts() - puts1 }));

    /* ── a second edit keeps the first; a part off the metal is refused and nothing is saved ── */
    await page.click(Q + ' .backHost .egCharmEdit');
    await page.waitForSelector('.ce-root .ce-items path', { timeout: 60000 }); await settleP(page);
    await page.keyboard.press('t');
    const sq2 = squares[0].b, at2 = await clientOf(page, sq2[0] + 0.6 * PT, sq2[3] - 0.8 * PT);
    await page.mouse.click(at2.x, at2.y); await settleP(page);
    await page.keyboard.type('1'); await page.keyboard.press('Enter'); await settleP(page);
    await typeField(page, 'pt', '5');
    const t1 = await findText(page, '1');
    await page.click('.ce-root [data-act="save"]');
    for (let t0 = Date.now(); Date.now() - t0 < 60000 && st.doc('Charm_Pool_Art', PID + '~front').at === recAt; ) await new Promise(r => setTimeout(r, 200));
    await R.settle(page, 800);
    const rec2 = st.doc('Charm_Pool_Art', PID + '~front');
    check('B19 a second edit (a "1" in the first square) is saved with the first: MAY still removed, SEPTEMBER, 25 and 1 added',
      rec2.at !== recAt && rec2.removed.slice().sort().join() === letters.slice().sort().join() && rec2.added.map(a => a.id).sort().join() === addedIds.concat([t1.id]).sort().join() && rec2.added.find(a => a.id === sep0.id).d === rec.added.find(a => a.id === sep0.id).d,
      JSON.stringify({ removed: rec2.removed, added: rec2.added.map(a => [a.id, a.text]) }));
    await R.openEngraveDetail(w, 'cal', { face: 'front' }); await R.settle(page, 900);
    const puts2 = artPuts(), rec2At = rec2.at;
    await page.click(Q + ' .backHost .egCharmEdit');
    await page.waitForSelector('.ce-root .ce-items path', { timeout: 60000 }); await settleP(page);
    const fr2 = await page.evaluate(() => CharmEditor.current.frame());
    await page.keyboard.press('Control+0'); await settleP(page);
    await page.keyboard.press('t');
    const off = await clientOf(page, fr2.x0 + fr2.wPt + 3 * PT, fr2.y0 + fr2.hPt / 2);
    await page.mouse.click(off.x, off.y); await settleP(page);
    await page.keyboard.type('X'); await page.keyboard.press('Enter'); await settleP(page);
    await page.keyboard.press('Escape'); await page.keyboard.press('v');
    await page.click('.ce-root .ce-head [data-face="back"]'); await settleP(page);
    const bfr = await page.evaluate(() => CharmEditor.current.frame());
    await page.keyboard.press('t');
    const inB = await clientOf(page, bfr.x0 + bfr.wPt / 3, bfr.y0 + bfr.hPt / 2);
    await page.mouse.click(inB.x, inB.y); await settleP(page);
    await page.keyboard.type('HI'); await page.keyboard.press('Enter'); await settleP(page);
    await page.click('.ce-root [data-act="save"]'); await settleP(page); await R.settle(page, 1200);
    const n3 = await page.evaluate(() => { const n = document.querySelector('#egQueue .rvItem[data-kind=placement] .backHost .egArtNote'); return { text: n ? n.textContent : '', bad: !!(n && n.classList.contains('bad')), open: !!document.querySelector('.ce-root') }; });
    await R.shot(w, 'CE-7-refused-note');
    check('B20 a word put off the metal is refused in the work area\'s note, in the module\'s words, and nothing of that edit is saved: not the front, not the back\'s valid addition',
      n3.bad && /^Not saved/.test(n3.text) && /X: it falls outside the piece's outline/.test(n3.text) && !n3.open && artPuts() === puts2 && st.doc('Charm_Pool_Art', PID + '~front').at === rec2At && !st.doc('Charm_Pool_Art', PID),
      JSON.stringify({ n3, puts: artPuts() - puts2 }));
    /* ── a part hidden with the eye is not engraved (what the person sees is what the laser does) ── */
    await R.openEngraveDetail(w, 'cal', { face: 'front' }); await R.settle(page, 900);
    await page.click(Q + ' .backHost .egCharmEdit');
    await page.waitForSelector('.ce-root .ce-items path', { timeout: 60000 }); await settleP(page);
    const line = await page.evaluate(() => { const it = CharmEditor.current.items().find(i => i.role === 'line' && i.source === 'master'); return it ? String(it.id) : null; });
    const eyeOf = id => page.evaluate(id => { const b = document.querySelector(`.ce-root .ce-layer[data-id="${id}"] [data-lact="eye"]`); return b ? b.title : ''; }, id), eye0 = await eyeOf(line);
    await page.click(`.ce-root .ce-layer[data-id="${line}"] [data-lact="eye"]`); await settleP(page);
    const eye = await page.evaluate(id => { const b = document.querySelector(`.ce-root .ce-layer[data-id="${id}"] [data-lact="eye"]`); return b ? b.title : ''; }, line);
    const rec3At = st.doc('Charm_Pool_Art', PID + '~front').at;
    await page.click('.ce-root [data-act="save"]');
    for (let t0 = Date.now(); Date.now() - t0 < 60000 && st.doc('Charm_Pool_Art', PID + '~front').at === rec3At; ) await new Promise(r => setTimeout(r, 200));
    await R.settle(page, 800);
    const rec3 = st.doc('Charm_Pool_Art', PID + '~front');
    check('B21 a grid line hidden with the eye ("Hide: not engraved") is saved as removed: the laser does what the person sees; the earlier edits stay',
      !!line && eye0 === 'Hide: not engraved' && eye === 'Show: engraved again' && rec3.at !== rec3At && rec3.removed.includes(line) && letters.every(l => rec3.removed.includes(l)) && rec3.added.length === rec2.added.length,
      JSON.stringify({ line, eye0, eye, removed: rec3.removed, added: rec3.added.length }));
    check('B22 no page error', w.errors.length === 0, JSON.stringify(w.errors).slice(0, 400));
    console.log('screenshots in ' + shots + ' (' + [entryShot, workShot].map(p => path.basename(p)).join(', ') + ', …)');
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
