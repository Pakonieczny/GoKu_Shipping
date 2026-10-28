// CHARM EDIT · PAUL'S REAL CASES (28 Oct 2026), on the real masters read from the live site (read-only: masterGet, and artList for his saved record) and seeded into the offline world
// (_engraveRealWorld.cjs: the real sorter page over the repo's fake site; made-up names, nothing written anywhere real):
//   fixtures/PERCHED_43564.ai   the drawing of SILHOUETTE_34560 (order 4211572095: a bird, its hatch and three cut lines; engraved on the BACK, "T")
//   fixtures/TINY_BAR_7427.ai   the drawing of TINY BAR_7427 (order 4203083230: engraved on the FRONT, "33.47765N 86.80020W" in Angelina)
//   fixtures/tinybar-front-art-4203083230.json   the front artwork record Paul saved for that bar (Charm_Pool_Art): his photo traced in 8 greys, 250 KB of paths
//   A  EDITSAVE ("Not saved · Hatched part: it falls outside the piece's outline … · Cut line: …", the editor closed and his picture lost): A0 (node) the bird's front handed back "changed" but
//      untouched (as given, and with its matrix written out again) is no change and is never refused (it read exactly Paul's "Cut line" refusal before); a part really moved still is. On the page:
//      the bird opened from its BACK card, a real picture traced and placed on its FRONT, grouped with the bird's own hatch: saved, only the picture in the record (A1); a part off the metal: the
//      editor stays OPEN with the work and says which part and why, nothing sent (A2), and once it is deleted the save is kept (A3); a save the cloud refuses after the editor closed opens the
//      editor again on the work with the reason (A4), and Save then keeps it (A5)
//   B  APPROVEHIT ("Pressing the green Approve does nothing"): his photo on the bar's front saved in the Charm Editor, then ONE real mouse press on Approve (a press at the button's place on the
//      screen, not a script's click: what was missed before) approves the words and the art and writes the front's file with every grey in it; the same on the bird's BACK after a part was
//      added there in the editor
//   C  FACEVIEW ("Engravings and cut-lines should always be visible … when viewing the front … all of the work … performed in the Charm Editor must 100% be showing"): the bird's back draws no
//      front art laid over it, its own added part, a Front | Back switch, and the card's thumbnails show its back and the front the Charm Editor drew on (C1); Front shows the front as the front,
//      the master's hatch and every part of the editor, view only (C2); a dot added on the front from the back's card shows the front with it the moment it is saved (C3); the bar's front draws
//      Paul's photo with the words (C4). C runs after A and B (it looks at what they saved)
//   D  the bar after B1 (Paul, 28 Oct 2026, order 4203083230): MADEPV ("In this view the vectorized image does not show up in the right side preview"): the order window's Sheet tab, its FRONT
//      ENGRAVING card's preview draws the photo's greys beside the words, read by its pixels (D1), and still does once the upkeep let go of the written placement (D2: it drew the words-only saved
//      picture); EDITDESIGN ("When I press the 'Edit Design' button ... the engraving text is not present"): the order window's Edit Design opens the piece itself, its front words ONE editable
//      text of the engraving, the photo with them (D4), and a part added there is saved for the piece, the words staying the engraving's, never copied into its art (D5)
//   T  TYPEWORDS ("When I select existing text on the charm that text is not recognized as text, it's just an object on the charm. The user also needs the ability to update existing text, even if
//      that text came from back engraving modal"; the live record of that bar held his retyped words "…W愛" as an outline part "engrave-words-front-mvsjt11z" and the card had lost them): T0 (node) the
//      repair rule and the glyph comparison (charm-nest-ce-words.js); the words are Type from the card, a click reads "Type" on the Control bar with Character in Properties (T1), and from the order
//      window's Open in Charm Editor and Edit Design while approved and shelved (T2); that very state (the outline part, the card's words taken off) opens as the engraving's Type and Save gives the
//      words back to the job, out of the art (T3); a copy on top of the words is dropped (T4); words retyped on an approved face go to the engraving, fitted again, back to its Approve, nothing in the
//      art (T5); moved, the fit moves as far (T6); his own record (fixtures/tinybar-front-words-4203083230.json) read back as Mynerve 2.889 pt from its outlines, the icon kept (T7)
//   PW_DIR=<playwright node_modules> CHROMIUM=<chrome> node tests/charm-nest/charm-edit-real-cases.cjs      (CR_ONLY=A0, A, B, C, D, T or T0 (D and T run B first); SHOTS=<dir> for the screenshots)
'use strict';
const path = require('node:path'), fs = require('node:fs');
const ROOT = path.join(__dirname, '../..'), FIX = path.join(__dirname, 'fixtures');
const RESULTS = [];
const check = (id, ok, evidence) => { RESULTS.push({ id, ok: !!ok }); console.log(`${ok ? 'PASS' : 'FAIL'}  ${id}  ${ok ? '' : evidence || ''}`.trimEnd()); };
const ONLY = process.env.CR_ONLY || '';
const Q = '#egQueue .rvItem[data-kind=placement]';
const ART = JSON.parse(fs.readFileSync(path.join(FIX, 'tinybar-front-art-4203083230.json'), 'utf8'));
const WORDS = JSON.parse(fs.readFileSync(path.join(FIX, 'tinybar-front-words-4203083230.json'), 'utf8'));   // (TYPEWORDS: the two parts the live record holds beside those greys: his words as outlines, an icon)
const __norm = s => String(s == null ? '' : s).replace(/\s+/g, ' ').trim();
const settleP = page => page.evaluate(() => new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r))));

/** The two real masters and the two orders, as Paul's (the index entries as masterGet returned them; the buyers made up). */
function seedWorld(R) {
  R.DESIGNS.SILHOUETTE_34560 = { file: 'PERCHED_43564.ai', entry: { sku: 'SILHOUETTE_34560', widthPt: 36.515260000000126, heightPt: 31.177639999999883, areaPt2: 449.3611111111111, members: 4, holes: 2, engravable: true, engravableBy: 'index', upAngle: 267.50234443756403, upSource: 'hole', charmHash: '831b2d80', backKeepOut: [] } };
  R.DESIGNS['TINY BAR_7427'] = { file: 'TINY_BAR_7427.ai', entry: { sku: 'TINY BAR_7427', sym: 'symmetric', widthPt: 50.956999999999994, heightPt: 11.25, areaPt2: 420.6388888888889, members: 1, holes: 0, engravable: true, engravableBy: 'index', upAngle: 89.852, upSource: 'long', charmHash: '9ac1e42a', backKeepOut: [] } };
  R.ORDERS.sil = Object.assign({}, R.ORDERS.back, { rid: '4290000141', sku: 'SILHOUETTE_34560', listing: '1890000141', title: 'Perched Bird Silhouette Necklace in Sterling Silver', buyer: 'Name Twelve', personalization: ['T'] });
  R.ORDERS.bar = { rid: '4290000151', sku: 'TINY BAR_7427', listing: '1890000151', title: 'Tiny Bar Necklace, Personalized Coordinates Bar Necklace in Sterling Silver', buyer: 'Name Thirteen', placed: { day: 0, h: 10, m: 20 }, ship: 5,
    variations: [{ name: 'Metal choice', value: 'Silver' }, { name: 'Chain length', value: '16"' }, { name: 'Font', value: 'Angelina' }], personalization: ['33.47765N 86.80020W'] };
  R.EXPECT_JOBS.sil = 1; R.EXPECT_JOBS.bar = 1;
}

/* ═══ A0 · the model, on the bird's real drawing ═══ */
async function partModel() {
  global.window = global; global.self = global;
  global.PDFLib = require(path.join(ROOT, 'vendor/pdf-lib-1.17.1.min.js')); require(path.join(ROOT, 'charm-nest-pdf.js'));
  global.CharmNestVector = require(path.join(ROOT, 'charm-nest-vector.js')); global.CharmNestGeom = require(path.join(ROOT, 'charm-nest-geom.js'));
  const AE = require(path.join(ROOT, 'charm-nest-art-edit.js')), CE = require(path.join(ROOT, 'charm-nest-charm-editor.js')), P = global.CharmNestPDF, G = global.CharmNestGeom, M = CE.model;
  const parsed = await P.parseSource(fs.readFileSync(path.join(FIX, 'PERCHED_43564.ai')), 'PERCHED_43564.ai'), charm = P.groupCharms(parsed).charms[0];
  const view = G.backView(charm, G.viewOptionsFor({ charmUp: charm.upAngle, entry: { upAngle: 267.5 }, face: 'front' })), items = AE.itemsOf(charm, view, 'front');
  const enc = (changed, placed) => { try { return { rec: AE.encode({ removed: [], changed, added: [] }, { charm, view, face: 'front', placed }) }; } catch (e) { return { refused: e.refused }; } };
  // every part of the front handed back "changed" exactly as given; then each with a matrix written again (a group's turn about a point and its inverse on the part: the same drawing)
  const plain = {}; for (const it of items) plain[it.id] = Object.assign({}, it);
  const Gm = M.mul(M.tr(-7, 3), M.mul(M.rotM(33), M.tr(7, -3))), Gi = M.inv(Gm), wrapped = {};
  for (const it of items) wrapped[it.id] = Object.assign({}, it, { matrix: M.mul(Gi, Gm).map(v => v + 1e-12) });
  const a = enc(plain, true), b = enc(wrapped, true);
  // a real change is still a change: the hatch moved a millimetre off its place stays refused off the metal only when it leaves the metal, the cut line moved on a placed piece is refused
  const hatch = items.find(i => i.role === 'hatch'), cut = items.find(i => i.role === 'cut' && i.name !== 'x');
  const moved = enc({ [hatch.id]: Object.assign({}, hatch, { tx: 2.835 }), [cut.id]: Object.assign({}, cut, { tx: 2.835 }) }, true);
  check(`A0 the bird's front (${items.length} parts: ${items.map(i => i.role).join(', ')}) handed back "changed" but untouched, as given and with its matrix written out again: no change in the record and nothing refused (it read "Hatched part … · Cut line: a cut line is the shape the sheet was nested with" for Paul); a part really moved is still a change (${moved.refused ? moved.refused.map(r => r.id === 'cut' ? 'cut line refused' : 'hatch ' + r.why.slice(0, 20)).join(', ') : 'kept'})`,
    a.rec && !Object.keys(a.rec.changed).length && !a.rec.cut && b.rec && !Object.keys(b.rec.changed).length && !b.rec.cut && !!moved.refused && moved.refused.some(r => r.id === 'cut'),
    JSON.stringify({ a: a.refused || Object.keys(a.rec.changed), b: b.refused || Object.keys(b.rec.changed), moved }).slice(0, 600));
  delete global.window; delete global.self;
}

async function partPage() {
  const R = require('./_engraveRealWorld.cjs'); seedWorld(R);
  const shots = process.env.SHOTS || '/tmp/charm-edit-real-cases-shots';
  const w = await R.world({ viewport: { width: 1440, height: 900 }, shots });
  if (!w) { console.log('SKIP no playwright-core'); return; }
  await w.page.waitForFunction(() => !window.CNRole || ['ask', 'role', 'admin'].includes(CNRole.state()), null, { timeout: 20000 }).catch(() => {});
  await w.page.evaluate(() => { try { if (window.CNRole && CNRole.state() === 'ask') CNRole.choose('design'); } catch (_) { /* no role step on this page */ } });   // (the "Laser or Design?" bar covers buttons)
  const page = w.page, st = w.srv.st;
  const until = async (fn, ms) => { const t0 = Date.now(); for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) return null; await new Promise(r => setTimeout(r, 200)); } };
  const artPuts = () => st.calls.filter(x => x.name === 'charmNestLibrary' && x.op === 'artPut').length;
  /** What a real press on Approve meets: the element at the button's middle on the screen, and the button. */
  const approveHit = () => page.evaluate(Q => { const b = document.querySelector(Q + ' [data-a="approve"]'); if (!b) return null; const r = b.getBoundingClientRect(), t = document.elementFromPoint(r.left + r.width / 2, r.top + r.height / 2); return { top: t === b || b.contains(t), on: t ? String(t.className || t.tagName).slice(0, 40) : null, wide: !!b.closest('.pvMain.egWide'), dis: b.disabled }; }, Q);
  const jobOf = () => page.evaluate(() => { const c = document.querySelector('#egQueue .rvItem[data-kind=placement]'), j = c && Engrave.items().get(c.dataset.key); return j ? { key: j.key, pid: j.copies[0], face: j.face === 'front' ? 'front' : 'back', state: j.state, words: (j.lines || []).join(' ') } : null; });
  const saveEditor = async () => { const n0 = artPuts(); await page.click('.ce-root [data-act="save"]'); await until(async () => artPuts() > n0 && !(await page.evaluate(() => !!document.querySelector('.ce-root'))), 90000); return until(() => page.evaluate(() => { const n = document.querySelector('#egQueue .rvItem[data-kind=placement] .egArtNote'); return n && !/^Saving/.test(n.textContent) ? n.textContent : null; }), 90000); };
  try {
    const made = await R.build(w, ['bar', 'sil']);
    const sheets = await page.evaluate(() => { const out = []; for (const s of allSheets()) if (s.charms && s.charms.length) { if (!s.folderPath) s.folderPath = 'charmnest/sheets/' + s.sheetId; out.push({ sheetId: s.sheetId, poolIds: s.charms.map(x => x.poolId) }); } return out; });
    for (const s of sheets) st.put('Charm_Nest_Sheets', s.sheetId, { sheetId: s.sheetId, poolIds: s.poolIds, backPool: [] });
    check(`B0 the world: the bar is engraved on its front ("${made.bar.jobs.map(j => j.text).join()}", ${made.bar.jobs.map(j => j.fontKey).join()}), the bird on its back ("${made.sil.jobs.map(j => j.text).join()}"), both fitted, on a sheet`,
      made.bar.jobs.length === 1 && made.bar.jobs[0].face === 'front' && made.bar.jobs[0].fitted && made.sil.jobs.length === 1 && made.sil.jobs[0].face === 'back' && made.sil.jobs[0].fitted, JSON.stringify({ bar: made.bar.jobs, sil: made.sil.jobs }));

    if (!ONLY || ONLY === 'A') {
      /* ── A · EDITSAVE: the bird, opened from its BACK card, a picture placed on its FRONT ── */
      await page.evaluate(() => new Promise(res => { const cv = document.createElement('canvas'); cv.width = 300; cv.height = 220; const g = cv.getContext('2d'); g.fillStyle = '#fff'; g.fillRect(0, 0, 300, 220); for (let i = 0; i < 40; i++) { g.fillStyle = `rgb(${i * 6},${i * 6},${i * 6})`; g.beginPath(); g.arc(30 + (i * 37) % 260, 30 + (i * 53) % 170, 8 + i % 15, 0, 7); g.fill(); } cv.toBlob(b => { window.__pic = new File([b], 'photo.png', { type: 'image/png' }); res(); }, 'image/png'); }));
      const open = async () => { await R.openEngraveDetail(w, 'sil', { face: 'back' }); await R.settle(page, 700); await page.$eval(Q + ' .egCharmEdit', b => b.click()); await page.waitForSelector('.ce-root .ce-items path', { timeout: 60000 }); await settleP(page); };
      const toFront = async () => { await page.click('.ce-root [data-face="front"]'); await settleP(page); };
      const ed = () => page.evaluate(() => { const E = CharmEditor.current; if (!E) return null; const ids = []; const walk = l => l.forEach(i => { ids.push(i.id); if (i.children) walk(i.children); }); walk(E.items('front')); const r = document.querySelector('.ce-root .ce-refuse'); return { face: E.face(), ids, refuse: r ? r.textContent : '', sel: E.selection() }; });
      await open(); const s0 = await jobOf(); await toFront();
      await page.evaluate(() => { const st = document.querySelector('.ce-root .ce-stage'), r = st.getBoundingClientRect(), dt = new DataTransfer(); dt.items.add(__pic); const at = { bubbles: true, cancelable: true, clientX: r.left + r.width / 2, clientY: r.top + r.height / 2, dataTransfer: dt }; st.dispatchEvent(new DragEvent('dragenter', at)); st.dispatchEvent(new DragEvent('dragover', at)); st.dispatchEvent(new DragEvent('drop', at)); document.dispatchEvent(new DragEvent('dragleave', { bubbles: true, clientX: 0, clientY: 0 })); window.dispatchEvent(new DragEvent('dragend', { bubbles: true })); });   // (the page's own drop target saw the drag come in: it is told it left, as a real drag tells it)
      await page.waitForSelector('.ce-root .cei .cei__card', { timeout: 20000 });
      await page.waitForFunction(() => { const c = CharmNestCeImage.current(); return c && !c.working() && document.querySelector('.cei [data-wait]').hidden; }, null, { timeout: 30000 });
      await page.click('.cei [data-do="place"]'); await page.waitForFunction(() => !document.querySelector('.ce-root .cei .cei__card'), null, { timeout: 60000 }); await settleP(page);
      // (a group made round the bird's own hatch and the picture, ⌘G: the hatch's parent and place change, what it draws does not)
      const grouped = await page.evaluate(() => { const E = CharmEditor.current, img = E.items().find(i => i.kind === 'group'), hatch = E.items().find(i => i.role === 'hatch' && i.source === 'master'); E.ctx.select([hatch.id, img.id]); return { img: img.id, hatch: hatch.id, n: img.children.length }; });
      await page.keyboard.press('Control+g'); await settleP(page);
      const ch1 = await page.evaluate(() => { const r = CharmEditor.current.result().changes.front; return { changed: Object.keys(r.changed), added: r.added.length }; });
      const puts1 = artPuts(), note1 = await saveEditor();
      const rec1 = st.doc('Charm_Pool_Art', s0.pid + '~front');
      check(`A1 Paul's case: the bird opened from its BACK card, a picture traced and placed on its FRONT (${grouped.n} greys) and put in one group with the bird's own hatch (⌘G: the editor lists ${ch1.changed.length} master part(s) as changed): Save keeps the picture and nothing else ("${note1}"): no part of the master in the record, no cut line, the bird's hatch and cut lines untouched`,
        /^Front artwork saved/.test(note1 || '') && artPuts() === puts1 + 1 && !!rec1 && !Object.keys(rec1.changed || {}).length && !(rec1.removed || []).length && !rec1.cut && (rec1.added || []).length === grouped.n, JSON.stringify({ grouped, ch1, note1, rec: rec1 && { changed: Object.keys(rec1.changed || {}), removed: rec1.removed, cut: rec1.cut, added: (rec1.added || []).length } }));
      // A2 · a part that truly cannot be kept: the editor stays open and says which and why; the rest of the work stays
      await open(); await toFront();
      await page.evaluate(() => { const E = CharmEditor.current, f = E.frame(), x = f.x0 - 6, y = f.y0 + f.hPt * 0.5, s = 2; E.ctx.addItems([{ kind: 'path', role: 'hatch', name: 'Star off the metal', fill: true, stroke: false, subpaths: [[['m', [x, y - s]], ['l', [x + s, y + s]], ['l', [x - s, y + s]], ['h']]], tx: 0, ty: 0, sx: 1, sy: 1, rot: 0 }, { kind: 'path', role: 'hatch', name: 'Dot on the bird', fill: true, stroke: false, subpaths: [[['m', [f.x0 + f.wPt * 0.45, f.y0 + f.hPt * 0.55]], ['l', [f.x0 + f.wPt * 0.47, f.y0 + f.hPt * 0.55]], ['l', [f.x0 + f.wPt * 0.46, f.y0 + f.hPt * 0.57]], ['h']]], tx: 0, ty: 0, sx: 1, sy: 1, rot: 0 }], { select: false }); E.ctx.commit('two'); });
      const puts2 = artPuts();
      await page.click('.ce-root [data-act="save"]');
      const r2 = await until(async () => { const e = await ed(); return e && e.refuse ? e : null; }, 15000);
      await R.shot(w, 'CR-A2-editor-says-why');
      const offId = r2 && await page.evaluate(() => { let id = null; const walk = l => l.forEach(i => { if (i.name === 'Star off the metal') id = i.id; if (i.children) walk(i.children); }); walk(CharmEditor.current.items('front')); return id; });
      check(`A2 a part placed off the metal: Save leaves the editor OPEN with all the work (${r2 ? r2.ids.length : 0} parts on the front), says "${r2 ? r2.refuse.replace(/\s+/g, ' ').slice(0, 130) : ''}…", that part selected, and sends nothing`,
        !!r2 && /Not saved yet/.test(r2.refuse) && /Star off the metal/.test(r2.refuse) && /outside the piece's outline/.test(r2.refuse) && !!offId && r2.sel.includes(offId) && r2.ids.length >= 6 && artPuts() === puts2, JSON.stringify(r2));
      await page.click('.ce-root .ce-refuse [data-ask]'); await settleP(page);
      await page.keyboard.press('Delete'); await settleP(page);
      const note2 = await saveEditor(), rec2 = st.doc('Charm_Pool_Art', s0.pid + '~front');
      check(`A3 … Keep editing, the part deleted, Save: kept ("${note2}"), the dot with the earlier picture`, /^Front artwork saved/.test(note2 || '') && !!rec2 && rec2.added.length === grouped.n + 1 && rec2.added.some(a => a.id && !/^img/.test(a.id)), JSON.stringify({ note2, n: rec2 && rec2.added.length }));
      // A4 · the cloud refuses after the editor closed (another computer saved first): the editor opens again on the work, says why, and a second Save keeps it
      await open(); await toFront();
      await page.evaluate(() => { const E = CharmEditor.current, f = E.frame(), x = f.x0 + f.wPt * 0.5, y = f.y0 + f.hPt * 0.6; E.ctx.addItems([{ kind: 'path', role: 'hatch', name: 'Second dot', fill: true, stroke: false, subpaths: [[['m', [x, y]], ['l', [x + 0.6, y]], ['l', [x + 0.3, y + 0.6]], ['h']]], tx: 0, ty: 0, sx: 1, sy: 1, rot: 0 }], { select: false }); E.ctx.commit('dot');
        const AE = CharmNestArtEdit, real = AE.save; let once = true; AE.save = function () { if (once) { once = false; return Promise.reject(Object.assign(new Error("This piece's artwork was edited elsewhere. Reopen it before saving your changes."), { conflict: true })); } return real.apply(AE, arguments); }; window.__realSave = real; });
      await page.click('.ce-root [data-act="save"]');
      const r4 = await until(async () => { const e = await ed(); return e && /edited elsewhere/.test(e.refuse) ? e : null; }, 20000);
      await R.shot(w, 'CR-A4-reopened');
      const kept4 = r4 && r4.ids.length && await page.evaluate(() => { let n = 0; const walk = l => l.forEach(i => { if (i.name === 'Second dot') n++; if (i.children) walk(i.children); }); walk(CharmEditor.current.items('front')); return n; });
      check(`A4 a save the cloud refused after the editor closed: the editor opens again with the work (the new dot there) and says "${r4 ? r4.refuse.replace(/\s+/g, ' ').slice(0, 110) : ''}…"`, !!r4 && kept4 === 1 && /Not saved/.test(r4.refuse), JSON.stringify(r4));
      await page.click('.ce-root .ce-refuse [data-ask]'); await settleP(page);
      const note4 = await saveEditor(), rec4 = st.doc('Charm_Pool_Art', s0.pid + '~front');
      await page.evaluate(() => { CharmNestArtEdit.save = window.__realSave; });
      check(`A5 … Save again: kept ("${note4}"), the dot added to the record`, /^Front artwork saved/.test(note4 || '') && !!rec4 && rec4.added.length === grouped.n + 2, JSON.stringify({ note4, n: rec4 && rec4.added.length }));
    }

    if (!ONLY || ONLY === 'B' || ONLY === 'D' || ONLY === 'T') {
      /* ── B1 · the bar: Paul's photo on its front, saved; one real press on Approve ── */
      await R.openEngraveDetail(w, 'bar', { face: 'front' }); await R.settle(page, 700);
      const j0 = await jobOf();
      await page.$eval(Q + ' .egCharmEdit', b => b.click());
      await page.waitForSelector('.ce-root .ce-items path', { timeout: 60000 }); await settleP(page);
      const put = await page.evaluate(art => {
        // his record's parts, decoded into the editor's front frame exactly where he placed them, as the image importer's group of greys
        const E = CharmEditor.current, AE = CharmNestArtEdit, G = CharmNestGeom, c = document.querySelector('#egQueue .rvItem[data-kind=placement]'), j = Engrave.items().get(c.dataset.key), charm = Pool.charmOf(j.copies[0]);
        const view = j.view && j.view.face === 'front' ? j.view : G.backView(charm, G.viewOptionsFor({ charmUp: charm.upAngle, entry: Master.entryFor('TINY BAR_7427') || {}, face: 'front' }));
        const d = AE.decode(Object.assign({}, art, { baseKey: AE.baseKeyOf(charm) }), charm, view);
        const kids = d.added.map((it, i) => ({ kind: 'path', role: 'hatch', name: 'Grey ' + (i + 1), fill: it.fill, stroke: false, artGrey: true, even: true, evenOdd: true, subpaths: it.subpaths, tx: 0, ty: 0, sx: 1, sy: 1, rot: 0 }));
        E.ctx.addItems([{ kind: 'group', role: 'hatch', name: 'Image · photo', children: kids }], { select: false }); E.ctx.commit('image');
        return { n: kids.length, face: E.face() };
      }, ART);
      const note = await saveEditor();
      await until(() => page.evaluate(k => { const j = Engrave.items().get(k); return j && j.state === 'review' && j.fit && j.view && j.verify; }, j0.key), 60000);
      await R.settle(page, 900);
      const hit = await approveHit();
      await R.shot(w, 'CR-B1-bar-photo-saved');
      await page.click(Q + ' [data-a="approve"]', { timeout: 5000 }).catch(e => { hit.clickError = String(e.message).split('\n')[0]; });
      const ap = await until(() => page.evaluate(k => { const j = Engrave.items().get(k); return j && /approved|written/.test(j.state) && j.approvedAt ? { state: j.state, at: j.approvedAt } : null; }, j0.key), 90000);
      const wr = await until(() => { const d = st.doc('Charm_Pool_Back', j0.pid + '~front'); return d && ap && +d.approvedAt === ap.at && !d.invalidated ? d : null; }, 90000);
      // (the face's own file engraves the words; the art is engraved with the piece by its sheet's file, which the save marked owed (filesOwed) and the page writes from the piece's charm: its greys)
      const file = wr ? [...st.blobs.keys()].filter(k => k.includes('/front/') && k.includes(j0.pid) && /\.ai$/.test(k)).pop() : null;
      const rec = st.doc('Charm_Pool_Art', j0.pid + '~front');
      const greys = await page.evaluate(pid => [...new Set((Pool.charmOf(pid).members || []).filter(m => m.artGrey).map(m => m.fillRGB[0]))].sort((a, b) => a - b), j0.pid);
      const want = [...new Set(ART.added.map(a => a.fill[0]))].sort((a, b) => a - b);
      check(`B1 the bar's front: Paul's photo (${put.n} greys) saved in the Charm Editor ("${note}"); Approve is the element under the pointer at its own place (${hit.top ? 'yes' : 'no: ' + hit.on}${hit.wide ? ', a wide piece' : ''}), and ONE real press approves the words with the art on the piece (greys ${greys.join(' ')}) and writes the front's file (the art goes to the laser with the piece in its sheet's file)`,
        put.n === 8 && /^Front artwork saved/.test(note || '') && hit.top && !hit.clickError && !!ap && !!wr && !!file && !!rec && (rec.added || []).length === 8 && JSON.stringify(greys) === JSON.stringify(want),
        JSON.stringify({ put, note, hit, ap, wr: !!wr, file, rec: !!rec, greys, want }));

      /* ── B2 · the bird: a part added on its BACK in the editor, saved; one real press on Approve ── */
      await R.openEngraveDetail(w, 'sil', { face: 'back' }); await R.settle(page, 700);
      const s0 = await jobOf();
      await page.$eval(Q + ' .egCharmEdit', b => b.click());
      await page.waitForSelector('.ce-root .ce-items path', { timeout: 60000 }); await settleP(page);
      await page.evaluate(() => { const E = CharmEditor.current, f = E.frame(), x = f.x0 + f.wPt * 0.42, y = f.y0 + f.hPt * 0.62, s = 1.2; E.ctx.addItems([{ kind: 'path', role: 'hatch', name: 'Star', fill: true, stroke: false, subpaths: [[['m', [x, y - s]], ['l', [x + s, y + s]], ['l', [x - s, y + s]], ['h']]], tx: 0, ty: 0, sx: 1, sy: 1, rot: 0 }], { select: false }); E.ctx.commit('star'); });
      const note2 = await saveEditor();
      await until(() => page.evaluate(k => { const j = Engrave.items().get(k); return j && j.state === 'review' && j.fit && j.view && j.verify; }, s0.key), 60000);
      await R.settle(page, 900);
      const hit2 = await approveHit();
      await page.click(Q + ' [data-a="approve"]', { timeout: 5000 }).catch(e => { hit2.clickError = String(e.message).split('\n')[0]; });
      const ap2 = await until(() => page.evaluate(k => { const j = Engrave.items().get(k); return j && /approved|written/.test(j.state) && j.approvedAt ? { state: j.state, at: j.approvedAt } : null; }, s0.key), 90000);
      const wr2 = await until(() => { const d = st.doc('Charm_Pool_Back', s0.pid); return d && ap2 && +d.approvedAt === ap2.at && !d.invalidated ? d : null; }, 90000);
      check(`B2 the bird's back: a part added there in the Charm Editor ("${note2}"); Approve is under the pointer (${hit2.top ? 'yes' : 'no: ' + hit2.on}) and ONE real press approves it and writes the back's file`,
        /^Back artwork saved/.test(note2 || '') && hit2.top && !hit2.clickError && !!ap2 && !!wr2, JSON.stringify({ note2, hit2, ap2, wr2: !!wr2 }));
    }
    if (!ONLY || ONLY === 'C') {
      /* ── C · FACEVIEW: every face drawn as that face, all the Charm Editor's work on it, at once ── */
      const wa = () => page.evaluate(Q => {
        const c = document.querySelector(Q), cv = c && c.querySelector('.backHost canvas'), sw = c && c.querySelector('.backHost .egFaceView'), j = c && Engrave.items().get(c.dataset.key);
        const own = (cv && cv._frontArt || []).filter(s => s.artAdded || s.artChanged), th = f => c && c.querySelector(`.engFace[data-face="${f}"]`);
        return { job: j && (j.face === 'front' ? 'front' : 'back'), viewFace: cv ? (cv._viewFace || (cv._frontFace ? 'front' : 'back')) : null, front: !!(cv && cv._frontFace), art: cv && cv._frontArt ? cv._frontArt.length : -1, own: own.length, ownIds: own.map(s => s.artMemberId),
          grey: own.filter(s => s.artGrey).length, back: cv && cv._backArt ? cv._backArt.length : -1, peek: !!(c && c.querySelector('.egFrontPeek')), sw: sw ? [...sw.querySelectorAll('[data-fv]')].map(b => b.dataset.fv + (b.getAttribute('aria-pressed') === 'true' ? '*' : '')).join(' ') : '',
          words: !!(j && j.fit && (j.fit.glyphs || []).length), thumbs: ['front', 'back'].filter(th).map(f => f + (th(f).dataset.art ? '(art)' : '') + (th(f).getAttribute('aria-current') === 'true' ? '*' : '') + (th(f).querySelector('.engThumb canvas, .engThumb svg, .engThumb img') ? '+pic' : '')).join(' ') };
      }, Q);
      // C1 · the bird, engraved on its back, its front drawn on in the Charm Editor (A): the back shows the back only, the front is a thumbnail of its own
      await R.openEngraveDetail(w, 'sil', { face: 'back' }); await R.settle(page, 1200);
      const c1 = await wa();
      await R.shot(w, 'CR-C1-bird-back');
      check(`C1 the bird's back (${c1.sw}): no "Show front", no front art drawn mirrored on it (${c1.art}), its own added part drawn (${c1.back}); the card's thumbnails show the back and the front the Charm Editor drew on (${c1.thumbs})`,
        c1.job === 'back' && c1.viewFace === 'back' && !c1.peek && c1.art === 0 && c1.back >= 1 && c1.sw === 'front back*' && /front\(art\)\+pic/.test(c1.thumbs) && /back\*?\+pic/.test(c1.thumbs), JSON.stringify(c1));
      // C2 · Front in the switch: the front as it is cut and engraved, every part of the editor on it, view only
      await page.click(Q + ' .backHost .egFaceView [data-fv="front"]'); await R.settle(page, 600);
      const c2 = await wa();
      await R.shot(w, 'CR-C2-bird-front');
      check(`C2 Front in the switch: the work area draws the bird's FRONT (${c2.art} parts of art: the master's hatch and the ${c2.own} parts the Charm Editor put there, ${c2.grey} greys), its cut lines, the card marking the front (${c2.thumbs}); Approve stays the back's`,
        c2.viewFace === 'front' && c2.front && c2.own >= 6 && c2.grey >= 4 && c2.art > c2.own && c2.sw === 'front* back' && /front\(art\)\*/.test(c2.thumbs) && !!(await page.$(Q + ' [data-a="approve"], ' + Q + ' [data-a="approved"]')), JSON.stringify(c2));
      // C3 · back to the back, then a dot added on the FRONT in the Charm Editor: the moment it is saved the work area shows the front with the dot, no reload
      await page.click(Q + ' .backHost .egFaceView [data-fv="back"]'); await R.settle(page, 400);
      const c3a = await wa();
      await page.$eval(Q + ' .egCharmEdit', b => b.click()); await page.waitForSelector('.ce-root .ce-items path', { timeout: 60000 }); await settleP(page);
      await page.click('.ce-root [data-face="front"]'); await settleP(page);
      const dotId = await page.evaluate(() => { const E = CharmEditor.current, f = E.frame(), x = f.x0 + f.wPt * 0.52, y = f.y0 + f.hPt * 0.58; const ids = E.ctx.addItems([{ kind: 'path', role: 'hatch', name: 'Third dot', fill: true, stroke: false, subpaths: [[['m', [x, y]], ['l', [x + 0.7, y]], ['l', [x + 0.35, y + 0.7]], ['h']]], tx: 0, ty: 0, sx: 1, sy: 1, rot: 0 }], { select: false }); E.ctx.commit('dot'); return ids[0]; });
      const t0 = Date.now(), note3 = await saveEditor();
      const c3 = await until(async () => { const s = await wa(); return s.viewFace === 'front' && s.ownIds.includes(dotId) ? s : null; }, 5000), ms = Date.now() - t0, c3z = c3 ? null : Object.assign(await wa(), { dotId, fv: await page.evaluate(() => { const c = document.querySelector('#egQueue .rvItem[data-kind=placement]'); const j = c && Engrave.items().get(c.dataset.key); return j ? { v: Engrave.viewFaceOf(j), pj: Object.entries(Engrave.jobsOfPiece(j.copies[0])).map(([f, x]) => f + ':' + (x ? x.state + (x.editingBack ? 'E' : '') + (x === j ? '=' : '') : '-')), st: j.state } : null; }) });
      await R.shot(w, 'CR-C3-saved-front-shown');
      check(`C3 from the back's card, a dot added on the front and saved ("${note3}"): the work area shows the FRONT with the dot ${ms} ms after Save (no reload), the switch on Front`, c3a.viewFace === 'back' && !!c3 && c3.sw === 'front* back', JSON.stringify({ c3a: c3a.viewFace, c3, ms, c3z }));
      // C4 · the bar, engraved on its front: its front draws Paul's photo (every grey) with the words, and its thumbnail the front
      await R.openEngraveDetail(w, 'bar', { face: 'front' }); await R.settle(page, 1200);
      const c4 = await wa();
      check(`C4 the bar's front: the work area draws Paul's photo (${c4.grey} grey parts) with the words fitted on it, the cut line, and the card's thumbnail is the front (${c4.thumbs})`,
        c4.job === 'front' && c4.viewFace === 'front' && c4.grey === 8 && c4.words && /front\*?\+pic/.test(c4.thumbs) && !c4.peek, JSON.stringify(c4));
    }
    if (!ONLY || ONLY === 'D') {
      /* ── D · the bar of order 4203083230 after B1 (his photo saved on the front, the words approved) ──
         MADEPV ("In this view the vectorized image does not show up in the right side preview"): the engraving card beside the sheet (order window's Sheet tab, the sheet window) draws the face as made
         EDITDESIGN ("When I press the 'Edit Design' button ... the engraving text is not present"): Edit Design opens the piece as the engraving card's Open in Charm Editor does, words as text */
      const bar = await page.evaluate(() => { const row = B.orders.rows.find(r => r.spec && r.spec.designSku === 'TINY BAR_7427'); const j = row && Engrave.jobsOf(row).find(x => x.face === 'front'); const pid = j && j.copies[0]; const sh = pid && Pool.sheetOf(pid); return j ? { key: row.key, rid: String(row.order.receiptId), job: j.key, pid, state: j.state, sheet: sh ? sh.sheetId : null } : null; });
      const greys = [...new Set(ART.added.map(a => Math.round(Math.max(0, Math.min(1, a.fill[0])) * 255)))].filter(v => v > 0 && v < 255);
      /** The picture in an engraving card's preview, read by its pixels: the photo's own greys (exact, as its parts are filled), and the words' dark ink beside them (outside the greys' box). */
      const pixels = sel => page.evaluate(({ sel, greys }) => {
        const cv = document.querySelector(sel + ' [data-engraving-preview] canvas'); if (!cv) { const s = document.querySelector(sel + ' [data-engraving-preview]'); return { canvas: false, slot: s ? s.innerHTML.slice(0, 120) : null }; }
        // (a grey of the photo: a pixel of one of its exact levels whose four neighbours are that grey too, the inside of a filled part, never the anti-aliased edge of a letter or the cut line;
        //  the words: their ink's exact #111 (renderBack's glyphs), which neither the cut line (#000) nor any grey level is)
        const W = cv.width, H = cv.height, d = cv.getContext('2d').getImageData(0, 0, W, H).data, lv = new Set(greys), at = (x, y) => (y * W + x) * 4, g = (x, y) => { const i = at(x, y); return d[i] === d[i + 1] && d[i] === d[i + 2] ? d[i] : -1; };
        let grey = 0, ink = 0, gx0 = 1e9, gx1 = -1, ix0 = 1e9, ix1 = -1;
        for (let y = 1; y < H - 1; y++) for (let x = 1; x < W - 1; x++) { const v = g(x, y); if (v === 17) { ink++; if (x < ix0) ix0 = x; if (x > ix1) ix1 = x; } else if (lv.has(v) && g(x - 1, y) === v && g(x + 1, y) === v && g(x, y - 1) === v && g(x, y + 1) === v) { grey++; if (x < gx0) gx0 = x; if (x > gx1) gx1 = x; } }
        return { canvas: true, w: W, h: H, grey, ink, greyX: grey ? [gx0, gx1] : null, inkX: ink ? [ix0, ix1] : null, beside: grey > 0 && ink > 0 && ix1 > gx1 + 20, made: !!cv._made, art: (cv._frontArt || []).filter(s => s.artGrey).length };
      }, { sel, greys });
      const shelved = () => page.evaluate(k => { const j = Engrave.items().get(k); return j ? { state: j.state, view: !!j.view, fit: !!j.fit, written: !!j.writtenFit } : null; }, bar.job);
      const sheetTab = async () => {
        await page.evaluate(async k => { CN.setMode('orders'); if (OrderWin.isOpen()) OrderWin.close(); await OrderWin.open(k, { view: 'sheet' }); }, bar.key);
        await until(() => page.evaluate(() => !!document.querySelector('#owSheetPanel [data-engraving-panel] [data-engraving-preview] > *')), 30000); await R.settle(page, 600);
        return pixels('#owSheetPanel [data-engraving-panel]');
      };
      // D1 · the order window's Sheet tab: the FRONT ENGRAVING card's preview
      const d1 = await sheetTab(), s1 = await shelved();
      await R.shot(w, 'CR-D1-sheet-tab-card');
      check(`D1 the order window's Sheet tab, the bar's FRONT ENGRAVING card (${s1 && s1.state}): its preview draws Paul's photo in its own greys (${d1.grey} px of the ${greys.length} grey levels, ${d1.art} grey parts) beside the words (${d1.ink} px of ink outside the photo)`,
        !!bar && /approved|written/.test(s1 && s1.state) && d1.canvas && d1.made && d1.art === 8 && d1.grey > 400 && d1.ink > 150 && d1.beside, JSON.stringify({ bar, s1, d1 }));
      // D2 · the same once the upkeep let go of the written placement (shelveWritten: no view, no fit, only the saved picture before): the card lays it out again for the picture
      await page.evaluate(() => { try { if (OrderWin.isOpen()) OrderWin.close(); } catch (_) {} }); await R.openEngraveDetail(w, 'sil', { face: 'back' }); await R.settle(page, 400);   // (the upkeep keeps the placement of the card in front: another piece's card is put in front, as a person moving on does)
      await page.evaluate(() => { try { CN.setMode('orders'); } catch (_) {} Engrave.shelveWritten(); });
      const s2 = await shelved();
      const d2 = s2 && !s2.view ? await sheetTab() : null;
      await R.shot(w, 'CR-D2-sheet-tab-card-shelved');
      check(`D2 … after the upkeep let go of the written placement (${JSON.stringify(s2)}): the card still draws the photo's greys (${d2 ? d2.grey : '-'} px) beside the words (${d2 ? d2.ink : '-'} px), not the words-only saved picture`,
        !!s2 && !s2.view && s2.written && !!d2 && d2.canvas && d2.grey > 400 && d2.ink > 150 && d2.beside, JSON.stringify({ s2, d2 }));
      // D3 · the sheet window's own card for the piece
      // (the sheet window's card is this very card: charm-nest-sheetwin.js renderEng draws CNEngravingSeals.panel and wirePanel with the piece's job, as the Sheet tab does; this offline world's
      //  sheets are working pages with no saved record to open that window on, so D1 and D2 are its check)
      await page.evaluate(() => { try { if (OrderWin.isOpen()) OrderWin.close(); } catch (_) {} });
      // D4 · Edit Design in the order window's header: the piece's own design (pc-<poolId>, kept by B1's save) opens THE PIECE, its front words as an editable text of the engraving, the photo's greys with it
      await page.evaluate(async k => { CN.setMode('orders'); if (OrderWin.isOpen()) OrderWin.close(); await OrderWin.open(k); }, bar.key);
      const btn = await until(() => page.evaluate(() => { const b = document.getElementById('owCpDesign'); return b && !b.hidden ? b.textContent.trim() : null; }), 20000);
      const s4 = await shelved(), puts4 = artPuts();
      if (btn) await page.click('#owCpDesign');
      await page.waitForSelector('.ce-root .ce-items path', { timeout: 60000 }).catch(() => {}); await settleP(page);
      const d4 = await page.evaluate(() => { const E = CharmEditor.current; if (!E) return null; const all = []; const walk = (l, d) => l.forEach(i => { all.push(Object.assign({ depth: d }, i)); if (i.children) walk(i.children, d + 1); }); walk(E.items('front'), 0); const t = all.find(i => i.kind === 'text');
        return { face: E.face(), text: t ? { text: t.text, source: t.source, fontKey: t.fontKey, depth: t.depth, eng: t.engraving ? t.engraving.face : null } : null, greys: all.filter(i => i.artGrey || (Array.isArray(i.fill) && i.fill[0] === i.fill[1] && i.fill[1] === i.fill[2] && i.fill[0] > 0 && i.fill[0] < 1 && i.kind === 'path')).length, design: !!document.querySelector('.ce-root [data-cp-ce-send]'), backN: E.items('back').length, title: (document.querySelector('.ce-root .ce-id h2') || {}).textContent || '' }; });
      await R.shot(w, 'CR-D4-edit-design');
      check(`D4 the order window's "${btn}" (the bar's job ${s4 && s4.view ? 'with' : 'without'} its placement in memory): the Charm Editor opens on the piece with its front words "${d4 && d4.text ? d4.text.text : ''}" as ONE editable text of the engraving (${d4 && d4.text ? d4.text.source + ', ' + d4.text.fontKey : '-'}) and the photo's ${d4 ? d4.greys : 0} greys, Send to sheet… beside Save`,
        btn === 'Edit Design' && !!d4 && !!d4.text && d4.text.text === '33.47765N 86.80020W' && d4.text.source === 'engrave' && d4.text.eng === 'front' && d4.text.depth === 0 && d4.greys >= 8 && d4.face === 'front' && d4.design, JSON.stringify({ btn, s4, d4 }));
      // D5 · a dot added there and saved: the piece keeps it, the words stay the engraving's (never copied into the art, never lost)
      const dot = d4 && await page.evaluate(() => { const E = CharmEditor.current, f = E.frame(), x = f.x0 + f.wPt * 0.9, y = f.y0 + f.hPt * 0.5; const ids = E.ctx.addItems([{ kind: 'path', role: 'hatch', name: 'Edit Design dot', fill: true, stroke: false, subpaths: [[['m', [x, y]], ['l', [x + 0.5, y]], ['l', [x + 0.25, y + 0.5]], ['h']]], tx: 0, ty: 0, sx: 1, sy: 1, rot: 0 }], { select: false }); E.ctx.commit('dot'); return ids[0]; });
      if (d4) await page.click('.ce-root [data-act="save"]');
      await until(async () => artPuts() > puts4 && !(await page.evaluate(() => !!document.querySelector('.ce-root'))), 90000);
      const rec5 = st.doc('Charm_Pool_Art', bar.pid + '~front');
      const j5 = await until(() => page.evaluate(k => { const j = Engrave.items().get(k); return j && j.fit && j.view ? { state: j.state, text: j.text, lines: (j.fit.lines || j.lines || []).join(' '), glyphs: (j.fit.glyphs || []).length, artOnly: !!j.fit.artOnly } : null; }, bar.job), 60000);
      check(`D5 … a dot added there and saved: the piece's record keeps the photo and the dot (${rec5 ? (rec5.added || []).length : 0} parts, no text among them), the front's words stay the engraving's ("${j5 ? j5.lines : ''}", ${j5 ? j5.glyphs : 0} glyphs, ${j5 ? j5.state : '-'})`,
        !!dot && !!rec5 && (rec5.added || []).length === 9 && !(rec5.added || []).some(a => a.kind === 'text' || a.artText || /33\.47765N/.test(String(a.text || a.name || ''))) && !!j5 && j5.lines === '33.47765N 86.80020W' && j5.glyphs > 0 && !j5.artOnly,
        JSON.stringify({ dot, rec: rec5 && (rec5.added || []).map(a => a.kind + ':' + (a.name || a.text || a.id)).slice(-3), j5 }));
      await page.evaluate(() => { try { if (OrderWin.isOpen()) OrderWin.close(); } catch (_) {} });
    }
    if (!ONLY || ONLY === 'T') {
      /* ── T · TYPEWORDS (Paul, 28 Oct 2026, order 4203083230: "When I select existing text on the charm that text is not recognized as text, it's just an object on the charm. The user also needs the
         ability to update existing text, even if that text came from back engraving modal"). The live sandbox record of that bar (read-only, 28 Oct 2026): Charm_Pool_Art 4203083230_5251589009_1~front
         held, beside the 8 greys, an added part "engrave-words-front-mvsjt11z" of kind "text", "33.47765N 86.80020W愛", its outlines in `d` (the old CETEXT save of retyped words), and the front's
         engraving record was invalidated 2 s later: the words had been taken off the card. Opened again: a Compound Path. ── */
      const bar = await page.evaluate(() => { const row = B.orders.rows.find(r => r.spec && r.spec.designSku === 'TINY BAR_7427'); const j = row && Engrave.jobsOf(row).find(x => x.face === 'front'); return j ? { key: row.key, job: j.key, pid: j.copies[0] } : null; });
      await page.evaluate(() => {
        const norm = s => String(s == null ? '' : s).replace(/\s+/g, ' ').trim();
        window.__tw = {
          norm,
          words: face => { const E = CharmEditor.current; if (!E) return null; let hit = null; const walk = l => l.forEach(i => { if (i.kind === 'text' && i.source === 'engrave') hit = hit || i; if (i.children) walk(i.children); }); walk(E.items(face)); return hit; },
          types: face => { const E = CharmEditor.current, out = []; const walk = l => l.forEach(i => { if (i.kind === 'text') out.push({ id: i.id, source: i.source, text: norm(i.text), fontKey: i.fontKey }); if (i.children) walk(i.children); }); walk(E.items(face)); return out; },
          outlined: face => { const E = CharmEditor.current, out = []; const walk = l => l.forEach(i => { if (i.kind === 'path' && i.text != null && norm(i.text)) out.push({ id: i.id, text: norm(i.text), name: i.name }); if (i.children) walk(i.children); }); walk(E.items(face)); return out; },
          job: k => { const j = Engrave.items().get(k); return j ? { state: j.state, lines: norm((j.fit && j.fit.lines || j.lines || []).join(' ')), artOnly: !!(j.fit && j.fit.artOnly), glyphs: j.fit && j.fit.glyphs ? j.fit.glyphs.length : 0, centre: j.fit && j.fit.centre ? j.fit.centre.slice() : null, size: j.fit ? j.fit.size : null, nudged: !!j.nudged, view: !!j.view } : null; }
        };
      });
      const recTexts = () => { const r = st.doc('Charm_Pool_Art', bar.pid + '~front'); return r ? (r.added || []).filter(a => a.kind === 'text' || a.text != null).map(a => a.id + ':' + a.text) : []; };
      const recGreys = () => { const r = st.doc('Charm_Pool_Art', bar.pid + '~front'); return r ? (r.added || []).filter(a => a.artGrey).length : 0; };
      const editorOpen = () => page.evaluate(() => !!document.querySelector('.ce-root'));
      const openCard = async () => { await R.openEngraveDetail(w, 'bar', { face: 'front' }); await R.settle(page, 700); await page.$eval(Q + ' .egCharmEdit', b => b.click()); await page.waitForSelector('.ce-root .ce-items path', { timeout: 60000 }); await settleP(page); await settleP(page); };
      const cancel = async () => { await page.click('.ce-root [data-act="cancel"]'); await settleP(page); if (await page.$('.ce-root .ce-ask [data-ask="yes"]')) await page.click('.ce-root .ce-ask [data-ask="yes"]'); await until(async () => !(await editorOpen()), 10000); };
      const saveNow = async () => { const n0 = artPuts(); await page.click('.ce-root [data-act="save"]'); await until(async () => !(await editorOpen()), 30000); return n0; };
      const clickOn = async id => {   // (a click on the letters, as a person selects them: the middle of the box can fall between two words)
        const b = await page.evaluate(id => CharmEditor.current.box(id), id); if (!b) return false;
        for (const f of [0.5, 0.3, 0.7, 0.15, 0.85, 0.42, 0.58]) {
          const c = await page.evaluate(([x, y]) => CharmEditor.current.clientOf(x, y), [b[0] + f * (b[2] - b[0]), (b[1] + b[3]) / 2]); await page.mouse.click(c.x, c.y); await settleP(page);
          if ((await page.evaluate(() => CharmEditor.current.selection())).includes(id)) return true;
        }
        return false;
      };
      /** The outlines of the words as the editor draws them for `text` where the card's words stand (the editor's own result: placed in the face), for a record made as the old hand-back made it. */
      const outlinesOf = async text => {
        await page.evaluate(t => CharmNestCeText.prepare({ job: null, faces: { front: { items: [{ kind: 'text', text: t, fontKey: 'mynerve' }] } } }), text);
        await openCard();
        // (this world fits the words beside Paul's photo, nearer the bar's end than his were: a longer line is set a little smaller where it must, so it stays on the metal as his did)
        const o = await page.evaluate(t => {
          const E = CharmEditor.current, it = __tw.words('front'), f = E.frame(); let r = null, size = it.sizePt;
          for (const k of [1, 0.92, 0.85, 0.78, 0.7]) {
            size = Math.round(it.sizePt * k * 1e6) / 1e6; E.ctx.replaceItems({ [it.id]: Object.assign({}, it, { text: t, sizePt: size }) }); E.ctx.commit('words');
            r = E.result().faces.front.items.find(i => i.id === it.id); const b = r.bbox;
            if (b && b[0] > f.x0 + 0.08 * f.wPt && b[2] < f.x0 + 0.92 * f.wPt) break;
          }
          return { subpaths: r.subpaths, fontKey: it.fontKey, sizePt: size };
        }, text);
        await cancel();
        return o;
      };
      /** The record as the old CETEXT save left it: the outlines of `text` added as a part "engrave-words-front-<moment>" of kind text, beside what the record holds. */
      const handBack = (o, text) => page.evaluate(async ([k, o, text]) => {
        const j = Engrave.items().get(k), pid = j.copies[0], AE = CharmNestArtEdit, charm = Pool.charmOf(pid), view = j.view || CharmNestGeom.backView(charm, CharmNestGeom.viewOptionsFor({ charmUp: charm.upAngle, entry: Master.entryFor('TINY BAR_7427') || {}, face: 'front' }));
        const had = AE.decode(AE.cached(pid, 'front'), charm, view).added;
        const part = { id: 'engrave-words-front-' + (Date.now() - 3600000).toString(36), kind: 'text', role: 'hatch', fill: [0, 0, 1], stroke: null, text, subpaths: o.subpaths };
        try { const info = await AE.save(pid, 'front', { removed: [], changed: {}, added: had.concat([part]) }, { charm, view, by: 'Paul_K', placed: true }); return { id: part.id, n: (info.record.added || []).length }; }
        catch (e) { return { id: part.id, error: String((e && e.message) || e), refused: e && e.refused }; }
      }, [bar.job, o, text]);

      // T1 · from the engraving card: the words are ONE Type of the engraving; a click shows "Type" on the Control bar and the Character section in Properties, as in Illustrator
      await openCard();
      const t1w = await page.evaluate(() => __tw.words('front')), t1j = await page.evaluate(k => __tw.job(k), bar.job);
      const t1sel = t1w ? await clickOn(t1w.id) : false;
      const t1 = await page.evaluate(() => { const k = document.querySelector('.ce-root [data-selkind]'), ch = document.querySelector('.ce-root [data-aip-sec="character"]'), pk = document.querySelector('.ce-root [data-aip-kind]'); return { bar: k ? k.textContent : null, character: ch ? !ch.hidden && ch.getBoundingClientRect().height > 0 : null, props: pk ? pk.textContent : null }; });
      await R.shot(w, 'CR-T1-words-are-type');
      check(`T1 the engraving card's Open in Charm Editor: the front words "${t1w ? t1w.text : ''}" are ONE Type of the engraving (${t1w ? t1w.source + ', ' + t1w.fontKey : '-'}); selected, the Control bar reads "${t1.bar}" and Properties "${t1.props}" with its Character section`,
        !!t1w && __norm(t1w.text) === t1j.lines && t1w.source === 'engrave' && t1w.engraving && t1w.engraving.face === 'front' && t1sel && t1.bar === 'Type' && (t1.props == null || t1.props === 'Type') && t1.character === true, JSON.stringify({ t1w: t1w && { text: t1w.text, source: t1w.source, eng: t1w.engraving }, t1j, t1sel, t1 }));
      await cancel();

      // T2 · approved and shelved (the upkeep let go of the written placement): the order window's Open in Charm Editor and its Edit Design hand the words in as Type too
      const shelve = async () => { await page.evaluate(() => { try { if (OrderWin.isOpen()) OrderWin.close(); } catch (_) {} }); await R.openEngraveDetail(w, 'sil', { face: 'back' }); await R.settle(page, 300); await page.evaluate(() => { try { CN.setMode('orders'); } catch (_) {} Engrave.shelveWritten(); }); return page.evaluate(k => __tw.job(k), bar.job); };
      const viaOrder = async sel => {
        await page.evaluate(async k => { CN.setMode('orders'); if (OrderWin.isOpen()) OrderWin.close(); await OrderWin.open(k); }, bar.key);
        const ok = sel === 'direct' ? await until(() => page.evaluate(() => OrderWin.isOpen()), 20000) : await until(() => page.evaluate(s => { const b = document.querySelector(s); return !!b && !b.hidden && !b.disabled; }, sel), 20000);
        if (!ok) return { button: false };
        if (sel === 'direct') await page.evaluate(k => { const row = B.orders.byKey.get(k), j = Engrave.jobsOf(row).find(x => x.face === 'front'), pid = (j && j.copies && j.copies[0]) || (row.poolIds || [])[0]; window.__pv = Engrave.openCharmEditor({ row, poolId: pid, from: 'orderWindow' }); }, bar.key);
        else await page.click(sel); await page.waitForSelector('.ce-root .ce-items path', { timeout: 60000 }).catch(() => {}); await settleP(page);
        const r = await page.evaluate(() => { const w = CharmEditor.current && __tw.words('front'); return { open: !!CharmEditor.current, w: w && { text: __tw.norm(w.text), source: w.source, face: w.engraving && w.engraving.face }, outlined: CharmEditor.current ? __tw.outlined('front').length : -1 }; });
        if (r.open) await cancel();
        await page.evaluate(() => { try { if (OrderWin.isOpen()) OrderWin.close(); } catch (_) {} });
        return Object.assign({ button: true }, r);
      };
      // (the front approved and its file written first, as B1 leaves it: C and D may have saved art on it since, which takes an approval back)
      if (!(await page.evaluate(k => Engrave.items().get(k).state === 'written', bar.job))) {
        await R.openEngraveDetail(w, 'bar', { face: 'front' }); await R.settle(page, 900);
        await until(() => page.evaluate(k => { const j = Engrave.items().get(k); return j && j.state === 'review' && j.fit && j.verify; }, bar.job), 60000);
        await page.click(Q + ' [data-a="approve"]', { timeout: 5000 }).catch(() => {});
        await until(() => page.evaluate(k => { const j = Engrave.items().get(k); return j && j.state === 'written' && (j.backs || []).some(b => b.approvedAt === j.approvedAt && b.outputs && b.outputs.png); }, bar.job), 90000);
      }
      const s2a = await shelve(), t2a = await viaOrder('direct');   // (OWNOCE: the order window has no Open in Charm Editor of its own any more; the piece opens as it did, from the order window)
      const s2b = await shelve(), t2b = await viaOrder('#owCpDesign');
      check(`T2 approved and shelved (${s2a && s2a.state}, placement let go: ${!(s2a && s2a.view)}): the piece opened from the order window and its Edit Design each open the front words as ONE Type of the engraving ("${t2a.w ? t2a.w.text : ''}" · "${t2b.w ? t2b.w.text : ''}"), no outlines of them`,
        !!s2a && /approved|written/.test(s2a.state) && !s2a.view && !!s2b && !s2b.view && [t2a, t2b].every(r => r.button && r.w && r.w.text === t1j.lines && r.w.source === 'engrave' && r.w.face === 'front' && r.outlined === 0), JSON.stringify({ s2a, s2b, t2a, t2b }));

      // T3 · Paul's piece as the live record had it: the words retyped "…W愛" saved by the old hand-back (an outline part "engrave-words-front-…" in the art) and taken off the card
      const T3TEXT = '33.47765N 86.80020W愛';
      const o3 = await outlinesOf(T3TEXT), hb3 = await handBack(o3, T3TEXT);
      const off3 = await page.evaluate(async k => { const j = Engrave.items().get(k); const r = await Engrave.saveSetup(j.row, { front: { text: '' }, reason: { code: 'other', text: 'Words changed in the Charm Editor', of: 'front' } }); return { ok: r && r.ok }; }, bar.job);
      const pre3 = { job: await page.evaluate(k => __tw.job(k), bar.job), rec: recTexts() };
      await openCard();
      const t3 = await page.evaluate(() => ({ w: __tw.words('front'), types: __tw.types('front'), outlined: __tw.outlined('front'), dirty: CharmEditor.current.dirty() }));
      await R.shot(w, 'CR-T3-outlined-words-are-type-again');
      const t3sel = t3.w ? await clickOn(t3.w.id) : false, t3bar = await page.evaluate(() => (document.querySelector('.ce-root [data-selkind]') || {}).textContent);
      check(`T3 the bar as the live record had it (words taken off the card: ${pre3.job && pre3.job.artOnly}; the art holding "${(pre3.rec[0] || '').split(':').slice(1).join(':')}" as outlines): opened from its card the words are ONE Type of the engraving again ("${t3.w ? __norm(t3.w.text) : ''}", ${t3.w ? t3.w.fontKey + ' ' + (+t3.w.sizePt).toFixed(2) + ' pt' : '-'}), no Compound Path of them left; selected it reads "${t3bar}"`,
        off3.ok && pre3.job.artOnly && pre3.rec.length === 1 && pre3.rec[0].startsWith(hb3.id) && !!t3.w && __norm(t3.w.text) === __norm(T3TEXT) && t3.w.engraving && t3.w.engraving.job === bar.job && t3.types.length === 1 && t3.outlined.length === 0 && t3sel && t3bar === 'Type' && Math.abs(+t3.w.sizePt - o3.sizePt) < 0.05 * o3.sizePt,
        JSON.stringify({ off3, pre3, hb3, t3: { w: t3.w && { text: t3.w.text, fontKey: t3.w.fontKey, sizePt: t3.w.sizePt, eng: t3.w.engraving }, types: t3.types, outlined: t3.outlined, dirty: t3.dirty }, o3: { fontKey: o3.fontKey, sizePt: o3.sizePt } }));
      const n3 = await saveNow();
      const j3 = await until(() => page.evaluate(([k, t]) => { const j = __tw.job(k); return j && j.glyphs > 0 && j.lines === t && !j.artOnly ? j : null; }, [bar.job, __norm(T3TEXT)]), 60000);
      check(`T3b … Save: the words go back to the engraving ("${j3 ? j3.lines : ''}", ${j3 ? j3.glyphs : 0} glyphs fitted, ${j3 ? j3.state : '-'}) and their outlines leave the art (the record's words: ${JSON.stringify(recTexts())}; its ${recGreys()} greys kept)`,
        !!j3 && j3.state === 'review' && artPuts() === n3 + 1 && recTexts().length === 0 && recGreys() === 8, JSON.stringify({ j3, rec: recTexts(), greys: recGreys(), puts: artPuts() - n3, note: await page.evaluate(() => (document.querySelector('#egQueue .rvItem[data-kind=placement] .egArtNote') || {}).textContent || '') }));

      // T4 · a copy of the words on top of them (the job keeps its words, the art an outline copy of the very same words): one Type, the copy taken out by Save, the words untouched
      const o4 = await outlinesOf(T3TEXT), hb4 = await handBack(o4, T3TEXT);
      await openCard();
      const t4 = await page.evaluate(() => ({ types: __tw.types('front'), outlined: __tw.outlined('front') }));
      const n4 = await saveNow();
      const j4 = await until(() => page.evaluate(k => { const j = __tw.job(k); return j && j.glyphs > 0 ? j : null; }, bar.job), 30000);
      check(`T4 an outline copy of the words on top of them (${hb4.id}): the editor shows ONE Type of them (${t4.types.map(t => t.source + ':' + t.text).join(', ')}) and no outlines; Save takes the copy out of the art (${JSON.stringify(recTexts())}) and the words stay "${j4 ? j4.lines : ''}"`,
        t4.types.length === 1 && t4.types[0].source === 'engrave' && t4.outlined.length === 0 && artPuts() === n4 + 1 && recTexts().length === 0 && recGreys() === 8 && !!j4 && j4.lines === __norm(T3TEXT), JSON.stringify({ t4, rec: recTexts(), j4 }));

      // T5 · the words retyped in the editor (double-click, the words box) on an APPROVED face: Save changes the engraving's words, fits them again and sends the face back to its Approve; nothing in the art
      await R.openEngraveDetail(w, 'bar', { face: 'front' }); await R.settle(page, 900);
      await page.click(Q + ' [data-a="approve"]', { timeout: 5000 }).catch(() => {});
      const ap5 = await until(() => page.evaluate(k => { const j = Engrave.items().get(k); return j && /approved|written/.test(j.state) ? j.state : null; }, bar.job), 90000);
      await openCard();
      const w5 = await page.evaluate(() => __tw.words('front'));
      let ta5 = null;
      for (const f of [0.5, 0.3, 0.7, 0.15, 0.85]) {
        const b = await page.evaluate(id => CharmEditor.current.box(id), w5.id), c = await page.evaluate(([x, y]) => CharmEditor.current.clientOf(x, y), [b[0] + f * (b[2] - b[0]), (b[1] + b[3]) / 2]);
        await page.mouse.dblclick(c.x, c.y); await settleP(page);
        ta5 = await page.evaluate(() => { const t = document.querySelector('.ce-root textarea.ce-editor'); return t ? t.value : null; }); if (ta5 != null) break;
      }
      await page.keyboard.press('Control+a'); await page.keyboard.type('N 33.47765 W 86.80020'); await page.keyboard.press('Enter'); await settleP(page);
      const n5 = await saveNow();
      const j5 = await until(() => page.evaluate(k => { const j = __tw.job(k); return j && j.glyphs > 0 && j.lines === 'N 33.47765 W 86.80020' ? j : null; }, bar.job), 60000);
      await R.settle(page, 600);
      const note5 = await page.evaluate(() => (document.querySelector('#egQueue .rvItem[data-kind=placement] .egArtNote') || {}).textContent || '');
      await R.shot(w, 'CR-T5-words-retyped-back-on-card');
      check(`T5 the approved front (${ap5}) retyped in the editor ("${ta5}" → "N 33.47765 W 86.80020"): Save gives the words to the engraving, fitted again (${j5 ? j5.glyphs : 0} glyphs, ${j5 ? j5.state : '-'}: approve it again), nothing saved in the art (${artPuts() - n5} art saves); the card says "${note5}"`,
        !!ap5 && __norm(ta5) === __norm(T3TEXT) && !!j5 && j5.state === 'review' && artPuts() === n5 && recTexts().length === 0 && /engraving now reads "N 33\.47765 W 86\.80020"/.test(note5), JSON.stringify({ ap5, ta5, j5, note5, rec: recTexts() }));

      // T6 · the words moved in the editor: the fit goes where they were put (its centre moved as far), still the engraving's words
      const c6 = await page.evaluate(k => __tw.job(k), bar.job);
      await openCard();
      const mv6 = await page.evaluate(() => { const E = CharmEditor.current, it = __tw.words('front'); E.ctx.replaceItems({ [it.id]: Object.assign({}, it, { tx: it.tx - 1.5, ty: it.ty + 0.3 }) }); E.ctx.commit('move'); return { id: it.id }; });
      const n6 = await saveNow();
      const j6 = await until(() => page.evaluate(([k, x0]) => { const j = __tw.job(k); return j && j.centre && j.nudged && Math.abs(j.centre[0] - x0) > 0.5 ? j : null; }, [bar.job, c6.centre[0]]), 30000);
      const d6 = j6 ? [j6.centre[0] - c6.centre[0], j6.centre[1] - c6.centre[1]] : null;
      check(`T6 the words moved 1.5 pt left and 0.3 pt down in the editor: Save moves the fit as far (${d6 ? d6.map(v => v.toFixed(3)).join(', ') : '-'} pt, y up), the same words ("${j6 ? j6.lines : ''}"), nothing in the art`,
        !!mv6 && !!j6 && Math.abs(d6[0] + 1.5) < 0.05 && Math.abs(d6[1] + 0.3) < 0.05 && j6.lines === 'N 33.47765 W 86.80020' && artPuts() === n6 && recTexts().length === 0, JSON.stringify({ c6, j6, d6 }));

      // T7 · Paul's own record: the very parts the live record holds beside his greys (fixtures/tinybar-front-words-4203083230.json: his retyped words as the old hand-back saved them, and a line-art
      //  icon), the words taken off the card as that save took them: the words open as Type in Mynerve at the size he had them (2.889 pt), read from their own outlines; the icon stays a path;
      //  Save gives the words back to the engraving and leaves the icon and the greys in the art
      const w7 = await page.evaluate(async ([k, rec]) => {
        const j = Engrave.items().get(k), pid = j.copies[0], AE = CharmNestArtEdit, charm = Pool.charmOf(pid), view = j.view;
        const had = AE.decode(AE.cached(pid, 'front'), charm, view).added, live = AE.decode(Object.assign({ removed: [], changed: {} }, rec), charm, view).added;
        try { await AE.save(pid, 'front', { removed: [], changed: {}, added: had.concat(live) }, { charm, view, by: 'Paul_K', placed: true }); } catch (e) { return { error: String((e && e.message) || e) }; }
        const r = await Engrave.saveSetup(j.row, { front: { text: '' }, reason: { code: 'other', text: 'Words changed in the Charm Editor', of: 'front' } });
        return { ok: r && r.ok, live: live.map(i => i.id) };
      }, [bar.job, WORDS]);
      const pre7 = { job: await page.evaluate(k => __tw.job(k), bar.job), rec: recTexts() };
      await openCard();
      const t7 = await page.evaluate(() => ({ w: __tw.words('front'), types: __tw.types('front'), outlined: __tw.outlined('front'), icon: !!CharmEditor.current.items('front').find(i => i.id === 'cemvsjxx527' || (i.children || []).some(c => c.id === 'cemvsjxx527')) }));
      await R.shot(w, 'CR-T7-pauls-record-words-are-type');
      const n7 = await saveNow();
      const j7 = await until(() => page.evaluate(k => { const j = __tw.job(k); return j && j.glyphs > 0 && j.lines === '33.47765N 86.80020W愛' && !j.artOnly ? j : null; }, bar.job), 60000);
      const rec7 = st.doc('Charm_Pool_Art', bar.pid + '~front');
      check(`T7 Paul's record (${(w7.live || []).join(', ')}; the card's words taken off: ${pre7.job && pre7.job.artOnly}): his words open as ONE Type of the engraving ("${t7.w ? t7.w.text : ''}" in ${t7.w ? t7.w.fontKey : '-'} at ${t7.w ? (+t7.w.sizePt).toFixed(3) : '-'} pt, read from their outlines; he had 2.889 pt), no outlines of them, the icon still a path; Save: the engraving reads "${j7 ? j7.lines : ''}" (${j7 ? j7.state : '-'}), the art keeps the icon and the greys and no words (${JSON.stringify(recTexts())})`,
        w7.ok && pre7.job.artOnly && pre7.rec.length === 1 && !!t7.w && __norm(t7.w.text) === '33.47765N 86.80020W愛' && t7.w.fontKey === 'mynerve' && Math.abs(+t7.w.sizePt - 2.889252) < 0.03 && t7.types.length === 1 && t7.outlined.length === 0 && t7.icon
        && artPuts() === n7 + 1 && !!j7 && recTexts().length === 0 && !!rec7 && rec7.added.some(a => a.id === 'cemvsjxx527') && recGreys() === 8,
        JSON.stringify({ w7, pre7, t7: { w: t7.w && { text: t7.w.text, fontKey: t7.w.fontKey, sizePt: t7.w.sizePt, tx: t7.w.tx, ty: t7.w.ty }, types: t7.types, outlined: t7.outlined, icon: t7.icon }, j7, rec: recTexts() }));
    }
    check('E1 no page error', w.errors.length === 0, JSON.stringify(w.errors).slice(0, 600));
    console.log('screenshots in ' + shots);
  } finally { await w.close(); }
}

/* ═══ T0 · TYPEWORDS, the repair rule and the glyph comparison (charm-nest-ce-words.js), with a font engine of made-up letters (each letter a box of its own width, as a font's advance) ═══ */
function partTypeModel() {
  const W = require(path.join(ROOT, 'charm-nest-ce-words.js'));
  const engine = (text, key, size, o) => {   // two "fonts": the letters' widths differ from font to font
    const wd = ch => (key === 'a' ? 0.3 + (ch.charCodeAt(0) % 5) * 0.07 : 0.32 + (ch.charCodeAt(0) % 3) * 0.11) * size, gap = 0.08 * size, chars = [...String(text)].filter(c => c !== '\n');
    const total = chars.reduce((a, c) => a + (c === ' ' ? 0.3 * size : wd(c) + gap), 0); let x = (o && o.align) === 'center' ? -total / 2 : 0; const subpaths = [];
    for (const c of chars) { if (c === ' ') { x += 0.3 * size; continue; } const w = wd(c), h = (/[A-Z0-9]/.test(c) ? 0.7 : 0.5) * size; subpaths.push([['m', [x, 0]], ['l', [x + w, 0]], ['l', [x + w, -h]], ['l', [x, -h]], ['h']]); x += w + gap; }
    return { subpaths };
  };
  const shift = (subs, dx, dy, k) => subs.map(sp => sp.map(op => (op[0] === 'h' ? op : [op[0]].concat(op.slice(1).map(p => [p[0] * (k || 1) + dx, p[1] * (k || 1) + dy])))));
  const part = (id, text, key, size, at, extra) => Object.assign({ id, kind: 'path', role: 'hatch', text, subpaths: shift(engine(text, key, size, { align: 'center' }).subpaths, at[0], at[1]), tx: 0, ty: 0, sx: 1, sy: 1, rot: 0 }, extra || {});
  const match = (p, fonts, loose) => W.match(p, { engine, fonts: fonts || ['a', 'b'], loose });
  // the glyph comparison: the very letters in their font and size are found (font, size, place); another font, or the same outlines stretched, are not
  const p1 = part('ce1', 'NOVEMBER 21', 'b', 4.2, [30, 12]), m1 = match(p1), stretched = Object.assign({}, p1, { subpaths: shift(p1.subpaths, 0, 0).map(sp => sp.map(op => (op[0] === 'h' ? op : [op[0]].concat(op.slice(1).map(p => [p[0] * 1.1, p[1]]))))) }), m2 = match(stretched);
  check(`T0a the glyph comparison: words saved as outlines are found as their letters (font ${m1 && m1.fontKey}, ${m1 && m1.sizePt} pt, exact ${m1 && m1.exact}); the same outlines stretched 10 % are not (${m2 ? m2.fontKey + ' exact ' + m2.exact : 'no match'})`,
    !!m1 && m1.exact && m1.fontKey === 'b' && Math.abs(m1.sizePt - 4.2) < 1e-6 && !(m2 && m2.exact), JSON.stringify({ m1, m2 }));
  // the repair rule, each case
  const sorted = (items, job, handed) => W.sortParts({ face: 'front', items, job, handed, match }).map(s => s.act + ':' + s.id + (s.repair ? '+repair' : ''));
  const hb = (t, n, k) => part('engrave-words-front-' + n, t, k || 'a', 3, [20, 5]);
  const a1 = sorted([hb('33.47765N 86.80020W愛', 'mvsjt11z'), { id: '0-x', kind: 'path', role: 'cut', subpaths: [[['m', [0, 0]], ['l', [1, 0]], ['l', [1, 1]], ['h']]] }], { key: 'j', words: [], fontKey: 'a' }, false);
  const a2 = sorted([hb('NAME ONE', 'mv1')], { key: 'j', words: ['NAME', 'ONE'], fontKey: 'a' }, true);
  const a2b = sorted([hb('NAME ONE', 'mv1')], { key: 'j', words: ['NAME ONE'], fontKey: 'a' }, false);
  const a3 = sorted([hb('OLD WORDS', 'mv1')], { key: 'j', words: ['NEW WORDS'], fontKey: 'a' }, true);
  const two = sorted([hb('MIMI', 'mv1'), hb('MIMI', 'mv2')], { key: 'j', words: [], fontKey: 'a' }, false);
  const bOk = sorted([part('ce7', 'HELLO', 'b', 5, [10, 4])], { key: 'j', words: ['X'], fontKey: 'a' }, true), bNo = sorted([Object.assign({}, stretched, { id: 'ce8' })], null, false), noJob = sorted([hb('MIMI', 'mv3')], null, false);
  check(`T0b the repair rule: a hand-back of words the card lost becomes the face's words (${a1}); a copy of the card's words on top of their Type is dropped (${a2}), with no Type it stands in for them (${a2b}); an earlier engraving's words are kept (${a3}); of two hand-backs the newer is the words, the older copy dropped (${two}); the editor's own words read back as Type when their letters prove it (${bOk}), else kept (${bNo}); a hand-back with no engraving on its face is the editor's words (${noJob}); nothing else is touched`,
    a1.join() === 'words:engrave-words-front-mvsjt11z+repair' && a2.join() === 'drop:engrave-words-front-mv1' && a2b.join() === 'words:engrave-words-front-mv1' && a3.join() === 'keep:engrave-words-front-mv1' && two.join() === 'words:engrave-words-front-mv2+repair,drop:engrave-words-front-mv1'
    && bOk.join() === 'type:ce7' && bNo.join() === 'keep:ce8' && noJob.join() === 'type:engrave-words-front-mv3', JSON.stringify({ a1, a2, a2b, a3, two, bOk, bNo, noJob }));
  // what a Save gives the job: words, font and place; what the fit cannot take is said
  const g = { id: 'w', kind: 'text', text: 'Name One', fontKey: 'a', sizePt: 3, align: 'center', tx: 2, ty: 1, sx: 1, sy: 1, rot: 0 };
  const cased = (t, c) => (c === 'upper' ? t.toUpperCase() : t);
  const d0 = W.diff(g, Object.assign({}, g, { name: 'renamed', locked: true }), cased), d1 = W.diff(g, Object.assign({}, g, { letterCase: 'upper', fontKey: 'b', bold: true }), cased), d2 = W.diff(g, Object.assign({}, g, { tx: 3 }), cased), d3 = W.diff(g, null, cased);
  const pl = W.placeOf(Object.assign({}, g, { rot: 90 }), { engine, m: [0, -1] });
  check(`T0c a Save's words: a lock or a name is no change; CAPITALS + another font + Bold give the job "${d1.words.join(' ')}" in the new font, Bold said as not kept (${d1.style}); a move is a place (${d2.place}); deleted is gone (${d3.gone}); the place turned back into the fit's frame (centre ${pl && pl.centre}, angle ${pl && pl.angle})`,
    !d0.changed && d1.changed && d1.text && d1.font && d1.words.join(' ') === 'NAME ONE' && d1.style.join() === 'bold' && d2.place && !d2.text && d3.gone && !!pl && pl.angle === -90 && Math.abs(pl.centre[0] - (pl.at[0] - 1)) < 1e-9 && Math.abs(pl.centre[1] + pl.at[1]) < 1e-9, JSON.stringify({ d0, d1, d2, d3, pl }));
}

(async () => {
  if (!ONLY || ONLY === 'A' || ONLY === 'A0') await partModel();
  if (!ONLY || ONLY === 'T' || ONLY === 'T0') partTypeModel();
  if (ONLY !== 'A0' && ONLY !== 'T0') await partPage();
  const bad = RESULTS.filter(r => !r.ok);
  console.log(`\n${RESULTS.length - bad.length}/${RESULTS.length} checks pass`);
  process.exit(bad.length ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
