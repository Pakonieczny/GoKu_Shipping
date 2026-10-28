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
//   PW_DIR=<playwright node_modules> CHROMIUM=<chrome> node tests/charm-nest/charm-edit-real-cases.cjs      (CR_ONLY=A0, A or B; SHOTS=<dir> for the screenshots)
'use strict';
const path = require('node:path'), fs = require('node:fs');
const ROOT = path.join(__dirname, '../..'), FIX = path.join(__dirname, 'fixtures');
const RESULTS = [];
const check = (id, ok, evidence) => { RESULTS.push({ id, ok: !!ok }); console.log(`${ok ? 'PASS' : 'FAIL'}  ${id}  ${ok ? '' : evidence || ''}`.trimEnd()); };
const ONLY = process.env.CR_ONLY || '';
const Q = '#egQueue .rvItem[data-kind=placement]';
const ART = JSON.parse(fs.readFileSync(path.join(FIX, 'tinybar-front-art-4203083230.json'), 'utf8'));
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

    if (!ONLY || ONLY === 'B') {
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
    check('E1 no page error', w.errors.length === 0, JSON.stringify(w.errors).slice(0, 600));
    console.log('screenshots in ' + shots);
  } finally { await w.close(); }
}

(async () => {
  if (!ONLY || ONLY === 'A' || ONLY === 'A0') await partModel();
  if (ONLY !== 'A0') await partPage();
  const bad = RESULTS.filter(r => !r.ok);
  console.log(`\n${RESULTS.length - bad.length}/${RESULTS.length} checks pass`);
  process.exit(bad.length ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
