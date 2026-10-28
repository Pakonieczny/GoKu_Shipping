// CHARM EDIT · PAUL'S REAL CASES (28 Oct 2026), on the real masters read from the live site (read-only: masterGet, and artList for his saved record) and seeded into the offline world
// (_engraveRealWorld.cjs: the real sorter page over the repo's fake site; made-up names, nothing written anywhere real):
//   fixtures/PERCHED_43564.ai   the drawing of SILHOUETTE_34560 (order 4211572095: a bird, its hatch and three cut lines; engraved on the BACK, "T")
//   fixtures/TINY_BAR_7427.ai   the drawing of TINY BAR_7427 (order 4203083230: engraved on the FRONT, "33.47765N 86.80020W" in Angelina)
//   fixtures/tinybar-front-art-4203083230.json   the front artwork record Paul saved for that bar (Charm_Pool_Art): his photo traced in 8 greys, 250 KB of paths
//   B  APPROVEHIT ("Pressing the green Approve does nothing"): his photo on the bar's front saved in the Charm Editor, then ONE real mouse press on Approve (a press at the button's place on the
//      screen, not a script's click: what was missed before) approves the words and the art and writes the front's file with every grey in it; the same on the bird's BACK after a part was
//      added there in the editor
//   PW_DIR=<playwright node_modules> CHROMIUM=<chrome> node tests/charm-nest/charm-edit-real-cases.cjs      (CR_ONLY=B; SHOTS=<dir> for the screenshots)
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
    check('E1 no page error', w.errors.length === 0, JSON.stringify(w.errors).slice(0, 600));
    console.log('screenshots in ' + shots);
  } finally { await w.close(); }
}

(async () => {
  await partPage();
  const bad = RESULTS.filter(r => !r.ok);
  console.log(`\n${RESULTS.length - bad.length}/${RESULTS.length} checks pass`);
  process.exit(bad.length ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
