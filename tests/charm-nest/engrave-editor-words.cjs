// EDITOR WORDS and MOVEKEYS (Paul, 27 Oct 2026, order 4205006715, a calendar charm whose buyer wrote "Note: the date should be November 10, 2024. Thank you!").
//   A  "The system is forcing me to have extra text on the front of the charm that I do not need. I already added the 'November' and '21' date to the front using the built in editor ... If the user
//      adds text or fonts or makes any changes to a charm using the built-in editor, then the system should not force the user to also add text on top of already added text from within the Charm
//      Editor." The real sorter page over the repo's fake site (_engraveRealWorld.cjs; offline, made-up names) with the calendar (_artCalendar.cjs) ordered for its FRONT with that note: the front waits
//      for words as before; Paul's own path (words "test", Edit charm, MAY taken out, NOVEMBER and 21 typed, the words box emptied and saved) ends on a front with NO words that Approve takes, with its
//      record, its seal and its file (no words in it) as any approval, and the sheet's drawing carries the editor's parts; a front drawn in the editor before any words were given is never asked for
//      them (the buyer's note stays an offer: the face switch does not fill it in); a front with no art edit still waits for its words, exactly as before.
//   B  "there's also an issue when I try to move the text using my arrow keys and shift button for some reason it switches to adjusting the size of the text when I click shift and arrow buttons or
//      just the arrow buttons". The calendar's back with two lines of words: the Line spacing slider pressed with the mouse, then the words pressed and ArrowRight / Shift+ArrowDown: they move 0.05 mm /
//      1 mm and keep their size; the slider pressed again and the arrows still move the words (not the slider); the slider reached with Tab takes its arrows; pushed against the edge the words stop,
//      never smaller.
//   PW_DIR=<playwright node_modules> CHROMIUM=<chrome> node tests/charm-nest/engrave-editor-words.cjs      (EW_ONLY=A or B; SHOTS=<dir> for the screenshots)
'use strict';
const fs = require('node:fs'), path = require('node:path');
const ROOT = path.join(__dirname, '../..');
const RESULTS = [];
const check = (id, ok, evidence) => { RESULTS.push({ id, ok: !!ok }); console.log(`${ok ? 'PASS' : 'FAIL'}  ${id}  ${ok ? '' : evidence || ''}`.trimEnd()); };
const ONLY = process.env.EW_ONLY || '';
const PT = 72 / 25.4;
const NOTE = 'Note: the date should be November 10, 2024. Thank you!';

async function nodeKit() {
  const hadWindow = 'window' in global, hadSelf = 'self' in global;
  global.window = global; global.self = global;
  global.PDFLib = global.PDFLib || require(path.join(ROOT, 'vendor/pdf-lib-1.17.1.min.js'));
  require(path.join(ROOT, 'charm-nest-pdf.js'));
  global.CharmNestGeom = global.CharmNestGeom || require(path.join(ROOT, 'charm-nest-geom.js'));
  const cal = await require('./_artCalendar.cjs')(ROOT);
  const kit = { P: global.CharmNestPDF, cal };
  if (!hadWindow) delete global.window; if (!hadSelf) delete global.self;
  return kit;
}
/** the paths a written face file engraves (every path off its cut reference layer) */
async function engravedOf(P, bytes) {
  const p = await P.parseSource(bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes), 'face.ai');
  return p.segments.concat(p.nested || []).filter(s => s.kind === 'path' && (s.fill || s.stroke) && !/CUT OUTLINE/.test(s.layer || '')).map(s => ({ fill: !!s.fill, bbox: s.bbox }));
}

const settleP = page => page.evaluate(() => new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r))));
async function drag(page, a, b) { await page.mouse.move(a.x, a.y); await page.mouse.down(); await page.mouse.move(b.x, b.y, { steps: 10 }); await page.mouse.up(); await settleP(page); }
async function typeField(page, f, value) { const el = page.locator(`.ce-root [data-f="${f}"]`); await el.click(); await page.keyboard.press('Control+a'); await page.keyboard.type(String(value)); await page.keyboard.press('Enter'); await settleP(page); }
const C = (page, fn, arg) => page.evaluate(fn, arg);
const clientOf = (page, x, y) => C(page, ([x, y]) => window.CharmEditor.current.clientOf(x, y), [x, y]);
const findText = (page, t) => C(page, t => { let f = null; const walk = l => l.forEach(i => { if (i.kind === 'text' && i.text === t) f = i; if (i.children) walk(i.children); }); walk(window.CharmEditor.current.items()); return f; }, t);
const union = bs => bs.reduce((a, b) => [Math.min(a[0], b[0]), Math.min(a[1], b[1]), Math.max(a[2], b[2]), Math.max(a[3], b[3])]);

async function run(kit) {
  const R = require('./_engraveRealWorld.cjs');
  const shots = process.env.SHOTS || '/tmp/engrave-editor-words-shots';
  const c = kit.cal.charm;
  R.DESIGNS.CALENDAR = { file: 'CALENDAR.ai', make: () => Buffer.from(kit.cal.bytes), entry: { sku: 'CALENDAR', widthPt: +(c.bbox[2] - c.bbox[0]).toFixed(3), heightPt: +(c.bbox[3] - c.bbox[1]).toFixed(3), areaPt2: 4200, members: c.members.length, holes: 0, engravable: true, engravableBy: 'index', upAngle: 90, upSource: 'drawn', charmHash: 'calhash01', backKeepOut: [] } };
  const calTitle = 'Calendar Necklace, Personalized Month Calendar Charm';
  // the calendar ordered for its FRONT with the buyer's note (Paul's order, three times: his own path, a front drawn before any words, one nobody draws), and once for its back with plain words
  R.ORDERS.paul = Object.assign({}, R.ORDERS.front, { rid: '4290000151', sku: 'CALENDAR', title: calTitle, buyer: 'Name Eleven', personalization: [NOTE] });
  R.ORDERS.fresh = Object.assign({}, R.ORDERS.front, { rid: '4290000152', sku: 'CALENDAR', title: calTitle, buyer: 'Name Twelve', personalization: [NOTE] });
  R.ORDERS.plain = Object.assign({}, R.ORDERS.front, { rid: '4290000153', sku: 'CALENDAR', title: calTitle, buyer: 'Name Thirteen', personalization: ['Note: the date should be May 3, 2025. Thank you!'] });
  R.ORDERS.move = Object.assign({}, R.ORDERS.back, { rid: '4290000154', sku: 'CALENDAR', listing: '1890000154', title: calTitle, buyer: 'Name Fourteen', personalization: ['Name One'] });
  Object.assign(R.EXPECT_JOBS, { paul: 1, fresh: 1, plain: 1, move: 1 });
  const w = await R.world({ shots });
  if (!w) { console.log('SKIP no playwright-core'); return; }
  const page = w.page, st = w.srv.st, Q = '#egQueue .rvItem[data-kind=placement]';
  try {
    const made = await R.build(w, ONLY === 'B' ? ['move'] : ONLY === 'A' ? ['paul', 'fresh', 'plain'] : ['paul', 'fresh', 'plain', 'move']);
    await page.evaluate(() => {
      window.__ew = {
        card: () => document.querySelector('#egQueue .rvItem[data-kind=placement]'),
        cv: () => document.querySelector('#egQueue .rvItem[data-kind=placement] .backHost canvas'),
        job: () => Engrave.items().get(document.querySelector('#egQueue .rvItem[data-kind=placement]').dataset.key),
        jobOf: (rid, face) => [...Engrave.items().values()].find(j => String(j.row.order.receiptId) === rid && (j.face === 'front' ? 'front' : 'back') === face),
        facts: j => j && ({ state: j.state, words: (j.lines || []).join('/'), artOnly: !!(j.fit && j.fit.artOnly), fit: !!j.fit, size: j.fit ? j.fit.size : null, centre: j.fit ? j.fit.centre.slice() : null, lines: (j.lines || []).slice(), gap: j.lineGap, verify: !!(j.verify && j.verify.geometry && j.verify.geometry.ok), approvedAt: j.approvedAt || 0, seals: CNEngravingSeals.list(j).map(s => s.how + ':' + s.by), reason: j.reason || '' }),
        cardFacts: () => { const c = document.querySelector('#egQueue .rvItem[data-kind=placement]'); if (!c) return null; const cv = c.querySelector('.backHost canvas'), ap = c.querySelector('[data-a="approve"]'); return { key: c.dataset.key, approve: !!ap, approveOff: !!(ap && ap.disabled), approved: !!c.querySelector('.egApproveButton.egApproved'), canvas: !!cv, box: !!(cv && cv._box), art: cv ? (cv._frontArt || []).length : 0, pill: !!c.querySelector('.backHost .egCharmEdit'), noBack: (c.querySelector('.backHost .noBack') || {}).textContent || '', ctl: (c.querySelector('.ctl') || {}).textContent || '', centreBtn: !!c.querySelector('.ctl [data-a="centre"]'), words: (c.querySelector('.engpWords') || {}).value, ask: (c.querySelector('.engpAsk') || {}).textContent || '', offer: (c.querySelector('.engpOffer') || {}).textContent || '', note: (c.querySelector('.backHost .egArtNote') || {}).textContent || '', title: cv ? cv.title : '' }; },
        async sheet(ids) {
          const charms = ids.map(id => Pool.charmOf(id)), placements = charms.map((ch, i) => ({ charm: ch, angle: 0, cxPt: 50 + 90 * i, cyPt: 60, scale: 1 }));
          const bytes = await CharmNestPDF.buildSheet({ sheet: { wPt: 110 + 90 * (ids.length - 1), hPt: 130 }, placements, sources: new Map(charms.map(ch => [ch.sourceId, (sourceOf(ch.sourceId) || {}).parsed])), title: 'editor words' });
          const p = await CharmNestPDF.parseSource(bytes, 'sheet.ai');
          return p.segments.filter(s => s.kind === 'xobj').map(f => ({ blue: f.children.filter(k => k.kind === 'path' && k.fill && k.fillRGB.join() === '0,0,1').map(k => k.bbox), black: f.children.filter(k => k.kind === 'path' && k.fill && k.fillRGB.join() === '0,0,0').length }));
        }
      };
    });
    const job = (rid, face) => page.evaluate(([rid, face]) => __ew.facts(__ew.jobOf(rid, face)), [rid, face]);
    const cardFacts = () => page.evaluate(() => __ew.cardFacts());
    const why = () => page.evaluate(Q => { const f = document.querySelector(Q + ' [data-engp="why"]'); if (f) f.click(); return !!f; }, Q);
    const setWords = text => page.evaluate(({ Q, text }) => { const t = document.querySelector(Q + ' .engpWords'), set = Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype, 'value').set; t.focus(); set.call(t, text); t.dispatchEvent(new InputEvent('input', { bubbles: true, inputType: 'insertText', data: text })); }, { Q, text });
    const savePanel = async () => { await page.click(Q + ' [data-engp="save"]'); await R.settle(page, 400); await why(); await R.settle(page, 600); };
    const waitFor = async (fn, arg, ms = 120000) => { for (let t0 = Date.now(); Date.now() - t0 < ms; ) { if (await page.evaluate(fn, arg)) return true; await new Promise(r => setTimeout(r, 250)); } return false; };
    const sheets = await page.evaluate(() => { const out = []; for (const s of allSheets()) if (s.charms && s.charms.length) { if (!s.folderPath) s.folderPath = 'charmnest/sheets/' + s.sheetId; out.push({ sheetId: s.sheetId, poolIds: s.charms.map(x => x.poolId) }); } return out; });
    for (const s of sheets) st.put('Charm_Nest_Sheets', s.sheetId, { sheetId: s.sheetId, poolIds: s.poolIds, backPool: [] });

    /** the calendar's editor on the open card: MAY swept out, NOVEMBER typed where it was (if asked), "21" at 5 pt centred in the day square of row 2, column 4, then Save */
    async function drawCalendar(pid, month) {
      await page.click(Q + ' .backHost .egCharmEdit');
      await page.waitForSelector('.ce-root .ce-items path', { timeout: 60000 }); await settleP(page); await settleP(page);
      const fr = await page.evaluate(() => CharmEditor.current.frame());
      let letters = [];
      if (month) {
        letters = await page.evaluate(() => CharmEditor.current.items().filter(i => i.role === 'hatch').map(i => i.id));
        const mBox = union(await Promise.all(letters.map(id => C(page, id => CharmEditor.current.box(id), id)))), pad = 0.6 * PT;
        await page.keyboard.press('m');
        await drag(page, await clientOf(page, mBox[0] - pad, mBox[1] - pad), await clientOf(page, mBox[2] + pad, mBox[3] + pad));
        await page.keyboard.press('Delete'); await settleP(page);
        await page.keyboard.press('t');
        const at = await clientOf(page, mBox[0], mBox[3]); await page.mouse.click(at.x, at.y); await settleP(page);
        await page.keyboard.type(month); await page.keyboard.press('Enter'); await settleP(page);
        const wmm = parseFloat(await page.locator('.ce-root [data-f="w"]').inputValue()), hmm = parseFloat(await page.locator('.ce-root [data-f="h"]').inputValue()), mid = [(mBox[0] + mBox[2]) / 2, (mBox[1] + mBox[3]) / 2];
        await typeField(page, 'x', (((mid[0] - fr.x0) / PT) - wmm / 2).toFixed(3));
        await typeField(page, 'y', (((mid[1] - fr.y0) / PT) - hmm / 2).toFixed(3));
        await page.keyboard.press('Escape'); await page.keyboard.press('v'); await settleP(page);
      }
      const squares = await page.evaluate(() => CharmEditor.current.items().filter(i => i.role === 'line').map(i => ({ id: i.id, b: CharmEditor.current.box(i.id) })).sort((a, b) => a.b[1] - b.b[1] || a.b[0] - b.b[0]));
      const sq = squares[1 * 5 + 3].b;
      await page.keyboard.press('t');
      const near = await clientOf(page, sq[0] + 1, sq[3] - 1); await page.mouse.click(near.x, near.y); await settleP(page);
      await page.keyboard.type('21'); await page.keyboard.press('Enter'); await settleP(page);
      await typeField(page, 'pt', '5');
      const wmm = parseFloat(await page.locator('.ce-root [data-f="w"]').inputValue()), hmm = parseFloat(await page.locator('.ce-root [data-f="h"]').inputValue());
      await typeField(page, 'x', ((((sq[0] + sq[2]) / 2 - fr.x0) / PT) - wmm / 2).toFixed(3));
      await typeField(page, 'y', ((((sq[1] + sq[3]) / 2 - fr.y0) / PT) - hmm / 2).toFixed(3));
      const added = [await findText(page, '21'), month ? await findText(page, month) : null].filter(Boolean).map(t => t.id);
      const had = (st.doc('Charm_Pool_Art', pid + '~front') || {}).at || 0;
      await page.click('.ce-root [data-act="save"]');
      for (let t0 = Date.now(); Date.now() - t0 < 60000 && ((st.doc('Charm_Pool_Art', pid + '~front') || {}).at || 0) === had; ) await new Promise(r => setTimeout(r, 200));
      await R.settle(page, 1200);
      return { letters, added, sq };
    }

    if (ONLY !== 'B') {
      const PID = made.paul.poolIds[0], FPID = made.fresh.poolIds[0], RID = made.paul.rid;
      check('A0 the world: the three calendars ordered for the front with a note wait for their front words (the note is no inscription); none has an art edit',
        ['paul', 'fresh', 'plain'].every(n => made[n].jobs.length === 1 && made[n].jobs[0].face === 'front' && made[n].jobs[0].state === 'words' && !made[n].jobs[0].text) && !st.doc('Charm_Pool_Art', PID + '~front'), JSON.stringify(['paul', 'fresh', 'plain'].map(n => made[n].jobs)));

      /* ── as before: no words, no art edit: the front waits for its words; the empty work area now holds the "Edit charm" pill ── */
      await R.openEngraveDetail(w, 'paul', { face: 'front' }); await R.settle(page, 800);
      const a1 = await cardFacts();
      await R.shot(w, 'EW-0-front-waiting-for-words-edit-charm-pill');
      check('A1 the front with no words waits for them as before (no Approve, the panel asks "Say which words go on the front"), and its empty work area holds the quiet "Edit charm" pill, so the charm can be drawn before any words are given',
        !a1.approve && !a1.canvas && /Say which words go on the front/.test(a1.ask) && !a1.offer && a1.pill, JSON.stringify(a1));

      /* ── Paul's own path: words "test", then the charm editor ── */
      await setWords('test'); await savePanel();
      await waitFor(rid => { const j = __ew.jobOf(rid, 'front'); return j && j.state === 'review' && j.fit && !j.fit.artOnly; }, RID);
      await R.openEngraveDetail(w, 'paul', { face: 'front' }); await R.settle(page, 800);
      const a2 = await job(RID, 'front');
      check('A2 Paul types "test" for the front and saves: it is fitted on the front and waits for Approve (a person\'s words)', a2.state === 'review' && /test/i.test(a2.words) && !a2.artOnly, JSON.stringify(a2));
      const drawn = await drawCalendar(PID, 'NOVEMBER');
      const rec = st.doc('Charm_Pool_Art', PID + '~front');
      await waitFor(rid => { const j = __ew.jobOf(rid, 'front'); return j && j.state === 'review' && j.fit; }, RID);
      await R.openEngraveDetail(w, 'paul', { face: 'front' }); await R.settle(page, 1000);
      const a3 = await job(RID, 'front'), a3c = await cardFacts();
      await R.shot(w, 'EW-1-paul-test-and-editor-parts');
      check('A3 the editor saves NOVEMBER and 21 on the front (MAY removed) for this piece; Paul\'s own words "test" are a person\'s, so they stay and are fitted again around the new drawing (his screenshot)',
        !!rec && rec.by === 'Test Operator' && drawn.letters.every(id => rec.removed.includes(id)) && rec.added.map(a => a.text).sort().join() === '21,NOVEMBER' && a3.state === 'review' && /test/i.test(a3.words) && !a3.artOnly && a3c.canvas && a3c.box,
        JSON.stringify({ rec: rec && { by: rec.by, removed: rec.removed.length, added: rec.added.map(a => a.text) }, a3, a3c }));

      /* ── the words box emptied and saved: the front is the editor's parts alone ── */
      await setWords(''); await savePanel();
      await waitFor(rid => { const j = __ew.jobOf(rid, 'front'); return j && j.fit && j.fit.artOnly; }, RID, 30000);
      await R.openEngraveDetail(w, 'paul', { face: 'front' }); await R.settle(page, 900);
      const a4 = await job(RID, 'front'), a4c = await cardFacts();
      await R.shot(w, 'EW-2-paul-words-emptied');
      check('A4 the words box emptied and saved: no words on the front (no letters, no transform box), its work area still draws NOVEMBER and 21, the bottom row says the editor\'s parts are its engraving, Approve is offered, and the panel says words are optional (it does not ask for them)',
        a4.state === 'review' && a4.artOnly && a4.words === '' && a4.verify && a4c.approve && !a4c.approveOff && a4c.canvas && !a4c.box && a4c.art > 20 && /charm editor's parts are this front's engraving/.test(a4c.ctl) && !a4c.centreBtn && a4c.words === '' && /No words needed/.test(a4c.offer) && !a4c.ask,
        JSON.stringify({ a4, a4c }));

      /* ── Approve ── */
      await page.click(Q + ' [data-a="approve"]');
      for (let t0 = Date.now(); Date.now() - t0 < 120000; ) { const b = st.doc('Charm_Pool_Back', PID + '~front'); if (b && b.outputs && b.outputs.ai && b.outputs.png) break; await new Promise(r => setTimeout(r, 300)); }
      await waitFor(rid => { const j = __ew.jobOf(rid, 'front'); return j && j.state === 'written'; }, RID, 60000);
      const back = st.doc('Charm_Pool_Back', PID + '~front'), a5 = await job(RID, 'front');
      const ai = back && back.outputs && back.outputs.ai && st.blobs.get(back.outputs.ai.path), inFile = ai ? await engravedOf(kit.P, new Uint8Array(ai.buf)) : null;
      check('A5 Approve takes the front with no words: its record (Charm_Pool_Back <poolId>~front: who, no words, "artOnly", verified file) and its seal, as any approval; the job is approved and its file written',
        !!back && back.face === 'front' && back.approvedBy === 'Test Operator' && back.text === '' && (back.lines || []).length === 0 && back.artOnly === true && !back.invalidated && back.verified && back.verified.file && back.verified.file.ok === true &&
        (back.engravingSeals || []).some(s => s.how === 'engraveApproved' && s.by === 'Test Operator') && a5.state === 'written' && a5.seals.filter(s => /^engraveApproved:/.test(s)).length === 1,
        JSON.stringify({ back: back && { face: back.face, by: back.approvedBy, text: back.text, lines: back.lines, artOnly: back.artOnly, file: back.verified && back.verified.file, seals: back.engravingSeals }, a5 }));
      check('A6 the front\'s file carries no words at all (nothing engraved off its cut reference): the editor\'s parts are engraved by the sheet\'s own drawing', !!inFile && inFile.length === 0, JSON.stringify(inFile));
      const sheet = await page.evaluate(ids => __ew.sheet(ids), [PID, made.plain.poolIds[0]]);
      const wide = sheet[0] && sheet[0].blue.find(b => b[2] - b[0] > 20), small = sheet[0] && sheet[0].blue.find(b => b[2] - b[0] < 8);
      check('A7 the sheet\'s drawing of the piece carries the editor\'s NOVEMBER and 21 as blue hatching, MAY gone (the other calendar keeps its three letters), and no black words',
        sheet.length === 2 && sheet[0].blue.length === 2 && !!wide && !!small && sheet[1].blue.length === 3 && sheet[0].black === 0, JSON.stringify(sheet));
      await R.openEngraveDetail(w, 'paul', { face: 'front' }); await R.settle(page, 1000);
      const a8c = await cardFacts();
      await R.shot(w, 'EW-3-paul-front-approved-editor-parts-only');
      check('A8 opened again, the approved front shows "Approved" with the editor\'s parts and no words', a8c.approved && !a8c.approve && a8c.canvas && !a8c.box && a8c.art > 20, JSON.stringify(a8c));

      /* ── a front drawn in the editor before any words: never asked for them ── */
      await R.openEngraveDetail(w, 'fresh', { face: 'front' }); await R.settle(page, 800);
      const f0 = await cardFacts();
      await drawCalendar(FPID, null);
      await waitFor(rid => { const j = __ew.jobOf(rid, 'front'); return j && j.fit && j.fit.artOnly; }, made.fresh.rid, 30000);
      await R.openEngraveDetail(w, 'fresh', { face: 'front' }); await R.settle(page, 900);
      const f1 = await job(made.fresh.rid, 'front'), f1c = await cardFacts();
      await R.shot(w, 'EW-4-fresh-front-drawn-no-words-asked');
      check('A9 a front still waiting for words, drawn in the editor from its empty work area (21 in a day square): it is placed as drawn and waits for Approve; the buyer\'s note is not put in as words, the panel says they are optional, the work area\'s note says none are needed',
        f0.pill && !f0.approve && f1.state === 'review' && f1.artOnly && f1.words === '' && f1c.approve && !f1c.approveOff && f1c.words === '' && /No words needed/.test(f1c.offer) && /No words needed on the front/.test(f1c.note),
        JSON.stringify({ f0, f1, f1c }));
      // the face switch offers, never fills in: None then Front leaves the drawn front's words box empty; on a front nobody drew it fills in the buyer's note, as it always did
      const switchWords = async name => { await R.openEngraveDetail(w, name, { face: 'front' }); await R.settle(page, 700); await page.click(Q + ' .engpSeg[data-mode="none"]'); await R.settle(page, 300); await page.click(Q + ' .engpSeg[data-mode="front"]'); await R.settle(page, 300); const v = await page.evaluate(Q => (document.querySelector(Q + ' .engpWords') || {}).value, Q); await page.click(Q + ' [data-engp="cancel"]'); await R.settle(page, 300); return v; };
      const swFresh = await switchWords('fresh'), swPlain = await switchWords('plain');
      check('A10 the face switch (None, then Front) leaves the drawn front\'s words box empty; on a front with no art edit it fills in the buyer\'s note exactly as before', swFresh === '' && swPlain === 'Note: the date should be May 3, 2025. Thank you!', JSON.stringify({ swFresh, swPlain }));

      /* ── a front with no art edit and no words: exactly as before ── */
      await R.openEngraveDetail(w, 'plain', { face: 'front' }); await R.settle(page, 700);
      const p0 = await job(made.plain.rid, 'front'), p0c = await cardFacts();
      check('A11 the front nobody drew still waits for its words: "words", no Approve, the panel asks for them, no offer line', p0.state === 'words' && !p0.fit && !p0c.approve && /Say which words go on the front/.test(p0c.ask) && !p0c.offer, JSON.stringify({ p0, p0c }));
    }

    if (ONLY !== 'A') {
      /* ═══ B · the arrows move the words ═══ */
      const MID = made.move.rid;
      await R.openEngraveDetail(w, 'move', { face: 'back' }); await R.settle(page, 800);
      await page.selectOption(Q + ' [data-a="linecount"]', '2');
      await waitFor(rid => { const j = __ew.jobOf(rid, 'back'); return j && j.state === 'review' && j.fit && (j.lines || []).length === 2; }, MID, 60000);
      await R.openEngraveDetail(w, 'move', { face: 'back' }); await R.settle(page, 800);
      const b0 = await job(MID, 'back');
      const sliderBox = await page.locator(Q + ' [data-a="spacing"]').boundingBox();
      check('B0 the back\'s words on two lines: the Line spacing slider is in the bottom row', b0.state === 'review' && b0.lines.length === 2 && !!sliderBox, JSON.stringify({ b0, sliderBox }));
      // the slider pressed with the mouse (it changes the spacing, as it should)
      await page.mouse.click(sliderBox.x + sliderBox.width * 0.7, sliderBox.y + sliderBox.height / 2); await R.settle(page, 500);
      const s0 = await job(MID, 'back'), spacing0 = await page.evaluate(Q => +document.querySelector(Q + ' [data-a="spacing"]').value, Q);
      const focus0 = await page.evaluate(() => document.activeElement && document.activeElement.dataset && document.activeElement.dataset.a);
      // the words pressed in the work area, then the arrows
      const wordsAt = await page.evaluate(() => { const cv = __ew.cv(), j = __ew.job(), p = cv._map.tx(j.fit.centre[0], j.fit.centre[1]), r = cv.getBoundingClientRect(); return { x: r.left + p[0] * r.width / cv.width, y: r.top + p[1] * r.height / cv.height }; });
      await page.mouse.click(wordsAt.x, wordsAt.y); await R.settle(page, 300);
      const focus1 = await page.evaluate(() => document.activeElement && document.activeElement.tagName);
      const s1 = await job(MID, 'back');
      await page.keyboard.press('ArrowRight'); await R.settle(page, 200);
      const s2 = await job(MID, 'back');
      await page.keyboard.press('Shift+ArrowDown'); await R.settle(page, 200);
      const s3 = await job(MID, 'back'), spacing3 = await page.evaluate(Q => +document.querySelector(Q + ' [data-a="spacing"]').value, Q), title = (await cardFacts()).title;
      await R.shot(w, 'EW-5-words-moved-by-arrows');
      const mm = (a, b, i) => (b.centre[i] - a.centre[i]) / PT;
      check(`B1 the slider pressed with the mouse (${focus0} focused, spacing ${spacing0}%), then the words pressed (the drawing takes the focus: ${focus1}) and ArrowRight: the words move 0.05 mm right, their size and lines unchanged`,
        focus0 === 'spacing' && focus1 === 'CANVAS' && s1.size === s0.size && Math.abs(mm(s1, s2, 0) - 0.05) < 1e-6 && Math.abs(mm(s1, s2, 1)) < 1e-9 && s2.size === s0.size && s2.lines.join('/') === s0.lines.join('/') && s2.verify,
        JSON.stringify({ focus0, focus1, s0: [s0.size, s0.centre], s2: [s2.size, s2.centre] }));
      check('B2 Shift+ArrowDown moves them 1 mm down, the size unchanged; the slider kept its value; the drawing\'s title says the steps', Math.abs(mm(s2, s3, 1) + 1) < 1e-6 && Math.abs(mm(s2, s3, 0)) < 1e-9 && s3.size === s0.size && spacing3 === spacing0 && /0\.05 mm, 1 mm with Shift/.test(title),
        JSON.stringify({ s3: [s3.size, s3.centre], spacing3, title }));
      // the slider pressed again, and the arrows straight after it (the words not pressed): they still move the words, never the slider's value
      await page.mouse.click(sliderBox.x + sliderBox.width * 0.7, sliderBox.y + sliderBox.height / 2); await R.settle(page, 400);
      const s4 = await job(MID, 'back'), spacing4 = await page.evaluate(Q => +document.querySelector(Q + ' [data-a="spacing"]').value, Q), focus4 = await page.evaluate(() => document.activeElement && document.activeElement.dataset && document.activeElement.dataset.a);
      await page.keyboard.press('ArrowLeft'); await page.keyboard.press('ArrowUp'); await R.settle(page, 300);
      const s5 = await job(MID, 'back'), spacing5 = await page.evaluate(Q => +document.querySelector(Q + ' [data-a="spacing"]').value, Q);
      check('B3 with the slider still focused from the mouse, ArrowLeft / ArrowUp move the words 0.05 mm each and leave the slider and the size alone', focus4 === 'spacing' && spacing5 === spacing4 && Math.abs(mm(s4, s5, 0) + 0.05) < 1e-6 && Math.abs(mm(s4, s5, 1) - 0.05) < 1e-6 && s5.size === s4.size && s5.gap === s4.gap,
        JSON.stringify({ focus4, spacing4, spacing5, s4: [s4.size, s4.centre], s5: [s5.size, s5.centre] }));
      // the slider reached with the keyboard keeps its arrows
      await page.evaluate(Q => document.querySelector(Q + ' [data-a="linecount"]').focus(), Q);
      await page.keyboard.press('Tab'); await R.settle(page, 100);
      const focus6 = await page.evaluate(() => document.activeElement && document.activeElement.dataset && document.activeElement.dataset.a);
      await page.keyboard.press('ArrowRight'); await page.keyboard.press('ArrowRight'); await R.settle(page, 600);
      const s6 = await job(MID, 'back'), spacing6 = await page.evaluate(Q => +document.querySelector(Q + ' [data-a="spacing"]').value, Q);
      check('B4 the slider reached with Tab takes its arrows (keyboard use): the spacing goes up 2 %, the words stay where they were', focus6 === 'spacing' && spacing6 === spacing5 + 2 && Math.abs(mm(s5, s6, 0)) < 1e-9 && Math.abs(mm(s5, s6, 1)) < 1e-9,
        JSON.stringify({ focus6, spacing5, spacing6, s6: [s6.size, s6.centre] }));
      // pushed against the edge: the words stop there, never smaller
      await page.mouse.click(wordsAt.x, wordsAt.y); await page.evaluate(() => __ew.cv().focus()); await R.settle(page, 200);
      const e0 = await job(MID, 'back'), sizes = [];
      for (let i = 0; i < 40; i++) { await page.keyboard.press('Shift+ArrowLeft'); sizes.push((await job(MID, 'back')).size); }
      const e1 = await job(MID, 'back');
      await R.shot(w, 'EW-6-words-pushed-to-the-edge-same-size');
      check('B5 forty Shift+ArrowLeft presses push the words to the edge of the metal: they stop there (checked clear), and their size never changes', sizes.every(s => s === e0.size) && e1.verify && e1.centre[0] < e0.centre[0] - 3 * PT && e1.state === 'review',
        JSON.stringify({ e0: [e0.size, e0.centre], e1: [e1.size, e1.centre], sizes: [...new Set(sizes)] }));
    }
    check('Z no page error', w.errors.length === 0, JSON.stringify(w.errors).slice(0, 400));
    console.log('screenshots in ' + shots);
  } finally { await w.close(); }
}

(async () => {
  const kit = await nodeKit();
  await run(kit);
  const bad = RESULTS.filter(r => !r.ok);
  console.log(`\n${RESULTS.length - bad.length}/${RESULTS.length} checks pass`);
  process.exit(bad.length ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
