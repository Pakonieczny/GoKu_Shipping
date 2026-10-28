// CETEXT (Paul, 28 Oct 2026: "When I press the 'Open in Charm Editor' button everything seems to be working correctly, however the text that is currently on the charm does not show up in the editor
// and I also don't see a way to change the text style and font style and alphabet style inside the charm editor as I had requested") and APPROVE AFTER THE EDITOR (Paul, 28 Oct 2026, order
// 4203083230: "After I added an image to this order and then try to approve the engraving by using the 'Approve' button, the system did nothing to approve the engraving and the image I added").
//   A  (node) the editor's model: the letter cases (Paul's "alphabet style": CAPITALS, lowercase, Title Case, Small Caps) are drawn from the words as typed; bold, italic and small capitals reach the
//      font engine; each style is its own drawing (cache) and a style set and unset is no change
//   B  the real sorter page over the repo's fake site (_engraveRealWorld.cjs; offline, made-up names): the back of a Stylish order ("Name One", turned on the charm with the work area's angle) opened
//      in the Charm Editor from its card: the words are ONE editable text on the back (the editor's own text kind), in the card's font, at the fitted size, standing exactly where the card fitted
//      them (to 0.05 pt), selected, with the text controls on the bar: the Font menu (every font of the card's drop-down, each name drawn in its own font, its kind beside it), Bold, Italic,
//      the alignment and the Alphabet menu. Each control draws the words again at once and is one step of the undo. Save: the words as the editor drew them are the back's engraving (the art
//      edit's record, their text kept), the card's own words come off (no words engraved twice), and Approve approves it and writes the back file
//   C  words handed in and not changed are no change: another part added on the front of a front + back order is saved alone, and the card keeps its words on both faces
//   D  Approve after the editor: a front order, an image (grey fills, as the image importer makes them) added in the editor and saved, back on the card: one Approve press approves it and writes the
//      front file; and an Approve pressed while that save is still going (slow) waits for it, says so at once, and approves what the save left (never the placement before it)
//   PW_DIR=<playwright node_modules> CHROMIUM=<chrome> node tests/charm-nest/charm-editor-words.cjs      (CW_ONLY=A, B, C or D; SHOTS=<dir> for the screenshots)
'use strict';
const path = require('node:path');
const ROOT = path.join(__dirname, '../..');
const RESULTS = [];
const check = (id, ok, evidence) => { RESULTS.push({ id, ok: !!ok }); console.log(`${ok ? 'PASS' : 'FAIL'}  ${id}  ${ok ? '' : evidence || ''}`.trimEnd()); };
const ONLY = process.env.CW_ONLY || '';
const Q = '#egQueue .rvItem[data-kind=placement]';

/* ═══ A · the model ═══ */
function partA() {
  const CE = require(path.join(ROOT, 'charm-nest-charm-editor.js')), M = CE.model;
  const cases = ['upper', 'lower', 'title', ''].map(c => M.casedText('mIMI de-la cruz', c));
  check('A1 the letter cases are drawn from the words as typed: CAPITALS, lowercase, Title Case (after a space or a hyphen), as typed', cases.join('|') === 'MIMI DE-LA CRUZ|mimi de-la cruz|Mimi De-La Cruz|mIMI de-la cruz', cases.join('|'));
  const asked = [];
  const engine = (t, k, s, o) => { asked.push([t, o.bold || false, o.italic || false, o.smallCaps || false]); return CE.builtinTextToPaths(t, k, s, o); };
  const geo = M.createGeo(engine), base = { id: 't', kind: 'text', role: 'hatch', text: 'Mimi', fontKey: 'x', sizePt: 10, align: 'center', tx: 0, ty: 0, sx: 1, sy: 1, rot: 0 };
  const g0 = geo.raw(base), gU = geo.raw(Object.assign({}, base, { letterCase: 'upper' })), gB = geo.raw(Object.assign({}, base, { bold: true, italic: true })), gS = geo.raw(Object.assign({}, base, { letterCase: 'small' }));
  check('A2 the engine is asked for the words in their case and for bold, italic and small capitals; each style is a drawing of its own', asked.some(a => a[0] === 'MIMI') && asked.some(a => a[1] && a[2]) && asked.some(a => a[3] && a[0] === 'Mimi') && g0 !== gU && g0 !== gB && g0 !== gS && geo.raw(base) === g0, JSON.stringify(asked));
  const out = it => M.exportItems([it], geo)[0], d = M.diffFace([out(base)], [out(Object.assign({}, base))]);
  const set = M.retext(base, { bold: true }, geo), unset = M.retext(set, { bold: undefined }, geo); delete unset.bold;
  check('A3 a style set and unset again is no change (the field leaves with it)', M.changeCount(d) === 0 && M.changeCount(M.diffFace([out(base)], [out(unset)])) === 0 && M.changeCount(M.diffFace([out(base)], [out(set)])) === 1, '');
}

/* ═══ B, C, D · the real page ═══ */
const settleP = page => page.evaluate(() => new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r))));
async function partBCD() {
  const R = require('./_engraveRealWorld.cjs');
  const shots = process.env.SHOTS || '/tmp/charm-editor-words-shots';
  const w = await R.world({ viewport: { width: 1440, height: 900 }, shots });
  if (w) await w.page.waitForFunction(() => !window.CNRole || ['ask', 'role', 'admin'].includes(CNRole.state()), null, { timeout: 20000 }).catch(() => {}).then(() => w.page.evaluate(() => { try { if (window.CNRole && CNRole.state() === 'ask') CNRole.choose('design'); } catch (_) { /* no role step on this page */ } }));   // (the "Laser or Design?" step (charm-nest-role.js) is answered as a person would: its bar, fixed over the top right, otherwise covers the card's Open in Charm Editor)
  if (!w) { console.log('SKIP no playwright-core'); return; }
  const page = w.page, st = w.srv.st;
  const until = async (fn, ms) => { const t0 = Date.now(); for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) return null; await new Promise(r => setTimeout(r, 150)); } };
  const artPuts = () => st.calls.filter(x => x.name === 'charmNestLibrary' && x.op === 'artPut').length;
  try {
    const made = await R.build(w, ['back', 'both', 'front']);
    const sheets = await page.evaluate(() => { const out = []; for (const s of allSheets()) if (s.charms && s.charms.length) { if (!s.folderPath) s.folderPath = 'charmnest/sheets/' + s.sheetId; out.push({ sheetId: s.sheetId, poolIds: s.charms.map(x => x.poolId) }); } return out; });
    for (const s of sheets) st.put('Charm_Nest_Sheets', s.sheetId, { sheetId: s.sheetId, poolIds: s.poolIds, backPool: [] });
    await page.evaluate(() => {
      /** the card's job, the words handed in, the fitted words' box in the editor's frame (x, -y of the job's view) and the editor's own outlines of them */
      window.__cw = {
        job: () => { const c = document.querySelector('#egQueue .rvItem[data-kind=placement]'); return c && Engrave.items().get(c.dataset.key); },
        subs(cmds) { const out = []; let sp = null, cur = null; for (const c of cmds || []) { if (c.type === 'M') { cur = [c.x, -c.y]; sp = [['m', cur]]; out.push(sp); } else if (c.type === 'Z') { if (sp) sp.push(['h']); sp = null; } else if (sp) { const p = [c.x, -c.y]; if (c.type === 'L') sp.push(['l', p]); else if (c.type === 'C') sp.push(['c', [c.x1, -c.y1], [c.x2, -c.y2], p]); else if (c.type === 'Q') sp.push(['c', [c.x1, -c.y1], [c.x1, -c.y1], p]); cur = p; } } return out; },
        fitBox: j => CharmEditor.model.bboxOf(__cw.subs(j.fit.cmds)),
        words: face => { const E = CharmEditor.current; let hit = null; const walk = l => l.forEach(i => { if (i.source === 'engrave' && i.kind === 'text') hit = i; if (i.children) walk(i.children); }); walk(E.items(face)); return hit; },
        drawn: (face, id) => { const it = CharmEditor.current.result().faces[face].items.find(i => i.id === id); return it ? { box: CharmEditor.model.bboxOf(it.subpaths), ink: CharmEditor.model.inkArea(it.subpaths), n: it.subpaths.length } : null; }
      };
    });
    const near = (a, b, tol) => !!(a && b) && a.every((v, i) => Math.abs(v - b[i]) < tol);

    if (!ONLY || ONLY === 'B') {
      /* ── B · the words of the back in the editor ── */
      await R.openEngraveDetail(w, 'back', { face: 'back' }); await R.settle(page, 700);
      const turned = await page.evaluate(() => { const j = __cw.job(); const ok = Engrave.rotateTo(j, 14); return { ok, angle: j.fit && j.fit.angle, state: j.state }; });
      await R.settle(page, 400);
      const j0 = await page.evaluate(() => { const j = __cw.job(); return { key: j.key, face: j.face || 'back', lines: j.fit.lines || j.lines, fontKey: j.fontKey, size: j.fit.size, angle: j.fit.angle, box: __cw.fitBox(j), weight: j.fit.weight }; });
      await page.click(Q + ' .egCharmEdit');
      await page.waitForSelector('.ce-root .ce-items path', { timeout: 60000 }); await settleP(page); await settleP(page);
      const sel0 = await page.evaluate(() => CharmEditor.current.selection());
      // (a click on the words, as a person selects them)
      { const b = await page.evaluate(() => { const w = __cw.words('back'); return w && CharmEditor.current.box(w.id); }); if (b) { const c = await page.evaluate(([x, y]) => CharmEditor.current.clientOf(x, y), [(b[0] + b[2]) / 2, (b[1] + b[3]) / 2]); await page.mouse.click(c.x, c.y); await settleP(page); } }
      const o1 = await page.evaluate(() => {
        const E = CharmEditor.current, wd = __cw.words('back'), fr = __cw.words('front'), d = wd && __cw.drawn('back', wd.id), bar = [...document.querySelectorAll('.ce-root .ce-props [data-when="text"]')];
        return { face: E.face(), wd, front: !!fr, d, sel: E.selection(), barShown: bar.length === 2 && bar.every(e => !e.hidden && e.getBoundingClientRect().width > 0), fontName: document.querySelector('.ce-root [data-fontname]').textContent, fontFam: getComputedStyle(document.querySelector('.ce-root [data-fontname]')).fontFamily, pt: document.querySelector('.ce-root [data-f="pt"]').value, caseName: document.querySelector('.ce-root [data-tsname]').textContent, alignOn: (document.querySelector('.ce-root [data-talign].is-on') || {}).dataset };
      });
      check(`B1 Open in Charm Editor on the back (the words turned ${(+j0.angle || 0).toFixed(1)}° on the charm): the card's words are ONE text of the editor on the back (its own text kind, "engrave"), the very lines the card fitted, in its font (${j0.fontKey}) at its size; none on the front (this order engraves its back)`,
        turned.ok !== false && Math.abs(+j0.angle) > 1 && o1.face === 'back' && !!o1.wd && o1.wd.kind === 'text' && o1.wd.text === j0.lines.join('\n') && o1.wd.fontKey === j0.fontKey && Math.abs(o1.wd.sizePt - j0.size) < 1e-6 && Math.abs(o1.wd.rot + j0.angle) < 1e-6 && !o1.front,
        JSON.stringify({ turned, j0, wd: o1.wd && { text: o1.wd.text, fontKey: o1.wd.fontKey, sizePt: o1.wd.sizePt, rot: o1.wd.rot }, front: o1.front }));
      check('B2 …standing exactly where the card fitted them: the editor\'s outlines of the words and the fitted letters have the same box on the charm (to 0.05 pt)', near(o1.d && o1.d.box, j0.box, 0.05), JSON.stringify({ drawn: o1.d && o1.d.box, fitted: j0.box }));
      o1.sel0 = sel0;
      await R.shot(w, 'CW-0-opened-words-selected');
      check(`B3 the words open unselected (a picture imported first goes into the outline), a click selects them and their text controls are on the bar: Font "${o1.fontName}" drawn in itself (${o1.fontFam}), ${o1.pt} pt, and "${o1.caseName}" (Bold, Italic, the alignment: centre, as the card centres them, the Alphabet)`,
        o1.sel0.length === 0 && o1.sel.join() === (o1.wd && o1.wd.id) && o1.barShown && o1.fontName === 'Stylish' && /CNEng playwrite-us-trad/.test(o1.fontFam) && +o1.pt > 0 && o1.caseName === 'As typed' && o1.alignOn && o1.alignOn.talign === 'center', JSON.stringify(o1));
      // the Font menu: every font of the card's own drop-down, each drawn in itself with its kind beside it
      await page.click('.ce-root [data-drop="font"] .ce-drop__b'); await settleP(page);
      const menu = await page.evaluate(() => {
        const items = [...document.querySelectorAll('.ce-root [data-drop="font"] .ce-menu .ce-item[data-font]')].map(b => ({ key: b.dataset.font, name: b.textContent, fam: getComputedStyle(b).fontFamily, kind: b.dataset.kind || '', after: getComputedStyle(b, '::after').content }));
        const card = (CharmNestEngraveSetup.fontChoices ? CharmNestEngraveSetup.fontChoices() : []).map(c => c.word);
        return { items, card, open: !document.querySelector('.ce-root [data-drop="font"] .ce-menu').hidden };
      });
      await R.shot(w, 'CW-1-font-menu');
      const names = menu.items.map(i => i.name);
      check(`B4 the Font menu holds every font of the card's drop-down (${menu.card.join(', ')}), each name drawn in its own font, its kind beside it (${menu.items.filter(i => i.kind).map(i => i.name + ': ' + i.kind).slice(0, 4).join(', ')} …)`,
        menu.open && menu.card.length >= 6 && menu.card.every(c => names.includes(c)) && menu.items.every(i => /CNEng /.test(i.fam)) && menu.items.find(i => i.name === 'Stylish').kind === 'cursive' && /capitals only/.test((menu.items.find(i => i.name === 'Ace') || {}).kind || ''), JSON.stringify(menu));
      // a font picked in the menu: drawn again at once, where they were; one step of the undo
      const wid = o1.wd.id, h0 = await page.evaluate(() => CharmEditor.current.history().undo);
      const anchor = it => { const g = CharmEditor.current.ctx.geo, m = g.matrix(it); return [m[4], m[5]]; };   // (where the words' anchor, the middle of their first baseline, lands on the charm)
      const an0 = await page.evaluate(`(${anchor})(__cw.words("back"))`);
      await page.click('.ce-root [data-drop="font"] .ce-menu .ce-item[data-font="crimson-text"]').catch(() => {});
      const f1 = await page.evaluate(id => ({ wd: __cw.words('back'), d: __cw.drawn('back', id), h: CharmEditor.current.history().undo, name: document.querySelector('.ce-root [data-fontname]').textContent }), wid);
      const an1 = await page.evaluate(`(${anchor})(__cw.words("back"))`);
      check('B5 Typewriter picked from the menu: the words are drawn again at once in Crimson Text (other outlines), their anchor (the middle of their first baseline) where it was, one step of the undo; the bar says Typewriter',
        f1.wd.fontKey === 'crimson-text' && f1.wd.text === o1.wd.text && f1.d && JSON.stringify(f1.d.box) !== JSON.stringify(o1.d.box) && near(an1, an0, 0.01) && f1.h === h0 + 1 && f1.name === 'Typewriter', JSON.stringify({ f1, h0, an0, an1 }));
      // (the card fitted these words small: under 2.2 mm of capital the font's heavier cut is the one cut anyway, as the fit cuts it; at 16 pt Bold shows)
      { const el = page.locator('.ce-root [data-f="pt"]'); await el.click(); await page.keyboard.press('Control+a'); await page.keyboard.type('16'); await page.keyboard.press('Enter'); await settleP(page); }
      const f8 = await page.evaluate(id => ({ wd: __cw.words('back'), d: __cw.drawn('back', id) }), wid);
      // Bold, Italic, the alignment, the Alphabet
      const style = async sel => { if (await page.evaluate(() => document.querySelector('.ce-root [data-drop="font"] .ce-menu').hidden)) { await page.click('.ce-root [data-drop="font"] .ce-drop__b'); await settleP(page); } await page.click('.ce-root [data-drop="font"] .ce-menu ' + sel); await settleP(page); };   // (the Style menu: it stays open from one pick to the next)
      await style('[data-tstyle="bold"]');
      const b1 = await page.evaluate(id => ({ wd: __cw.words('back'), d: __cw.drawn('back', id), on: document.querySelector('.ce-root [data-tstyle="bold"]').classList.contains('is-on') }), wid);
      await style('[data-tstyle="italic"]');
      const i1 = await page.evaluate(id => ({ wd: __cw.words('back'), d: __cw.drawn('back', id), on: document.querySelector('.ce-root [data-tstyle="italic"]').classList.contains('is-on') }), wid);
      await style('[data-talign="right"]');
      const a1 = await page.evaluate(() => ({ wd: __cw.words('back'), on: (document.querySelector('.ce-root [data-talign].is-on') || {}).dataset }));
      check(`B6 at 16 pt, Bold draws the family's heavier cut (more ink: ${f8.d.ink.toFixed(1)} → ${b1.d.ink.toFixed(1)} pt²), Italic slants the letters (another box), right alignment is kept on the words; each lit on the bar`,
        f8.wd.sizePt === 16 && b1.wd.bold === true && b1.on && b1.d.ink > f8.d.ink * 1.04 && i1.wd.italic === true && i1.on && JSON.stringify(i1.d.box) !== JSON.stringify(b1.d.box) && a1.wd.align === 'right' && a1.on && a1.on.talign === 'right', JSON.stringify({ b1: b1.d, i1: i1.d, a1 }));
      const caseMenu = await page.evaluate(() => [...document.querySelectorAll('.ce-root [data-drop="font"] .ce-menu [data-case]')].map(b => b.dataset.case + ':' + b.getAttribute('aria-label')));
      await style('[data-case="upper"]');
      const u1 = await page.evaluate(id => ({ wd: __cw.words('back'), d: __cw.drawn('back', id), name: document.querySelector('.ce-root [data-tsname]').textContent }), wid);
      await style('[data-case="small"]');
      const s1 = await page.evaluate(id => ({ wd: __cw.words('back'), d: __cw.drawn('back', id), name: document.querySelector('.ce-root [data-tsname]').textContent }), wid);
      await R.shot(w, 'CW-2-text-selected-controls');
      check(`B7 the Alphabet menu (${caseMenu.join(', ')}): CAPITALS draws NAME ONE from the words as typed (still "${u1.wd.text}"), Small Caps draws the small letters as small capitals (lower than the capitals: ${(u1.d.box[3] - u1.d.box[1]).toFixed(2)} → ${(s1.d.box[3] - s1.d.box[1]).toFixed(2)} pt high)`,
        caseMenu.length === 5 && u1.wd.letterCase === 'upper' && u1.wd.text === o1.wd.text && /CAPITALS/.test(u1.name) && JSON.stringify(u1.d.box) !== JSON.stringify(i1.d.box) && s1.wd.letterCase === 'small' && /Small Caps/.test(s1.name) && JSON.stringify(s1.d.box) !== JSON.stringify(u1.d.box), JSON.stringify({ u1: u1.d, s1: s1.d, caseMenu }));
      // the undo takes every step back, one by one, to the words as they were handed in; the redo puts them back
      const steps = await page.evaluate(() => CharmEditor.current.history().undo);
      for (let i = 0; i < 7; i++) { await page.keyboard.press('Control+z'); await settleP(page); }
      const z0 = await page.evaluate(id => ({ wd: __cw.words('back'), d: __cw.drawn('back', id), dirty: CharmEditor.current.dirty() }), wid);
      for (let i = 0; i < 7; i++) { await page.keyboard.press('Control+Shift+z'); await settleP(page); }
      const z1 = await page.evaluate(id => ({ wd: __cw.words('back'), d: __cw.drawn('back', id) }), wid);
      check(`B8 seven steps on the undo (font, size, bold, italic, alignment, two letter cases): undone, the words are as they were handed in (no change at all); redone, Small Caps in Typewriter 16 pt bold italic, right`,
        steps === h0 + 7 && z0.wd.fontKey === j0.fontKey && !z0.wd.bold && !z0.wd.italic && !z0.wd.letterCase && z0.wd.align === 'center' && near(z0.d.box, o1.d.box, 1e-6) && !z0.dirty && z1.wd.fontKey === 'crimson-text' && z1.wd.bold && z1.wd.italic && z1.wd.letterCase === 'small' && z1.wd.align === 'right' && near(z1.d.box, s1.d.box, 1e-6),
        JSON.stringify({ steps, h0, z0: z0.wd, z1: z1.wd, dirty: z0.dirty }));
      // retyped: double-click the words, the editor's own words box
      { const b = z1.d.box, c = await page.evaluate(([x, y]) => CharmEditor.current.clientOf(x, y), [(b[0] + b[2]) / 2, (b[1] + b[3]) / 2]); await page.mouse.dblclick(c.x, c.y); await settleP(page); }
      const ta = await page.evaluate(() => { const t = document.querySelector('.ce-root textarea.ce-editor'); return t ? t.value : null; });
      await page.keyboard.press('Control+a'); await page.keyboard.type('Mimi'); await page.keyboard.press('Enter'); await settleP(page);
      const r1 = await page.evaluate(id => ({ wd: __cw.words('back'), d: __cw.drawn('back', id) }), wid);
      check(`B9 a double-click opens the words box on the words ("${ta}") and they are retyped: "Mimi", in their font and style`, ta === o1.wd.text && r1.wd.text === 'Mimi' && r1.wd.fontKey === 'crimson-text' && r1.wd.letterCase === 'small' && !!r1.d, JSON.stringify({ ta, wd: r1.wd }));
      await R.shot(w, 'CW-3-retyped');
      // (back to the size the card fitted, so "Mimi" stands on the metal)
      { const el = page.locator('.ce-root [data-f="pt"]'); await el.click(); await page.keyboard.press('Control+a'); await page.keyboard.type(String(+j0.size.toFixed(2))); await page.keyboard.press('Enter'); await settleP(page); }
      /* ── Save: the words as the editor drew them are the back's engraving; the card's own words come off; Approve writes the back ── */
      const PID = made.back.poolIds ? made.back.poolIds[0] : await page.evaluate(k => Engrave.items().get(k).copies[0], j0.key);
      const puts0 = artPuts();
      await page.click('.ce-root [data-act="save"]');
      await until(async () => artPuts() > puts0 && !(await page.evaluate(() => !!document.querySelector('.ce-root'))), 60000);
      await until(() => page.evaluate(k => { const j = Engrave.items().get(k); return j && j.state === 'review' && j.fit && j.fit.artOnly; }, j0.key), 60000);
      await R.openEngraveDetail(w, 'back', { face: 'back' }); await R.settle(page, 900);
      const sv = await page.evaluate(([k, pid]) => {
        const j = Engrave.items().get(k), ch = Pool.charmOf(pid), line = (j.row.parentRow || j.row), ov = line.engravingOverride || {};
        return { state: j.state, artOnly: !!(j.fit && j.fit.artOnly), lines: j.lines, text: j.text, art: (ch.artBack || []).filter(m => m.artText === 'Mimi').length, ov: ov.back === undefined ? 'none' : ov.back, reason: ov.reason && ov.reason.text, note: (document.querySelector('#egQueue .rvItem[data-kind=placement] .egArtNote') || {}).textContent || '' };
      }, [j0.key, PID]);
      const rec = st.doc('Charm_Pool_Art', PID);
      check(`B10 Save: the record keeps the words as the editor drew them ("Mimi" with its outlines, hatched); the back is engraved by them and the card's own words come off it (the line says so: no back words, "${sv.reason}"); the card says "${sv.note}"`,
        !!rec && rec.added.some(a => a.text === 'Mimi' && a.role === 'hatch' && /^M/.test(a.d)) && sv.art >= 1 && sv.artOnly && sv.state === 'review' && !(sv.lines || []).length && !sv.text && sv.ov === null && /words changed in the Charm Editor/i.test(sv.reason || '') && /The back words are engraved as the Charm Editor drew them/.test(sv.note),
        JSON.stringify({ rec: rec && rec.added.map(a => [a.id, a.text, a.role]), sv }));
      const backs0 = st.calls.filter(x => x.op === 'saveSheetBack' || x.op === 'backPut').length;
      const p = await page.evaluate(Q => { const b = document.querySelector(Q + ' [data-a="approve"]'); if (!b) return 'none'; if (b.disabled) return 'dimmed'; b.click(); return 'pressed'; }, Q);
      const ap = await until(() => page.evaluate(k => { const j = Engrave.items().get(k); return j && /approved|written/.test(j.state) ? { state: j.state, at: j.approvedAt } : null; }, j0.key), 60000);
      const wr = await until(() => { const d = st.doc('Charm_Pool_Back', PID); return d && ap && +d.approvedAt === ap.at ? d : null; }, 60000);
      check('B11 Approve approves the back as the editor drew it and writes its file (its record, with the words as artwork)', p === 'pressed' && !!ap && !!wr, JSON.stringify({ p, ap, wr: wr && { approvedAt: wr.approvedAt, art: !!wr.artBack, backs0 } }));
    }

    if (!ONLY || ONLY === 'C') {
      /* ── C · words handed in and not changed are no change ── */
      await R.openEngraveDetail(w, 'both', { face: 'front' }); await R.settle(page, 700);
      const jf = await page.evaluate(() => { const j = __cw.job(); return { key: j.key, lines: (j.lines || []).slice(), pid: j.copies[0] }; });
      await page.click(Q + ' .egCharmEdit');
      await page.waitForSelector('.ce-root .ce-items path', { timeout: 60000 }); await settleP(page);
      const c1 = await page.evaluate(() => { const f = __cw.words('front'), b = __cw.words('back'); return { front: f && f.text, back: b && b.text, dirty: CharmEditor.current.dirty() }; });
      check(`C1 a front + back order: each face's words on its own face ("${c1.front}" on the front, "${c1.back}" on the back), and nothing reads as a change`, c1.front === 'W' && c1.back === 'NAME\nONE' || (c1.back || '').replace(/\n/g, ' ') === 'NAME ONE' && c1.front === 'W' && !c1.dirty, JSON.stringify(c1));
      await page.evaluate(() => { const E = CharmEditor.current, f = E.frame(), x = f.x0 + f.wPt * 0.5, y = f.y0 + f.hPt * 0.62, s = 1.2; E.ctx.addItems([{ kind: 'path', role: 'line', name: 'Dot', stroke: true, fill: false, lwPt: 0.25, subpaths: [[['m', [x - s, y - s]], ['l', [x + s, y - s]], ['l', [x + s, y + s]], ['l', [x - s, y + s]], ['h']]], tx: 0, ty: 0, sx: 1, sy: 1, rot: 0 }]); E.ctx.commit('dot'); });
      const puts1 = artPuts();
      await page.click('.ce-root [data-act="save"]');
      await until(async () => artPuts() > puts1 && !(await page.evaluate(() => !!document.querySelector('.ce-root'))), 60000);
      await until(() => page.evaluate(k => { const j = Engrave.items().get(k); return j && j.state === 'review' && j.fit; }, jf.key), 60000);
      const recC = st.doc('Charm_Pool_Art', jf.pid + '~front');
      const c2 = await page.evaluate(k => { const j = Engrave.items().get(k), b = Engrave.jobsOfPiece(j.copies[0]).back; return { front: j.lines, artOnly: !!(j.fit && j.fit.artOnly), back: b && b.lines }; }, jf.key);
      check('C2 Save keeps the part added and nothing of the words (they were not changed): the card keeps its words on the front (fitted again around the new drawing) and on the back',
        !!recC && recC.added.length === 1 && !recC.added.some(a => a.text) && c2.front.join(' ') === jf.lines.join(' ') && !c2.artOnly && (c2.back || []).length > 0, JSON.stringify({ rec: recC && recC.added.map(a => [a.id, a.text]), c2 }));
    }

    if (!ONLY || ONLY === 'D') {
      /* ── D · Approve after the editor (Paul, order 4203083230) ── */
      const addImage = () => page.evaluate(() => {
        const E = CharmEditor.current, f = E.frame(), cx = f.x0 + f.wPt * 0.5, cy = f.y0 + f.hPt * 0.3, r = Math.min(f.wPt, f.hPt) * 0.07;
        const sq = (x, y, s) => [[['m', [x - s, y - s]], ['l', [x + s, y - s]], ['l', [x + s, y + s]], ['l', [x - s, y + s]], ['h']]];
        E.ctx.addItems([{ kind: 'group', role: 'hatch', name: 'Image · photo', children: [
          { kind: 'path', role: 'hatch', name: 'Grey 1', fill: [0.3, 0.3, 0.3], stroke: false, artGrey: true, even: true, evenOdd: true, subpaths: sq(cx, cy, r), tx: 0, ty: 0, sx: 1, sy: 1, rot: 0 },
          { kind: 'path', role: 'hatch', name: 'Grey 2', fill: [0.6, 0.6, 0.6], stroke: false, artGrey: true, even: true, evenOdd: true, subpaths: sq(cx + r * 0.4, cy + r * 0.4, r * 0.4), tx: 0, ty: 0, sx: 1, sy: 1, rot: 0 }] }], { select: false });
        E.ctx.commit('image');
      });
      const approveNow = () => page.evaluate(Q => { const b = document.querySelector(Q + ' [data-a="approve"]'); if (!b) return 'none'; if (b.disabled) return 'dimmed'; b.click(); return 'pressed'; }, Q);
      await R.openEngraveDetail(w, 'front', { face: 'front' }); await R.settle(page, 700);
      const jd = await page.evaluate(() => { const j = __cw.job(); return { key: j.key, pid: j.copies[0], lines: j.lines.slice(), state: j.state }; });
      // 1 · Approve pressed while the editor's save is still going (the cloud slow): it waits, says so at once, and approves what the save left (before, it approved the placement as it
      //     was and the save landing a moment later took that approval back: "the system did nothing")
      await page.click(Q + ' .egCharmEdit');
      await page.waitForSelector('.ce-root .ce-items path', { timeout: 60000 }); await settleP(page);
      await page.evaluate(() => { const E = CharmEditor.current, f = E.frame(), x = f.x0 + f.wPt * 0.5, y = f.y0 + f.hPt * 0.2, s = 1; E.ctx.addItems([{ kind: 'path', role: 'hatch', name: 'Star', fill: true, stroke: false, subpaths: [[['m', [x, y - s]], ['l', [x + s, y + s]], ['l', [x - s, y + s]], ['h']]], tx: 0, ty: 0, sx: 1, sy: 1, rot: 0 }], { select: false }); E.ctx.commit('star'); });
      await page.evaluate(() => { const AE = window.CharmNestArtEdit, real = AE.save; window.__slowSave = { real }; AE.save = (...a) => new Promise(r => setTimeout(r, 2500)).then(() => real.apply(AE, a)); });
      const puts3 = artPuts();
      await page.click('.ce-root [data-act="save"]');
      await page.waitForFunction(() => { const n = document.querySelector('#egQueue .rvItem[data-kind=placement] .egArtNote'); return !document.querySelector('.ce-root') && n && /^Saving/.test(n.textContent); }, null, { timeout: 10000 }).catch(() => {});
      const tP = Date.now(), pE = await approveNow();
      const said = await page.waitForFunction(() => { const n = document.querySelector('#egQueue .rvItem[data-kind=placement] .egApproveWhy'); return n ? n.textContent : false; }, null, { timeout: 2000 }).then(h => h.jsonValue()).catch(() => '');
      const sayMs = Date.now() - tP, putsAtSay = artPuts();
      const apE = await until(() => page.evaluate(k => { const j = Engrave.items().get(k); return j && /approved|written/.test(j.state) && j.approvedAt ? { state: j.state, at: j.approvedAt } : null; }, jd.key), 90000);
      await page.evaluate(() => { CharmNestArtEdit.save = __slowSave.real; });
      const recE = st.doc('Charm_Pool_Art', jd.pid + '~front');
      await R.settle(page, 1500);
      const endE = await page.evaluate(k => { const j = Engrave.items().get(k); return { state: j.state, at: j.approvedAt, reason: j.reason }; }, jd.key);
      const fileE = await until(() => { const d = st.doc('Charm_Pool_Back', jd.pid + '~front'); return d && apE && +d.approvedAt === apE.at && !d.invalidated ? d : null; }, 60000);
      check(`D1 a part added in the editor, Approve pressed while its save is still going (2.5 s): the card says "${said}" (${sayMs} ms), and the approval follows the save: approved after the art was kept, and it stays approved (its file written)`,
        jd.state === 'review' && pE === 'pressed' && /^Saving this piece's artwork from the Charm Editor first/.test(said) && sayMs < 1500 && putsAtSay === puts3 && !!recE && !!apE && +apE.at >= +recE.at && /approved|written/.test(endE.state) && endE.at === apE.at && !!fileE,
        JSON.stringify({ jd, pE, said, sayMs, putsAtSay, puts3, apE, recAt: recE && recE.at, endE, file: !!fileE }));
      // 2 · Paul's order: an image (grey fills, as the image importer makes them) added to the approved front, saved, back on the card: ONE Approve press approves it all again
      await R.openEngraveDetail(w, 'front', { face: 'front' }); await R.settle(page, 700);
      await page.click(Q + ' .egCharmEdit');
      await page.waitForSelector('.ce-root .ce-items path', { timeout: 60000 }); await settleP(page);
      await addImage();
      const puts2 = artPuts();
      await page.click('.ce-root [data-act="save"]');
      await until(async () => artPuts() > puts2 && !(await page.evaluate(() => !!document.querySelector('.ce-root'))), 60000);
      await until(() => page.evaluate(k => { const j = Engrave.items().get(k); return j && j.state === 'review' && j.fit && j.view; }, jd.key), 60000);
      await R.openEngraveDetail(w, 'front', { face: 'front' }); await R.settle(page, 900);
      const d0 = await page.evaluate(() => ({ note: (document.querySelector('#egQueue .rvItem[data-kind=placement] .egArtNote') || {}).textContent || '', grey: (Pool.charmOf(__cw.job().copies[0]).members || []).filter(m => m.artGrey).length, state: __cw.job().state }));
      await R.shot(w, 'CW-4-image-saved-card');
      const pD = await approveNow();
      const apD = await until(() => page.evaluate(k => { const j = Engrave.items().get(k); return j && /approved|written/.test(j.state) ? { state: j.state, at: j.approvedAt, lines: j.lines } : null; }, jd.key), 60000);
      const wrD = await until(() => { const d = st.doc('Charm_Pool_Back', jd.pid + '~front'); return d && apD && +d.approvedAt === apD.at && !d.invalidated ? d : null; }, 60000);
      const sealD = [...st.docs.entries()].filter(([k, v]) => /Order_Timeline/.test(k) && String(v.orderId) === made.front.rid && v.type === 'engraveApproved').length;
      check(`D2 an image added in the editor to the approved front and saved ("${d0.note}", ${d0.grey} grey parts on the piece), back on the card: ONE Approve press approves the front again with its words and writes its file, with its seal`,
        d0.grey === 2 && /^Front artwork saved/.test(d0.note) && d0.state === 'review' && pD === 'pressed' && !!apD && !!apE && apD.at !== apE.at && apD.lines.join(' ') === jd.lines.join(' ') && !!wrD && sealD >= 2, JSON.stringify({ d0, pD, apD, wrD: !!wrD, sealD }));
    }
    check('E1 no page error', w.errors.length === 0, JSON.stringify(w.errors).slice(0, 600));
    console.log('screenshots in ' + shots);
  } finally { await w.close(); }
}

(async () => {
  if (!ONLY || ONLY === 'A') partA();
  if (ONLY !== 'A') await partBCD();
  const bad = RESULTS.filter(r => !r.ok);
  console.log(`\n${RESULTS.length - bad.length}/${RESULTS.length} checks pass`);
  process.exit(bad.length ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
