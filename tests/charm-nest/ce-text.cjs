// CE TEXT (Paul, 28 Oct 2026: "Users also need to be able to add any emoji and any language that we already support as part of the back engraving."; D9: emoji engraved as
// monochrome OUTLINES). The Charm Editor's Text tool sets words exactly as the back engraving does (charm-nest-ce-text.js over Engrave's fonts: the engraving font, then the
// script font of a letter it lacks, shaped by HarfBuzz, right to left where the words are, then the Noto Emoji outlines), and its words box has the engraving's own Emoji picker.
// The real sorter page over the repo's fake site (_engraveRealWorld.cjs: the real intake, pool, classifier and fit; offline, made-up names), a back-only order of the calendar charm
// (_artCalendar.cjs, a per-SKU master, 60 x 70 pt; font Stylish):
//   1  "Edit charm" opens the editor with every engraving font: the piece's own first (Stylish), the shop's words, and the other installed fonts
//   2  Text tool, a click on the charm, "Love 💕 " typed; the words box's Emoji button opens the picker OVER the editor without closing the box; "moon" searched, Enter puts the
//      first match in at the caret; " שלום" typed: the Hebrew font is fetched on demand and the words drawn again; the line under the box says "Set in Noto Sans Hebrew"
//   3  the outlines are the sum of their parts (the Latin letters, both emoji, the Hebrew word: each one's own outline count), placed where the words were put (the start of the
//      baseline at the click), within the charm; a Hebrew-only text grows LEFT from its click (its start is its right end); textToPaths' main-thread time per call (ms)
//   4  Save: the Charm_Pool_Art record of the piece's back keeps the words as blue-hatched outlines (who, the words, every outline where the editor drew it)
//   SLOT: $SP/int/slot.sh node tests/charm-nest/ce-text.cjs        (SHOTS=<dir> for screenshots)
'use strict';
const path = require('node:path');
const R = require('./_engraveRealWorld.cjs');
const ROOT = path.join(__dirname, '../..');
const RESULTS = [];
const check = (id, ok, evidence) => { RESULTS.push({ id, ok: !!ok }); console.log(`${ok ? 'PASS' : 'FAIL'}  ${id}  ${ok ? '' : evidence || ''}`.trimEnd()); };
const PT = 72 / 25.4;
const settleP = page => page.evaluate(() => new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r))));
const C = (page, fn, arg) => page.evaluate(fn, arg);

/** the calendar charm, drawn here as the shop draws its masters (node: pdf-lib and the PDF writer; the globals are put back so playwright does not think it runs in a page) */
async function calendar() {
  const hadWindow = 'window' in global, hadSelf = 'self' in global;
  global.window = global; global.self = global;
  global.PDFLib = global.PDFLib || require(path.join(ROOT, 'vendor/pdf-lib-1.17.1.min.js'));
  require(path.join(ROOT, 'charm-nest-pdf.js'));
  global.CharmNestGeom = global.CharmNestGeom || require(path.join(ROOT, 'charm-nest-geom.js'));
  const cal = await require('./_artCalendar.cjs')(ROOT);
  if (!hadWindow) delete global.window; if (!hadSelf) delete global.self;
  return cal;
}

(async () => {
  const cal = await calendar(), c = cal.charm;
  R.DESIGNS.CALENDAR = { file: 'CALENDAR.ai', make: () => Buffer.from(cal.bytes), entry: { sku: 'CALENDAR', widthPt: +(c.bbox[2] - c.bbox[0]).toFixed(3), heightPt: +(c.bbox[3] - c.bbox[1]).toFixed(3), areaPt2: 4200, members: c.members.length, holes: 0, engravable: true, engravableBy: 'index', upAngle: 90, upSource: 'drawn', charmHash: 'calhash01', backKeepOut: [] } };
  R.ORDERS.calText = Object.assign({}, R.ORDERS.back, { rid: '4290000141', sku: 'CALENDAR', listing: '1890000141', title: 'Calendar Necklace, Personalized Month Calendar Charm', buyer: 'Name Thirteen', personalization: ['Name Two'] });
  R.EXPECT_JOBS.calText = 1;
  const w = await R.world({ shots: process.env.SHOTS || path.join(require('node:os').tmpdir(), 'ce-text-shots') });
  if (!w) { console.log('SKIP no playwright-core'); return; }
  const page = w.page, st = w.srv.st, Q = '#egQueue .rvItem[data-kind=placement]', errors = [], gets = [];
  page.on('pageerror', e => errors.push(e.message));
  page.on('response', r => { const u = r.url(); if (/\/vendor\/fonts\/scripts\/|script-fonts\.json|hb\.wasm/.test(u)) gets.push(u.replace(/^.*?\/vendor\//, 'vendor/').replace(/\?.*/, '') + ' ' + r.status()); });
  try {
    const made = await R.build(w, ['calText']);
    const PID = made.calText.poolIds[0];
    check('0 the world: the calendar is pooled and its back fitted', made.calText.jobs.length === 1 && made.calText.jobs[0].fitted, JSON.stringify(made.calText.jobs));
    await R.openEngraveDetail(w, 'calText', { face: 'back' });
    await page.click(Q + ' .backHost .egCharmEdit');
    await page.waitForSelector('.ce-root .ce-items path', { timeout: 60000 }); await settleP(page);

    /* 1 · the fonts */
    const f1 = await page.evaluate(() => ({ face: CharmEditor.current.face(), fonts: [...document.querySelectorAll('.ce-root .ce-item[data-font]')].map(b => [b.dataset.font, b.textContent]), all: CharmNestOrders.ENGRAVING_FONTS.map(f => f.id) }));
    check(`1 the editor offers every engraving font (${f1.fonts.length}): the piece's own first (Stylish), the shop's words, and the installed fonts a saved back may name (Jost, Alegreya Sans, Playwrite US Modern)`,
      f1.face === 'back' && f1.fonts[0][1] === 'Stylish' && f1.all.every(id => f1.fonts.some(f => f[0] === id)) && ['Ace', 'Pristina', 'Typewriter', 'Britannic Bold', 'Jost', 'Alegreya Sans', 'Playwrite US Modern'].every(n => f1.fonts.some(f => f[1] === n)),
      JSON.stringify(f1));

    /* 2 · the words: typed, an emoji picked over the editor, a Hebrew word fetched on demand */
    const fr = await page.evaluate(() => CharmEditor.current.frame());
    const at = [fr.x0 + fr.wPt * 0.1, fr.y0 + fr.hPt * 0.8];   // (the start of the baseline: low on the charm, at its left, under its words)
    await page.keyboard.press('t');
    { const el = page.locator('.ce-root [data-f="pt"]'); await el.click(); await page.keyboard.press('Control+a'); await page.keyboard.type('4'); await page.keyboard.press('Enter'); await settleP(page); }
    const pa = await C(page, p => CharmEditor.current.clientOf(p[0], p[1]), at);
    await page.mouse.click(pa.x, pa.y); await settleP(page);
    await page.keyboard.type('Love 💕 '); await settleP(page);
    const b2 = await page.evaluate(() => { const b = document.querySelector('.ce-root .ce-tx-emoji'), ta = document.querySelector('.ce-root textarea.ce-editor'); const r = b && b.getBoundingClientRect(), t = ta && ta.getBoundingClientRect(); return { btn: !!b, inBox: !!(r && t && r.left >= t.left && r.right <= t.right + 1 && r.top >= t.top - 1 && r.bottom <= t.bottom + 1), dir: ta && ta.dir }; });
    await page.click('.ce-root .ce-tx-emoji'); await settleP(page);
    await page.waitForFunction(() => window.CNEmojiPicker.state().open && window.CNEmojiPicker.state().font === 'ready', null, { timeout: 30000 });
    const pk = await page.evaluate(() => { const pop = document.querySelector('.emPop'), r = pop.getBoundingClientRect(), hit = document.elementFromPoint(r.left + r.width / 2, r.top + r.height / 2); return { open: !pop.hidden, over: !!(hit && pop.contains(hit)), z: getComputedStyle(pop).zIndex, editing: !!CharmEditor.current.editing(), focus: !!(document.activeElement && pop.contains(document.activeElement)) }; });
    await page.keyboard.type('moon'); await settleP(page);
    const found = await page.evaluate(() => window.CNEmojiPicker.state().cells);
    await R.shot(w, 'CET-1-picker-over-editor');
    await page.keyboard.press('Enter'); await settleP(page);
    await page.keyboard.press('Escape'); await settleP(page);
    const afterPick = await page.evaluate(() => ({ open: window.CNEmojiPicker.state().open, editing: !!CharmEditor.current.editing(), value: document.querySelector('.ce-root textarea.ce-editor').value, focus: document.activeElement && document.activeElement.className }));
    const picked = [...new Intl.Segmenter(undefined, { granularity: 'grapheme' }).segment(afterPick.value.slice('Love 💕 '.length))].map(s => s.segment)[0] || '';
    check(`2a the words box has its Emoji button inside it (dir=${b2.dir}); the picker opens OVER the editor (z ${pk.z}), the box stays open; "moon" finds ${found}, Enter puts "${picked}" in at the caret, Esc goes back to the words`,
      b2.btn && b2.inBox && b2.dir === 'auto' && pk.open && pk.over && pk.editing && pk.focus && found > 0 && /\p{Extended_Pictographic}/u.test(picked) && !afterPick.open && afterPick.editing && /ce-editor/.test(afterPick.focus || ''),
      JSON.stringify({ b2, pk, found, afterPick }));
    await page.keyboard.type(' שלום'); await settleP(page);
    await page.waitForFunction(() => window.CharmNestText.sharedScripts.isLoaded('hebrew') && !window.CharmNestCeText.state().inflight.length, null, { timeout: 60000 });
    await settleP(page);
    const words = 'Love 💕 ' + picked + ' שלום';
    const n2 = await page.evaluate(() => ({ note: (document.querySelector('.ce-root .ce-tx-note') || {}).textContent || '', id: CharmEditor.current.editing() }));
    await R.shot(w, 'CET-2-words-typed');
    check(`2b " שלום" typed: the Hebrew font fetched on demand (${gets.filter(g => /scripts\//.test(g)).join(', ')}) and the words drawn again; the line under the box says "${n2.note}"`,
      gets.some(g => /NotoSansHebrew-Regular\.ttf 200/.test(g)) && /Set in Noto Sans Hebrew/.test(n2.note) && !/Not engraved/.test(n2.note), JSON.stringify({ gets, n2 }));
    await page.keyboard.press('Enter'); await settleP(page);

    /* 3 · the outlines */
    const o3 = await page.evaluate(([id, words, at]) => {
      const E = CharmEditor.current, it = (function find(l) { for (const i of l) { if (i.id === id) return i; if (i.children) { const h = find(i.children); if (h) return h; } } return null; })(E.items());
      const out = E.result().faces.back.items.find(i => i.id === id), M = CharmEditor.model, T = CharmNestCeText;
      const n = s => T.textToPaths(s, it.fontKey, it.sizePt, {}).subpaths.length;
      const seg = [...new Intl.Segmenter(undefined, { granularity: 'grapheme' }).segment(words)].map(s => s.segment).filter(s => /\p{Extended_Pictographic}/u.test(s));
      const parts = { Love: n('Love'), emoji: seg.map(n), hebrew: n('שלום') }, b = M.bboxOf(out.subpaths), fr = E.frame();
      // main-thread time of the engine per call (each size new, so nothing comes from a cache)
      const t0 = performance.now(); for (let i = 0; i < 20; i++) T.textToPaths(words, it.fontKey, it.sizePt + 0.01 * (i + 1), {}); const ms = (performance.now() - t0) / 20;
      const drawn = ((document.querySelector(`.ce-items path[data-id="${CSS.escape(id)}"]`) || { getAttribute: () => '' }).getAttribute('d').match(/M/g) || []).length;   // (what the work area draws)
      return { text: it.text, font: it.fontKey, size: it.sizePt, subs: out.subpaths.length, drawn, parts, b, fr, inFrame: b[0] > fr.x0 && b[2] < fr.x0 + fr.wPt && b[1] > fr.y0 && b[3] < fr.y0 + fr.hPt, ms, emojiN: seg.length };
    }, [n2.id, words, at]);
    const sum = o3.parts.Love + o3.parts.emoji.reduce((a, b) => a + b, 0) + o3.parts.hebrew;
    check(`3a the words "${o3.text}" (${o3.font}, ${o3.size} pt) are ${o3.subs} filled outlines, the sum of their parts (Love ${o3.parts.Love} + emoji ${o3.parts.emoji.join(' + ')} + Hebrew ${o3.parts.hebrew}), drawn so on the charm (${o3.drawn}), inside it`,
      o3.text === words && o3.emojiN === 2 && o3.parts.emoji.every(k => k > 0) && o3.parts.hebrew > 0 && o3.subs === sum && o3.drawn === sum && o3.inFrame, JSON.stringify(o3));
    check(`3b placed where the words were put: the outlines start at the click (${((o3.b[0] - at[0]) / PT).toFixed(2)} mm), the baseline at its height (letters above it, ${((at[1] - o3.b[1]) / PT).toFixed(2)} mm tall)`,
      Math.abs(o3.b[0] - at[0]) < 0.6 * PT && o3.b[1] < at[1] && o3.b[3] > at[1] - 0.2 && o3.b[3] < at[1] + o3.size * 0.45, JSON.stringify({ b: o3.b, at }));
    check(`3c textToPaths takes ${o3.ms.toFixed(2)} ms of main-thread time per call for these words (Latin, two emoji, shaped Hebrew), under the 16 ms of a frame`, o3.ms < 16, String(o3.ms));
    // a Hebrew-only text: it reads right to left, so its start (the click) is its RIGHT end
    const at2 = [fr.x0 + fr.wPt * 0.85, fr.y0 + fr.hPt * 0.92];
    await page.keyboard.press('t');
    const pb = await C(page, p => CharmEditor.current.clientOf(p[0], p[1]), at2);
    await page.mouse.click(pb.x, pb.y); await settleP(page);
    await page.keyboard.type('אמא'); await settleP(page);
    const rtlId = await page.evaluate(() => CharmEditor.current.editing());
    await page.keyboard.press('Enter'); await settleP(page);
    const rb = await C(page, id => CharmEditor.current.box(id), rtlId);
    check(`3d a Hebrew-only text grows LEFT from where it was put: its right end at the click (${((rb[2] - at2[0]) / PT).toFixed(2)} mm), its left end ${((at2[0] - rb[0]) / PT).toFixed(2)} mm to the left`,
      !!rb && Math.abs(rb[2] - at2[0]) < 0.6 * PT && rb[0] < at2[0] - 1, JSON.stringify({ rb, at2 }));
    await R.shot(w, 'CET-3-outlines');

    /* 4 · Save into the art record */
    const want = await page.evaluate(id => { const it = CharmEditor.current.result().faces.back.items.find(i => i.id === id), j = Engrave.items().get(document.querySelector('#egQueue .rvItem[data-kind=placement]').dataset.key), fr = CharmNestArtEdit.frame(j.view); let b = null; for (const sp of it.subpaths) for (const o of sp) for (const p of o.slice(1)) { const q = fr.toSource(p); b = b ? [Math.min(b[0], q[0]), Math.min(b[1], q[1]), Math.max(b[2], q[0]), Math.max(b[3], q[1])] : [q[0], q[1], q[0], q[1]]; } return b; }, n2.id);
    await page.click('.ce-root [data-act="save"]');
    // (the record of a piece's back is keyed by its pool id, as Charm_Pool_Back's: charm-nest-art-edit.js recKey)
    for (let t0 = Date.now(); Date.now() - t0 < 60000 && !st.doc('Charm_Pool_Art', PID); ) await new Promise(r => setTimeout(r, 200));
    const rec = st.doc('Charm_Pool_Art', PID);
    const e4 = rec && (rec.added || []).find(a => a.id === n2.id);
    const got = e4 && await page.evaluate(d => { let b = null; for (const sp of CharmNestArtEdit.fromD(d)) for (const o of sp) for (const p of o.slice(1)) b = b ? [Math.min(b[0], p[0]), Math.min(b[1], p[1]), Math.max(b[2], p[0]), Math.max(b[3], p[1])] : [p[0], p[1], p[0], p[1]]; return { b, subs: CharmNestArtEdit.fromD(d).length }; }, e4.d);
    check(`4 Save: the piece's back record keeps the words as blue-hatched outlines (${got ? got.subs : 0} of them), by the signed-in person, every outline where the editor drew it (to ${got ? Math.max(...got.b.map((v, i) => Math.abs(v - want[i]))).toFixed(4) : '?'} pt)`,
      !!rec && rec.by === 'Test Operator' && !!e4 && e4.text === words && e4.role === 'hatch' && e4.fill && e4.fill.join() === '0,0,1' && got.subs === o3.subs && got.b.every((v, i) => Math.abs(v - want[i]) < 0.002) && rec.added.length === 2,
      JSON.stringify({ rec: rec && { by: rec.by, added: (rec.added || []).map(a => [a.id, a.text, a.role]) }, want, got, docs: st.list('Charm_Pool_Art').map(x => x._id), note: await page.evaluate(() => [...document.querySelectorAll('.egArtNote')].map(n => n.textContent).join(' | ')), open: await page.evaluate(() => !!CharmEditor.current) }));
    await R.shot(w, 'CET-4-saved');
    check('5 no page errors', errors.length === 0, errors.slice(0, 3).join(' | '));
  } finally { await w.close(); }
  const bad = RESULTS.filter(r => !r.ok);
  console.log(`\nce-text: ${RESULTS.length - bad.length}/${RESULTS.length} passed`);
  process.exit(bad.length ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
