// ALPHAFIND (Paul, 28 Oct 2026: "I also don't see the option to choose a alphabet/language letters/characters. These alternate alphabets needs to be searchable for equivalent english letters
// and words as in some languages a single sign can have several meanings and entire words or phrases depending on context. I don't want this solution to turn into a massive translation system.")
//   A  (node) charm-nest-alphabet-data.js + the picker's search: every alphabet's letters with the Latin they stand for, the words in 17 languages with every meaning; "love" finds love in every
//      language, "a" finds the letters that are an a, "alpha" finds α, "shalom" (a spelling) and אהבה (the letters) are found, a sign with several meanings says them all (心: heart, mind ...),
//      a word that changes with who says it says so (Hebrew "I love you"), a phrase whose little words are missing still finds something ("forever and always")
//   B  the real page (_engraveRealWorld.cjs, offline): the Engraving card's words box has "Aα" beside Emoji; it opens the finder; "love" + Hebrew → a press puts אהבה in the words at the caret, as
//      typing does (the box's input event), and the finder stays open
//   C  the Charm Editor: the Font menu has "Other alphabets"; with the words selected it opens the words box and the finder on it; "omega" in Letters → Ω goes into the words, and the editor's
//      text item reads it; the words box has its own "Aα" beside Emoji
//   PW_DIR=<playwright node_modules> CHROMIUM=<chrome> node tests/charm-nest/alphabet-finder.cjs
'use strict';
const path = require('node:path');
const ROOT = path.join(__dirname, '../..');
const RESULTS = [];
const check = (id, ok, evidence) => { RESULTS.push({ id, ok: !!ok }); console.log(`${ok ? 'PASS' : 'FAIL'}  ${id}  ${ok ? '' : evidence || ''}`.trimEnd()); };
const Q = '#egQueue .rvItem[data-kind=placement]';

function partA() {
  global.window = global;
  const D = require(path.join(ROOT, 'charm-nest-alphabet-data.js'));
  require(path.join(ROOT, 'charm-nest-alphabet-picker.js'));
  const P = global.CNAlphabetPicker, e = D.entries(), q = (s, o) => P.query(s, o);
  const scripts = new Set(e.filter(x => x.kind === 'letter').map(x => x.script)), langs = new Set(e.filter(x => x.kind === 'word').map(x => x.lang));
  check(`A1 the letters of ${[...scripts].join(', ')} and words in ${langs.size} languages, every one with its text and (a word) how it is said`,
    ['Greek', 'Cyrillic', 'Hebrew', 'Arabic', 'Devanagari', 'Japanese', 'Korean'].every(s => scripts.has(s)) && langs.size >= 15 && e.every(x => x.text && x.text.trim() && (x.kind === 'letter' || x.roman)), JSON.stringify([...scripts]));
  const love = q('love');
  check(`A2 "love" finds love in every language first (${love.slice(0, 4).map(x => x.text).join(', ')} …)`, love.length >= 15 && love.slice(0, 15).every(x => x.en === 'love'), JSON.stringify(love.slice(0, 15).map(x => x.en)));
  const a = q('a', { kind: 'letters' }), alpha = q('alpha', { kind: 'all' });
  check(`A3 "a" in Letters finds the letters that are an a (${a.slice(0, 5).map(x => x.text).join(' ')}), "alpha" finds α`, a.slice(0, 5).every(x => /\ba\b/.test(x.roman)) && alpha[0] && alpha[0].text === 'Α α', JSON.stringify({ a: a.slice(0, 5).map(x => x.text), alpha: alpha.slice(0, 2).map(x => x.text) }));
  const sh = q('shalom'), he = q('אהבה'), xin = q('heart', { lang: 'zh' });
  check('A4 a Latin spelling (shalom) and the letters themselves (אהבה) are found; a sign says all its meanings (心: heart, mind ...); "shalom" says it is also hello and goodbye',
    sh[0] && sh[0].text === 'שלום' && /hello and goodbye/.test(sh.map(x => x.note).join(' ')) && he[0] && he[0].text === 'אהבה' && xin[0] && xin[0].text === '心' && /mind/.test(xin[0].note), JSON.stringify({ sh: sh.map(x => x.text), he: he.map(x => x.text), xin: xin.slice(0, 1) }));
  const ily = q('I love you', { lang: 'he' }), fa = q('forever and always', { lang: 'it' });
  check('A5 a word that changes with who says it says so (Hebrew "I love you"); a phrase whose little words are missing still finds what it has ("forever and always" → per sempre)',
    ily[0] && /said by a man; a woman says/.test(ily[0].note) && fa.length >= 1 && fa[0].text === 'per sempre', JSON.stringify({ ily: ily.slice(0, 1), fa: fa.map(x => x.text) }));
  delete global.CNAlphabetPicker; delete global.CNAlphabetData;
}

const settleP = page => page.evaluate(() => new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r))));
async function partBC() {
  const R = require('./_engraveRealWorld.cjs');
  const w = await R.world({ viewport: { width: 1440, height: 900 } });
  if (!w) { console.log('SKIP no playwright-core'); return; }
  const page = w.page;
  await page.waitForFunction(() => !window.CNRole || ['ask', 'role', 'admin'].includes(CNRole.state()), null, { timeout: 20000 }).catch(() => {});
  await page.evaluate(() => { try { if (window.CNRole && CNRole.state() === 'ask') CNRole.choose('design'); } catch (_) {} });
  try {
    await R.build(w, ['back']);
    await R.openEngraveDetail(w, 'back', { face: 'back' }); await R.settle(page, 700);
    /* ── B · the Engraving card's words box ── */
    const ab = Q + ' [data-alpha]';
    const b0 = await page.evaluate(sel => { const b = document.querySelector(sel), e = b && b.parentElement.querySelector('[data-emoji]'); return { has: !!b, text: b && b.textContent, besideEmoji: !!e, aria: b && b.getAttribute('aria-haspopup') }; }, ab);
    check('B1 the Engraving card\'s words box has "Aα" beside Emoji, ready to open the finder', b0.has && b0.text === 'Aα' && b0.besideEmoji && b0.aria === 'dialog', JSON.stringify(b0));
    const ta = await page.evaluate(sel => { const t = document.querySelector(sel).closest('.engpBox').querySelector('textarea'); t.focus(); t.setSelectionRange(t.value.length, t.value.length); return t.value; }, ab);
    await page.evaluate(() => { window.__inputs = 0; document.addEventListener('input', e => { if (e.target && e.target.classList && e.target.classList.contains('engpWords')) window.__inputs++; }, true); });
    await page.click(ab);
    await page.waitForSelector('.cnAp:not([hidden])', { timeout: 8000 });
    await page.fill('.cnAp .cnAp__q', 'love');
    await page.click('.cnAp [data-k="words"]');
    await page.selectOption('.cnAp .cnAp__lang', 'he');
    await settleP(page);
    const row = await page.evaluate(() => { const r = document.querySelector('.cnAp .cnAp__row'); return r && { t: r.querySelector('.cnAp__t').textContent, m: r.querySelector('.cnAp__m').textContent }; });
    await page.click('.cnAp .cnAp__row');
    await settleP(page);
    const after = await page.evaluate(sel => ({ v: document.querySelector(sel).closest('.engpBox').querySelector('textarea').value, inputs: window.__inputs, open: !!document.querySelector('.cnAp:not([hidden])') }), ab);
    check(`B2 "love" in Hebrew: ${row && row.t} (${row && row.m}); a press puts it in the words at the caret as typing does, and the finder stays open`,
      row && row.t === 'אהבה' && after.v === ta + 'אהבה' && after.inputs >= 1 && after.open, JSON.stringify({ row, ta, after }));
    await page.keyboard.press('Escape'); await settleP(page);
    const shut = await page.evaluate(() => !document.querySelector('.cnAp:not([hidden])'));
    check('B3 Esc closes the finder', shut, '');

    /* ── C · the Charm Editor ── */
    await page.click(Q + ' .egCharmEdit');
    await page.waitForSelector('.ce-root .ce-items path', { timeout: 60000 }); await settleP(page); await settleP(page);
    const words = () => page.evaluate(() => { const E = CharmEditor.current; let hit = null; const walk = l => l.forEach(i => { if (i.kind === 'text') hit = hit || i; if (i.children) walk(i.children); }); walk(E.items('back')); walk(E.items('front')); return hit && { id: hit.id, text: hit.text }; });
    const w0 = await words();
    { const b = await page.evaluate(id => CharmEditor.current.box(id), w0.id); const c = await page.evaluate(([x, y]) => CharmEditor.current.clientOf(x, y), [(b[0] + b[2]) / 2, (b[1] + b[3]) / 2]); await page.mouse.click(c.x, c.y); await settleP(page); }
    await page.click('.ce-root [data-drop="font"] .ce-drop__b'); await settleP(page);
    const menu = await page.evaluate(() => { const m = document.querySelector('.ce-root [data-drop="font"] .ce-menu'); return { heads: [...m.querySelectorAll('.ce-menu__head')].map(h => h.firstChild.textContent.trim()), find: !!m.querySelector('[data-alphafind]') }; });
    check(`C1 the Font menu: ${menu.heads.join(' · ')}, with "Find letters, words & phrases…" under Other alphabets`, menu.find && menu.heads.includes('Other alphabets') && menu.heads.includes('Letter case'), JSON.stringify(menu));
    await page.click('.ce-root [data-alphafind]');
    await page.waitForSelector('.cnAp:not([hidden])', { timeout: 8000 });
    const opened = await page.evaluate(() => ({ editing: CharmEditor.current.editing(), btn: !!document.querySelector('.ce-root .ce-tx-alpha'), emoji: !!document.querySelector('.ce-root .ce-tx-emoji:not(.ce-tx-alpha)') }));
    await page.fill('.cnAp .cnAp__q', 'omega');
    await page.click('.cnAp [data-k="letters"]');
    await page.selectOption('.cnAp .cnAp__lang', '');
    await settleP(page);
    const first = await page.evaluate(() => (document.querySelector('.cnAp .cnAp__row .cnAp__t') || {}).textContent);
    await page.click('.cnAp .cnAp__row'); await settleP(page); await settleP(page);
    const w1 = await words();
    check(`C2 Other alphabets opens the words box (editing ${opened.editing}, with its own Aα beside Emoji) and the finder on it; "omega" in Letters → ${first}: Ω goes into the words (${w0.text} → ${w1.text})`,
      opened.editing === w0.id && opened.btn && opened.emoji && first === 'Ω ω' && w1.text.includes('Ω') && w1.text.length === w0.text.length + 1, JSON.stringify({ opened, first, w0, w1 }));
    if (process.env.SHOTS) await page.screenshot({ path: path.join(process.env.SHOTS, 'alphabet-finder-editor.png') });
    check('D1 no page error', w.errors.length === 0, JSON.stringify(w.errors).slice(0, 600));
  } finally { await w.close(); }
}

(async () => {
  partA();
  await partBC();
  const bad = RESULTS.filter(r => !r.ok);
  console.log(`\n${RESULTS.length - bad.length}/${RESULTS.length} checks pass`);
  process.exit(bad.length ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
