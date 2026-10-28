/* charm-nest-alphabet-picker.js — FIND A LETTER, WORD OR PHRASE OF ANOTHER ALPHABET BY ITS ENGLISH (ALPHAFIND; Paul, 28 Oct 2026: "I also don't see the option to choose a alphabet/language
   letters/characters. These alternate alphabets needs to be searchable for equivalent english letters and words as in some languages a single sign can have several meanings and entire words
   or phrases depending on context. I don't want this solution to turn into a massive translation system. I just want away for users to easily find equivalent characters, words, and meanings
   and phrases in other languages.")

   ONE component for every words box, beside its Emoji button (charm-nest-emoji-picker.js, whose ways it follows): the Charm Editor's words box (charm-nest-ce-text.js), the Engraving card's
   words and the Review card that asks for the words (charm-nest-bridge.js) call
       CNAlphabetPicker.attach({ textarea, button, key })
   and the picker does the rest. What it offers is the fixed reference of charm-nest-alphabet-data.js (CNAlphabetData): the letters of Greek, Cyrillic, Hebrew, Arabic, Devanagari, Japanese
   kana and Korean with the Latin letters they stand for, and the words and phrases people engrave in 17 languages, each with ALL its meanings and a note where it changes with who says it. Not
   a translator: nothing is worked out or asked of a service.
     · a popover under (or over) the button, kept inside the window (as wide as the window on a phone); never a modal;
     · a search box that reads English ("love", "mother", "peace", "a", "alpha"), a Latin spelling ("ahava", "shalom") or the letters themselves ("愛", "אהבה"); every word typed must be found;
     · Words & phrases | Letters | All, and a language: with no search, the words of that language (or its alphabet, for Letters);
     · a row shows the letters large, what they mean (every meaning), the language, how it is said and the note; a press puts them in the words at the caret through the browser's own text
       insertion (the box's `input` event, its undo), and the picker stays open for more; Esc, a press outside or the button again closes it and the caret is back where it was;
     · ↑ ↓ move through the rows, Enter puts the row in. The language and the tab are kept on this computer (inside try/catch: the picker works without it).
   CNAlphabetPicker = { attach, open, close, isOpen, query(q, { kind, lang }) -> rows (for the tests), insert(text) } */
(function (root) {
  'use strict';
  if (!root || root.CNAlphabetPicker) return;
  const KEY = 'cn.alphabet.pick';
  const tryDo = (f, d) => { try { return f(); } catch (_) { return d; } };
  const esc = v => String(v == null ? '' : v).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const D = () => root.CNAlphabetData || null;
  const doc = () => root.document;
  const MAX = 80;
  const S = { open: false, ta: null, btn: null, key: '', pop: null, q: '', kind: 'words', lang: '', rows: [], at: 0, sel: { s: 0, e: 0 } };
  { const kept = tryDo(() => JSON.parse(root.localStorage.getItem(KEY) || 'null'), null); if (kept && typeof kept === 'object') { if (['words', 'letters', 'all'].includes(kept.kind)) S.kind = kept.kind; if (typeof kept.lang === 'string') S.lang = kept.lang; } }
  const keep = () => tryDo(() => root.localStorage.setItem(KEY, JSON.stringify({ kind: S.kind, lang: S.lang })));

  /* ── the search ── */
  // (a word of the entry, whole or at its start: "mom" finds "mom", "mother" is found by "moth")
  const wordsOf = s => String(s || '').split(/[^\p{L}\p{N}']+/u).filter(Boolean);
  const NONLATIN = /[^\u0000-ɏ\s\d.,'!?-]/u;
  /** The rows for a search: every word of `q` found in the entry (its English, meanings, letter name, Latin spelling, language, note, or its own letters); best first. */
  function query(q, o) {
    const A = D(); if (!A) return [];
    o = o || {};
    const kind = o.kind || 'words', lang = o.lang || '', all = A.entries().filter(e => (kind === 'all' || (kind === 'words' ? e.kind === 'word' : e.kind === 'letter')) && (!lang || e.lang === lang));
    const raw = String(q == null ? '' : q).trim();
    if (!raw) return kind === 'letters' || lang ? all.slice(0, kind === 'letters' ? 400 : MAX) : all.filter(e => POPULAR.has(e.en)).slice(0, MAX);
    // (the little words of a phrase ("forever AND always") are not looked for, unless they are all there is)
    const toks0 = wordsOf(A.plain(raw)), keepT = toks0.filter(t => !STOP.has(t)), toks = toks0.length > 1 && keepT.length ? keepT : toks0, native = NONLATIN.test(raw) ? raw.toLowerCase() : '';
    const scoreOf = (e, every) => {
      if (native) { const t = e.text.toLowerCase(); return t.includes(native) ? (t === native ? 50 : 20) : 0; }
      const en = wordsOf(A.plain(e.en)), mean = wordsOf(A.plain(e.meanings.join(' '))), name = wordsOf(A.plain(e.name)), roman = wordsOf(A.plain(e.roman));
      let score = 0, found = 0;
      for (const t of toks) {
        const hit = (list, whole, start) => (list.includes(t) ? whole : list.some(w => w.startsWith(t)) ? start : 0);
        const s1 = Math.max(hit(en, 8, 4), hit(mean, 7, 3), hit(name, 6, 3), hit(roman, 6, 2), e.hay.includes(t) ? 1 : 0);
        if (!s1 && every) return 0;
        if (s1) { score += s1; found++; }
      }
      if (!found) return 0;
      if (A.plain(e.en) === A.plain(raw) || e.meanings.some(m => A.plain(m) === A.plain(raw))) score += 10;   // (the whole phrase as typed)
      if (e.kind === 'letter' && toks.length === 1 && roman.includes(toks[0])) score += 4;                       // ("a": the letters that ARE an a)
      return score;
    };
    // every word found first; when nothing has them all ("forever and always"), what has any of them, the most of them first
    let out = all.map(e => ({ e, score: scoreOf(e, true) })).filter(x => x.score > 0);
    if (!out.length && !native && toks.length > 1) out = all.map(e => ({ e, score: scoreOf(e, false) })).filter(x => x.score > 0);
    out.sort((x, y) => y.score - x.score || (x.e.kind === y.e.kind ? 0 : x.e.kind === 'word' ? -1 : 1));
    return out.slice(0, MAX).map(x => x.e);
  }
  const STOP = new Set(['and', 'or', 'the', 'of', 'to', 'my', 'a', 'an', 'in', 'for', 'with', 'is']);
  const POPULAR = new Set(['love', 'I love you', 'mother', 'father', 'family', 'forever', 'always', 'hope', 'faith', 'peace', 'strength', 'heart']);

  /* ── the popover ── */
  const CSS = `
.cnAp{position:fixed;z-index:2147483200;width:min(440px,calc(100vw - 16px));max-height:min(560px,calc(100vh - 16px));display:flex;flex-direction:column;background:var(--card,#fffefb);color:var(--ink,#1c1a17);border:1px solid var(--line,#e4ddd0);border-radius:12px;box-shadow:0 14px 40px rgba(30,26,20,.18);font:13px/1.35 var(--sans,system-ui,sans-serif);overflow:hidden}
.cnAp[hidden]{display:none}
.cnAp__head{display:grid;gap:8px;padding:10px 10px 8px;border-bottom:1px solid var(--line,#e4ddd0)}
.cnAp__q{width:100%;box-sizing:border-box;border:1px solid var(--line,#ddd);border-radius:8px;padding:8px 10px;font:14px var(--sans,system-ui,sans-serif);background:var(--card2,#faf8f3);color:inherit}
.cnAp__q:focus{outline:2px solid rgba(169,130,63,.35);border-color:var(--gold,#a9823f);background:var(--card,#fff)}
.cnAp__bar{display:flex;gap:8px;align-items:center;flex-wrap:wrap}
.cnAp__tabs{display:inline-flex;border:1px solid var(--line,#ddd);border-radius:8px;overflow:hidden}
.cnAp__tabs button{border:0;background:transparent;padding:5px 10px;font:600 12px var(--sans,system-ui,sans-serif);color:var(--ink70,#5b554c);cursor:pointer}
.cnAp__tabs button[aria-pressed=true]{background:var(--ink,#1c1a17);color:#fff}
.cnAp__tabs button:hover:not([aria-pressed=true]){background:var(--paper2,#f2eee4)}
.cnAp__lang{flex:1 1 140px;min-width:0;border:1px solid var(--line,#ddd);border-radius:8px;padding:5px 8px;font:12px var(--sans,system-ui,sans-serif);background:var(--card,#fff);color:inherit}
.cnAp__list{overflow:auto;flex:1 1 auto;padding:4px 0;overscroll-behavior:contain}
.cnAp__row{display:grid;grid-template-columns:minmax(64px,auto) 1fr;gap:4px 12px;align-items:center;width:100%;text-align:left;border:0;background:transparent;padding:8px 12px;cursor:pointer;color:inherit;font:inherit}
.cnAp__row:hover,.cnAp__row[aria-selected=true]{background:var(--paper2,#f2eee4)}
.cnAp__row:focus-visible{outline:2px solid var(--gold,#a9823f);outline-offset:-2px}
.cnAp__t{font:24px/1.15 "Noto Sans","Noto Sans CJK SC","Noto Sans Hebrew","Noto Sans Arabic","Noto Sans Devanagari",system-ui,sans-serif;white-space:nowrap;max-width:200px;overflow:hidden;text-overflow:ellipsis;unicode-bidi:plaintext}
.cnAp__row--letter .cnAp__t{font-size:26px;letter-spacing:.06em}
.cnAp__m{min-width:0}
.cnAp__m b{font-weight:600}
.cnAp__m span{color:var(--ink70,#5b554c)}
.cnAp__m small{display:block;color:var(--ink45,#7a746a);font-size:11.5px;margin-top:2px}
.cnAp__empty{padding:22px 14px;color:var(--ink70,#5b554c);text-align:center}
.cnAp__foot{padding:8px 12px;border-top:1px solid var(--line,#e4ddd0);color:var(--ink45,#7a746a);font-size:11.5px}
.cnApBtn{font-weight:600}
@media (prefers-reduced-motion:no-preference){.cnAp{animation:cnApIn .14s ease-out}@keyframes cnApIn{from{opacity:0;transform:translateY(-4px)}}}`;
  const css = () => { const d = doc(); if (!d || d.getElementById('cnApCss')) return; const s = d.createElement('style'); s.id = 'cnApCss'; s.textContent = CSS; d.head.appendChild(s); };
  function langOptions() {
    const A = D(); if (!A) return '';
    const seen = [...new Set(A.entries().map(e => e.lang))].map(l => [l, (A.LANGS[l] || [l])[0]]).sort((a, b) => a[1].localeCompare(b[1]));
    return `<option value="">Every language</option>` + seen.map(([l, n]) => `<option value="${esc(l)}"${S.lang === l ? ' selected' : ''}>${esc(n)}</option>`).join('');
  }
  function build() {
    css();
    const d = doc(), pop = d.createElement('div');
    pop.className = 'cnAp'; pop.hidden = true; pop.setAttribute('role', 'dialog'); pop.setAttribute('aria-label', 'Letters, words and phrases of other alphabets');
    pop.innerHTML = `<div class="cnAp__head"><input class="cnAp__q" type="search" placeholder="Search in English: love, mother, peace, a, alpha…" aria-label="Search letters, words and phrases" autocomplete="off" spellcheck="false">
      <div class="cnAp__bar"><span class="cnAp__tabs" role="group" aria-label="What to show"><button type="button" data-k="words">Words &amp; phrases</button><button type="button" data-k="letters">Letters</button><button type="button" data-k="all">All</button></span>
      <select class="cnAp__lang" aria-label="Language">${langOptions()}</select></div></div>
      <div class="cnAp__list" role="listbox" aria-label="Results"></div>
      <div class="cnAp__foot">A press adds it to the words. A reference, not a translator: check the spelling with the customer, some words change with who says them.</div>`;
    // (a press inside is no press outside, and never takes the caret from the words box unless it is the search box)
    pop.addEventListener('pointerdown', e => { e.stopPropagation(); if (!e.target.closest('input,select')) e.preventDefault(); });
    for (const t of ['pointerup', 'click', 'dblclick', 'contextmenu', 'wheel', 'keydown', 'keyup']) pop.addEventListener(t, e => e.stopPropagation());
    const q = pop.querySelector('.cnAp__q'), lang = pop.querySelector('.cnAp__lang');
    q.addEventListener('input', () => { S.q = q.value; S.at = 0; draw(); });
    q.addEventListener('keydown', onKey);
    lang.addEventListener('change', () => { S.lang = lang.value; S.at = 0; keep(); draw(); });
    pop.querySelectorAll('[data-k]').forEach(b => b.addEventListener('click', () => { S.kind = b.dataset.k; S.at = 0; keep(); draw(); }));
    pop.querySelector('.cnAp__list').addEventListener('click', e => { const r = e.target.closest('[data-i]'); if (r) pick(+r.dataset.i); });
    pop.addEventListener('keydown', e => { if (e.target !== q) onKey(e); });
    d.body.appendChild(pop);
    S.pop = pop;
  }
  function onKey(e) {
    if (e.key === 'Escape') { e.preventDefault(); close(true); return; }
    if (e.key === 'ArrowDown' || e.key === 'ArrowUp') { e.preventDefault(); if (!S.rows.length) return; S.at = (S.at + (e.key === 'ArrowDown' ? 1 : -1) + S.rows.length) % S.rows.length; mark(true); return; }
    if (e.key === 'Enter') { e.preventDefault(); if (S.rows[S.at]) pick(S.at); }
  }
  function rowHtml(e, i) {
    const meaning = e.kind === 'letter' ? `<b>${esc(e.roman)}</b> <span>· ${esc(e.name)} · ${esc(e.langName)}</span>` : `<b>${esc(e.meanings.join(', '))}</b> <span>· ${esc(e.langName)}${e.roman && e.roman !== e.text ? ' · ' + esc(e.roman) : ''}</span>`;
    return `<button type="button" class="cnAp__row cnAp__row--${e.kind}" role="option" data-i="${i}" aria-selected="${i === S.at}" title="Add ${esc(e.text)} to the words"><span class="cnAp__t" lang="${esc(e.lang)}" dir="auto">${esc(e.text)}</span><span class="cnAp__m">${meaning}${e.note ? `<small>${esc(e.note)}</small>` : ''}</span></button>`;
  }
  function draw() {
    const pop = S.pop; if (!pop) return;
    pop.querySelectorAll('[data-k]').forEach(b => b.setAttribute('aria-pressed', String(b.dataset.k === S.kind)));
    const sel = pop.querySelector('.cnAp__lang'); if (sel.value !== S.lang) sel.value = S.lang;
    S.rows = query(S.q, { kind: S.kind, lang: S.lang });
    const list = pop.querySelector('.cnAp__list');
    list.innerHTML = S.rows.length ? S.rows.map(rowHtml).join('') : `<div class="cnAp__empty">Nothing found for “${esc(S.q)}”${S.lang ? ' in ' + esc((D().LANGS[S.lang] || [S.lang])[0]) : ''}. Try another English word${S.lang ? ', or every language' : ''}.</div>`;
    list.scrollTop = 0;
  }
  function mark(scroll) {
    if (!S.pop) return;
    S.pop.querySelectorAll('.cnAp__row').forEach(r => r.setAttribute('aria-selected', String(+r.dataset.i === S.at)));
    const r = S.pop.querySelector(`.cnAp__row[data-i="${S.at}"]`); if (scroll && r) r.scrollIntoView({ block: 'nearest' });
  }
  function place() {
    const pop = S.pop, b = S.btn; if (!pop || !b || !b.isConnected) return;
    const r = b.getBoundingClientRect(), vw = doc().documentElement.clientWidth || root.innerWidth, vh = doc().documentElement.clientHeight || root.innerHeight;
    pop.style.left = '0px'; pop.style.top = '0px';
    const pr = pop.getBoundingClientRect(), w = pr.width, h = pr.height;
    const left = Math.max(8, Math.min(vw - w - 8, r.right - w)), below = r.bottom + 6, top = below + h <= vh - 8 ? below : Math.max(8, r.top - h - 6);
    pop.style.left = left + 'px'; pop.style.top = top + 'px';
  }
  /** The text put in the words at the caret (or over the selection), as typing puts it: the box's input event and its undo. */
  function insert(text) {
    const ta = S.ta; if (!ta || !ta.isConnected || !text) return false;
    let ok = false;
    try {
      let s = ta.selectionStart, e = ta.selectionEnd; if (doc().activeElement !== ta) { s = S.sel.s; e = S.sel.e; }
      ta.focus({ preventScroll: true }); ta.setSelectionRange(s, e);
      ok = doc().execCommand('insertText', false, text);
    } catch (_) { ok = false; }
    if (!ok) { const s = ta.selectionStart, e = ta.selectionEnd; ta.setRangeText(text, s, e, 'end'); ta.dispatchEvent(new Event('input', { bubbles: true })); }
    S.sel = { s: ta.selectionStart, e: ta.selectionEnd };
    return true;
  }
  function pick(i) {
    const e = S.rows[i]; if (!e) return;
    // (a letter shown with its other forms, "Α α" or "כ ך", gives its first: the capital, or the form inside a word)
    const text = e.kind === 'letter' ? e.text.split(' ')[0] : e.text;
    S.at = i; mark(false);
    insert(text);
    try { const q = S.pop.querySelector('.cnAp__q'); if (S.q) { q.focus({ preventScroll: true }); } } catch (_) {}
  }
  const outside = e => { if (!S.open) return; if (S.pop && S.pop.contains(e.target)) return; if (S.btn && S.btn.contains(e.target)) return; close(false); };
  function open(key, ta, btn) {
    if (!D()) return false;
    if (!S.pop) build();
    if (S.open) close(false);
    S.open = true; S.key = key || ''; S.ta = ta; S.btn = btn; S.sel = { s: ta.selectionStart || 0, e: ta.selectionEnd || 0 };
    btn.setAttribute('aria-expanded', 'true'); btn.classList.add('on');
    S.pop.hidden = false; draw(); place();
    const q = S.pop.querySelector('.cnAp__q'); q.value = S.q; q.focus({ preventScroll: true }); q.select();
    doc().addEventListener('pointerdown', outside, true); root.addEventListener('resize', place); root.addEventListener('scroll', place, true);
    return true;
  }
  function close(refocus) {
    if (!S.open) return;
    S.open = false; if (S.pop) S.pop.hidden = true;
    if (S.btn) { S.btn.setAttribute('aria-expanded', 'false'); S.btn.classList.remove('on'); }
    doc().removeEventListener('pointerdown', outside, true); root.removeEventListener('resize', place); root.removeEventListener('scroll', place, true);
    if (refocus && S.ta && S.ta.isConnected) tryDo(() => { S.ta.focus({ preventScroll: true }); S.ta.setSelectionRange(S.sel.s, S.sel.e); });
  }
  /** A words box and its button: the button opens and closes the picker over this box; the box's caret is remembered for a pick. → false when there is nothing to offer (the button goes). */
  function attach(o) {
    const ta = o && o.textarea, btn = o && o.button;
    if (!ta || !btn) return false;
    if (!D()) { tryDo(() => btn.remove()); return false; }
    const key = String(o.key == null ? '' : o.key);
    btn.setAttribute('aria-haspopup', 'dialog'); btn.setAttribute('aria-expanded', 'false');
    btn.addEventListener('click', e => { e.preventDefault(); e.stopPropagation(); if (S.open && S.btn === btn) close(true); else open(key, ta, btn); });
    const markSel = () => { if (S.ta === ta && doc().activeElement === ta) S.sel = { s: ta.selectionStart, e: ta.selectionEnd }; };
    ['keyup', 'mouseup', 'select', 'input', 'touchend'].forEach(ev => ta.addEventListener(ev, markSel));
    if (S.open && S.key === key) { S.ta = ta; S.btn = btn; btn.setAttribute('aria-expanded', 'true'); btn.classList.add('on'); place(); }
    return true;
  }
  root.CNAlphabetPicker = { attach, open, close: refocus => close(!!refocus), isOpen: () => S.open, query, insert, state: () => ({ open: S.open, q: S.q, kind: S.kind, lang: S.lang, rows: S.rows.length }) };
})(typeof window !== 'undefined' ? window : globalThis);
