/* The emoji picker for the words of the Back engraving (Paul, 6 Oct 2026: "You need to add a new button as part of the Back
 * Engraving ... The new button "Emoji" must contain all of the compatible emojis that can be included in the text box. Make it a
 * comprehensive emoji list popup that is easily usable by the user at a glance.").
 *
 * ONE component for every words box (the Engraving card, the Review card that asks for the words): charm-nest-bridge.js calls
 *     CNEmojiPicker.attach({ textarea, button, key })
 * after it builds a card, and the picker does the rest. What it offers is exactly what the laser can engrave: charm-nest-emoji-data.js
 * (CNEmojiData) lists the sequences that are in vendor/fonts/emoji-sequences.json and not unsupported; each cell is drawn as it will
 * be ENGRAVED, in one colour, by the same code the engraver uses: the engraving font that Engrave loads (Source Sans 3 wrapped by
 * CharmNestText.withEmoji with the Noto Emoji outlines and the shape map) is asked for the path of the cell's text, exactly as
 * Engrave.fitText asks for it, and the path is drawn as an inline SVG. No second font file, no colour system emoji, no copy of the
 * map. While that font is not loaded yet (first open, or a retry after a failed load) the picker says "Loading emoji…".
 *
 * Behaviour (plan: /plans/emoji-picker/plan.md):
 *   · a popover under (or over) the button, fixed to the window with the page's own popover conventions (like the mail menu), kept
 *     inside the window, a panel as wide as the window on a phone; never a modal, never full screen;
 *   · category tabs (one icon each) and Recent, a search box, big cells in a virtualised grid (only the rows in view
 *     exist), the name of the emoji under the pointer or the keys in the footer line, the count of what the laser can engrave;
 *   · a pick goes into the textarea at the caret (or over the selection) through the browser's own text insertion, so the textarea sees
 *     the same `input` event typing gives it, the undo stack and the line breaks stay, and nothing is submitted; the picker stays
 *     open for several picks. Esc, a tap outside, or the button again closes it and the caret is back where it was;
 *   · the card behind the picker is rebuilt now and then (the fit after a pick redraws it): the picker is not part of the card, it
 *     just follows the new textarea and button, so it does not flicker or lose its place;
 *   · skin tones: the laser cuts one outline for almost every tone of a person or hand, so there is no tone chooser; the six emoji
 *     whose outline does change with the tone (ts) show a strip of their six variants, drawn as engraved, for the cell under the pointer
 *     or the keys; any other pick puts in the plain emoji (typed or pasted toned ones still engrave and still resolve here);
 *   · a cell that stands for other spellings with exactly the same outline (s: man/woman health worker ...) says so in the footer;
 *   · the last 24 picks are kept in localStorage (inside try/catch; the picker works without it).
 */
(function (root) {
  'use strict';
  const RECENT_KEY = 'cn.emoji.recent', RECENT_MAX = 24;
  const tryDo = (f, d) => { try { return f(); } catch (_) { return d; } };
  const store = { get: k => tryDo(() => root.localStorage.getItem(k), null), set: (k, v) => tryDo(() => { root.localStorage.setItem(k, v); }) };
  const esc = v => String(v == null ? '' : v).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const D = () => root.CNEmojiData || null;
  const doc = () => root.document;
  const coarse = () => !!tryDo(() => root.matchMedia('(pointer:coarse)').matches, false);
  const CLOCK = '<svg viewBox="0 0 24 24" aria-hidden="true"><circle cx="12" cy="12" r="8.5" fill="none" stroke="currentColor" stroke-width="1.8"/><path d="M12 7.2V12l3.2 2" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/></svg>';
  const MAGNIFIER = '<svg viewBox="0 0 24 24" aria-hidden="true"><circle cx="10.5" cy="10.5" r="6" fill="none" stroke="currentColor" stroke-width="1.8"/><path d="M15 15l5 5" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"/></svg>';

  const S = {
    open: false, key: null, ta: null, btn: null, sel: { s: 0, e: 0 },
    q: '', recent: [], act: -1, dirtyRecent: false, stripItem: null, stripFor: '', stripFont: '', stripTimer: 0,
    font: null, fontState: 'idle', fontP: null, gcache: new Map(), idx: null,
    pop: null, el: {}, rows: [], tops: [], flat: [], secs: [], cols: 6, cell: 40, gone: 0, raf: 0, painted: new Map(), watch: null
  };

  /* ── the engraving font: the very one Engrave cuts with ── */
  async function defaultFont() {
    const E = root.Engrave;
    if (!E || !E.loadFonts) return null;
    await E.loadFonts();
    const F = E.fonts;
    return F && F.ok && F.emoji && F.Regular && F.Regular.getPath ? F.Regular : null;
  }
  function ensureFont() {
    if (S.fontState === 'ready') return Promise.resolve();
    if (S.fontP) return S.fontP;
    S.fontState = 'loading'; loading(true);
    S.fontP = (async () => {
      let f = null;
      try { f = await (S.provider || defaultFont)(); } catch (_) { f = null; }
      S.font = f; S.fontState = f ? 'ready' : 'failed'; S.gcache.clear();
    })().then(() => { S.fontP = null; loading(false); paintTabs(); repaint(); foot(); });
    return S.fontP;
  }
  /* the path of one cell, from the engraving font: size 100, baseline 0; the emoji box is the cap height tall, as Engrave draws it */
  function glyph(seq) {
    let g = S.gcache.get(seq);
    if (g !== undefined) return g;
    g = null;
    try {
      const f = S.font, size = 100, cap = (f.tables.os2.sCapHeight || f.ascender * .7) / f.unitsPerEm * size;
      const p = f.getPath(seq, 0, 0, size), adv = f.getAdvanceWidth(seq, size);
      if (p.commands.length) { const w = Math.max(cap, adv - size * .06); g = { d: p.toPathData(1), vb: `0 ${(-cap).toFixed(2)} ${w.toFixed(2)} ${cap.toFixed(2)}` }; }
    } catch (_) { g = null; }
    S.gcache.set(seq, g);
    return g;
  }
  const glyphHTML = (seq, cls) => {
    if (S.fontState === 'ready') { const g = glyph(seq); if (g) return `<svg class="emG${cls ? ' ' + cls : ''}" viewBox="${g.vb}" aria-hidden="true"><path d="${g.d}"/></svg>`; return `<span class="emTx" aria-hidden="true">${esc(seq)}</span>`; }
    if (S.fontState === 'failed') return `<span class="emTx" aria-hidden="true">${esc(seq)}</span>`;
    return '<span class="emGhost"></span>';
  };

  /* ── what is on offer ── */
  function index() {
    if (S.idx) return S.idx;
    const d = D(), by = new Map(), rev = new Map();
    for (const g of d.groups || []) for (const it of g.items || []) by.set(it.c, it);
    for (const [base, m] of Object.entries(d.toned || {})) for (const [tid, full] of Object.entries(m || {})) rev.set(full, { base, tid });
    return (S.idx = { by, rev });
  }
  const show = it => ({ seq: it.c, name: it.n, item: it });
  // the names of the spellings that engrave exactly like a cell (the data lists their sequences, not their names)
  const MAN = /\u{1F468}|\u2642/u, WOMAN = /\u{1F469}|\u2640/u;
  function alsoName(seq, it) {
    const g = MAN.test(seq) ? 'man' : WOMAN.test(seq) ? 'woman' : '';
    if (!g) return seq;
    return (g + ' ' + String(it.n).replace(/^person\b\s*/, '')).replace(/\s+:/, ':').trim();
  }
  function resolve(seq) {
    const ix = index(), d = D();
    let it = ix.by.get(seq);
    if (it) return { seq, name: it.n, item: it };
    const r = ix.rev.get(seq);
    if (r) { const b = ix.by.get(r.base); if (b) { const t = (d.tones || []).find(x => x.id === r.tid); return { seq, name: b.n + (t ? ' · ' + t.name.toLowerCase() + ' skin' : ''), item: b }; } }
    it = d.find ? d.find(seq) : null;
    return it ? { seq, name: it.n, item: it } : null;
  }
  const loadRecent = () => { const a = tryDo(() => JSON.parse(store.get(RECENT_KEY) || '[]'), []); return (Array.isArray(a) ? a : []).filter(x => typeof x === 'string').slice(0, RECENT_MAX); };
  function pushRecent(seq) {
    S.recent = [seq, ...loadRecent().filter(x => x !== seq)].slice(0, RECENT_MAX);
    store.set(RECENT_KEY, JSON.stringify(S.recent)); S.dirtyRecent = true;
  }

  /* ── the rows of the grid (a heading, then the cells in rows of S.cols), and which of them exist right now ── */
  function layout() {
    const d = D(), secs = [];
    if (S.q) {
      let r = []; try { r = d.search(S.q) || []; } catch (_) { r = []; }
      secs.push({ id: 'results', name: r.length ? `${r.length} found` : 'Nothing matches', list: r.map(show), note: `No emoji the laser can engrave matches “${S.q}”. Try another word, such as heart, hand, or a flag’s country.` });
    } else {
      secs.push({ id: 'recent', name: 'Recent', list: S.recent.map(resolve).filter(Boolean), note: 'The emoji you pick appear here.' });
      for (const g of d.groups || []) secs.push({ id: g.id, name: g.name, list: (g.items || []).map(show) });
    }
    const rows = [], flat = [], cols = S.cols, cell = S.cell;
    for (const s of secs) {
      s.row = rows.length; rows.push({ t: 'head', h: 30, s });
      if (!s.list.length) { rows.push({ t: 'note', h: S.q ? 58 : 34, text: s.note }); continue; }
      for (let i = 0; i < s.list.length; i += cols) { const part = s.list.slice(i, i + cols); rows.push({ t: 'row', h: cell, from: flat.length, n: part.length }); for (const x of part) flat.push(x); }
    }
    const tops = []; let y = 4; for (const r of rows) { tops.push(y); y += r.h; }
    // room under the last section, so that every tab can bring its section to the top of the grid
    const lastTop = tops[secs[secs.length - 1].row], pad = S.q ? 8 : Math.max(8, S.el.grid.clientHeight - (y - lastTop));
    S.secs = secs; S.rows = rows; S.tops = tops; S.flat = flat; S.total = y + pad;
    S.el.spacer.style.height = S.total + 'px';
    S.painted.forEach(n => n.remove()); S.painted.clear();
    if (S.act >= flat.length) S.act = flat.length - 1;
    S.dirtyRecent = false;
  }
  function paint() {
    const g = S.el.grid; if (!g || !S.rows.length) return;
    const top = g.scrollTop, bottom = top + g.clientHeight, pad = S.cell * 3;
    let lo = 0, hi = S.rows.length - 1;
    while (lo < hi) { const mid = (lo + hi) >> 1; if (S.tops[mid] + S.rows[mid].h < top - pad) lo = mid + 1; else hi = mid; }
    const want = new Set();
    for (let i = lo; i < S.rows.length && S.tops[i] < bottom + pad; i++) {
      want.add(i);
      if (S.painted.has(i)) continue;
      const r = S.rows[i], n = doc().createElement('div');
      n.style.top = S.tops[i] + 'px'; n.style.height = r.h + 'px';
      if (r.t === 'head') { n.className = 'emHead'; n.dataset.s = r.s.id; n.textContent = r.s.name; }
      else if (r.t === 'note') { n.className = 'emNote'; n.textContent = r.text; }
      else {
        n.className = 'emRow'; let h = '';
        for (let k = 0; k < r.n; k++) { const j = r.from + k, it = S.flat[j]; h += `<div class="emCell${j === S.act ? ' act' : ''}" role="option" id="emc${j}" data-i="${j}" aria-label="${esc(it.name)}" aria-selected="${j === S.act}">${glyphHTML(it.seq)}</div>`; }
        n.innerHTML = h;
      }
      S.el.spacer.appendChild(n); S.painted.set(i, n);
    }
    S.painted.forEach((n, i) => { if (!want.has(i)) { n.remove(); S.painted.delete(i); } });
  }
  function repaint() { if (!S.open) return; S.painted.forEach(n => n.remove()); S.painted.clear(); paint(); }
  function measure() {
    const g = S.el.grid; S.cell = coarse() ? 44 : 40;
    const w = g.clientWidth - 8; S.cols = Math.max(4, Math.floor(w / S.cell));
    g.style.setProperty('--emCols', S.cols); g.style.setProperty('--emCell', S.cell + 'px');
  }
  function section() {   // the section at the top of the grid, for the tab that is "on"
    const t = S.el.grid.scrollTop + 6; let k = 0;
    for (let i = 0; i < S.rows.length; i++) { if (S.rows[i].t === 'head' && S.tops[i] <= t) k = i; else if (S.tops[i] > t) break; }
    return S.rows[k] && S.rows[k].s ? S.rows[k].s.id : null;
  }
  function tabsOn() {
    const id = S.q ? null : section();
    S.el.tabs.querySelectorAll('.emTab').forEach(b => { const on = b.dataset.g === id; b.classList.toggle('on', on); b.setAttribute('aria-selected', on); b.tabIndex = on || (!id && b.dataset.g === 'recent') ? 0 : -1; });
  }
  function jump(id) {
    if (S.q) { S.q = ''; S.el.input.value = ''; layout(); }
    else if (id === 'recent' && S.dirtyRecent) layout();
    const s = S.secs.find(x => x.id === id); if (!s) return;
    S.el.grid.scrollTop = S.tops[s.row] - 2; paint(); tabsOn();
  }
  const paintTabs = () => { if (!S.el.tabs) return; S.el.tabs.querySelectorAll('.emTab[data-g]').forEach(b => { if (b.dataset.g === 'recent') return; const g = (D().groups || []).find(x => x.id === b.dataset.g); b.querySelector('.emTI').innerHTML = g ? glyphHTML(g.icon) : ''; }); foot(); };

  /* ── the line at the foot: the name of what is under the pointer or the keys, and what the laser can engrave ── */
  function buildStrip(it) {
    const d = D(), m = d.toned[it.c] || {};
    return (d.tones || []).map(t => { const seq = t.id ? m[t.id] : it.c; return seq ? `<button type="button" class="emSt" data-seq="${esc(seq)}" data-nm="${esc(it.n + ' · ' + (t.id ? t.name + ' skin tone' : 'no skin tone'))}" aria-label="${esc(it.n + ', ' + (t.id ? t.name + ' skin tone' : 'no skin tone'))}">${glyphHTML(seq)}</button>` : ''; }).join('');
  }
  function foot(hint) {
    if (!S.el.nm) return;
    const cell = S.act >= 0 ? S.flat[S.act] : null, it = cell && cell.item, d = D(), sit = S.stripItem;
    S.el.pv.innerHTML = cell ? glyphHTML(cell.seq) : '';
    S.el.nm.textContent = hint || (cell ? cell.name : S.fontState === 'failed' ? 'The laser outline could not load; emoji still go into the words.' : 'Pick one: it goes in at the caret.');
    const want = sit && d.toned && d.toned[sit.c] ? sit.c : '';
    if (want !== S.stripFor || S.stripFont !== S.fontState) { S.stripFor = want; S.stripFont = S.fontState; S.el.strip.innerHTML = want ? buildStrip(sit) : ''; }
    S.el.strip.hidden = !want; S.el.ct.hidden = !!want;
    const also = it && it.s && it.s.length ? 'Also: ' + it.s.map(x => alsoName(x, it)).join(', ') : '';
    S.el.ct.textContent = also || `${Number(d.count || 0).toLocaleString('en-US')} emojis the laser can engrave`;
    S.el.ct.classList.toggle('also', !!also);
  }
  function loading(on) { if (S.el.load) S.el.load.hidden = !on; if (S.el.grid) S.el.grid.setAttribute('aria-busy', on ? 'true' : 'false'); }

  /* ── the choice of the keys and the pointer ── */
  function strip(i, key) {   // the tone strip belongs to the last emoji whose tones engrave differently; keys drop it at once, a pointer on its way to it gets a moment
    const it = i >= 0 && S.flat[i] ? S.flat[i].item : null;
    clearTimeout(S.stripTimer);
    if (it && it.ts) S.stripItem = it;
    else if (S.stripItem) { if (key) S.stripItem = null; else S.stripTimer = setTimeout(() => { if (S.open && S.el.strip.matches(':hover, :focus-within')) return; S.stripItem = null; foot(); }, 1600); }
  }
  function setAct(i, scroll) {
    strip(i, scroll);
    if (S.act === i) return foot();
    const old = S.el.spacer.querySelector('.emCell.act'); if (old) { old.classList.remove('act'); old.setAttribute('aria-selected', 'false'); }
    S.act = i;
    if (i >= 0) {
      if (scroll) {
        const row = S.rows.findIndex(r => r.t === 'row' && i >= r.from && i < r.from + r.n), g = S.el.grid;
        if (row >= 0) { const t = S.tops[row], b = t + S.rows[row].h; if (t - 30 < g.scrollTop) g.scrollTop = Math.max(0, t - 34); else if (b > g.scrollTop + g.clientHeight) g.scrollTop = b - g.clientHeight + 6; paint(); }
      }
      const n = S.el.spacer.querySelector(`#emc${i}`); if (n) { n.classList.add('act'); n.setAttribute('aria-selected', 'true'); }
      S.el.grid.setAttribute('aria-activedescendant', 'emc' + i);
    } else S.el.grid.removeAttribute('aria-activedescendant');
    foot();
  }
  function move(key) {
    const n = S.flat.length; if (!n) return;
    if (S.act < 0) return setAct(0, true);
    const rowOf = i => S.rows.findIndex(r => r.t === 'row' && i >= r.from && i < r.from + r.n);
    let i = S.act;
    if (key === 'ArrowRight') i = Math.min(n - 1, i + 1);
    else if (key === 'ArrowLeft') i = Math.max(0, i - 1);
    else if (key === 'Home') i = 0;
    else if (key === 'End') i = n - 1;
    else {
      const rows = S.rows, here = rowOf(i), col = i - rows[here].from, dir = key === 'ArrowUp' || key === 'PageUp' ? -1 : 1;
      let steps = key.startsWith('Page') ? Math.max(1, Math.floor(S.el.grid.clientHeight / S.cell) - 1) : 1, r = here;
      while (steps > 0) { let q = r + dir; while (q >= 0 && q < rows.length && rows[q].t !== 'row') q += dir; if (q < 0 || q >= rows.length) break; r = q; steps--; }
      i = rows[r].from + Math.min(col, rows[r].n - 1);
    }
    setAct(i, true);
  }

  /* ── putting it in the words ── */
  function insert(seq) {
    const ta = S.ta; if (!ta || !ta.isConnected || !seq) return false;
    const was = doc().activeElement, inPop = was && S.pop.contains(was);
    const hadMode = ta.getAttribute('inputmode');
    let ok = false;
    try {
      let s = ta.selectionStart, e = ta.selectionEnd; if (s == null) { s = S.sel.s; e = S.sel.e; }
      ta.setAttribute('inputmode', 'none');   // a phone must not raise its keyboard for a pick
      ta.focus({ preventScroll: true }); ta.setSelectionRange(s, e);
      ok = doc().execCommand('insertText', false, seq);   // the browser's own insertion: the `input` event of typing, the undo stack, the caret after it
    } catch (_) { ok = false; }
    if (!ok) {   // (no execCommand: write it and say so the way typing does)
      const s = ta.selectionStart, e = ta.selectionEnd; ta.setRangeText(seq, s, e, 'end');
      ta.dispatchEvent(new root.InputEvent('input', { bubbles: true, inputType: 'insertText', data: seq }));
    }
    S.sel = { s: ta.selectionStart, e: ta.selectionEnd };
    if (hadMode == null) ta.removeAttribute('inputmode'); else ta.setAttribute('inputmode', hadMode);
    if (inPop && was.isConnected) was.focus({ preventScroll: true }); else if (inPop) S.el.grid.focus({ preventScroll: true });
    pushRecent(seq);
    if (S.dirtyRecent && S.el.grid.scrollTop < (S.tops[S.secs.length > 1 ? 1 : 0] || 0)) { const a = S.act; const keep = S.act >= 0 ? S.flat[S.act].seq : null; layout(); if (keep != null) { const k = S.flat.findIndex(x => x.seq === keep); S.act = k; } paint(); tabsOn(); foot(); }
    return true;
  }

  /* ── where it sits ── */
  function place() {
    const pop = S.pop; if (!pop || !S.btn) return;
    const vv = root.visualViewport, vw = vv ? vv.width : root.innerWidth, vh = vv ? vv.height : root.innerHeight, ox = vv ? vv.offsetLeft : 0, oy = vv ? vv.offsetTop : 0;
    // anchored to the words box as a whole (the box, its label, the Emoji button), so the words being written stay in view beside it
    const M = 8, W = Math.min(vw <= 520 ? vw - 2 * M : 380, vw - 2 * M), bb = S.btn.getBoundingClientRect(), box = S.ta && S.ta.isConnected && S.ta.closest('.pvWords');
    const b = box ? box.getBoundingClientRect() : (() => { const t = S.ta && S.ta.isConnected ? S.ta.getBoundingClientRect() : bb; return { left: Math.min(t.left, bb.left), right: Math.max(t.right, bb.right), top: Math.min(t.top, bb.top), bottom: Math.max(t.bottom, bb.bottom) }; })();
    if (box && (b.width === 0 || b.height === 0)) return;
    const below = oy + vh - b.bottom - 6 - M, above = b.top - oy - 6 - M, MAXH = 470, WANT = 360;
    let top, H;
    if (below >= Math.min(MAXH, WANT) || below >= above) { H = Math.min(MAXH, below); top = b.bottom + 6; } else { H = Math.min(MAXH, above); top = b.top - 6 - H; }
    if (H < 250) { H = Math.min(MAXH, vh - 2 * M); top = oy + M; if (b.bottom + 6 + 250 <= oy + vh - M) { H = Math.min(MAXH, oy + vh - M - (b.bottom + 6)); top = b.bottom + 6; } }
    let left = Math.min(Math.max(b.left, ox + M), ox + vw - M - W); if (vw <= 520) left = ox + M;
    pop.style.cssText = `left:${Math.round(left)}px;top:${Math.round(Math.max(oy + M, top))}px;width:${Math.round(W)}px;height:${Math.round(H)}px`;
  }
  const queuePlace = () => { if (S.raf || !S.open) return; S.raf = root.requestAnimationFrame(() => { S.raf = 0; if (!S.open) return; place(); const c = S.cols; measure(); if (c !== S.cols) layout(); paint(); }); };

  /* ── open and close ── */
  function build() {
    if (S.pop) return;
    const d = doc(), pop = d.createElement('div');
    pop.className = 'emPop'; pop.hidden = true; pop.setAttribute('role', 'dialog'); pop.setAttribute('aria-label', 'Emoji');
    const groups = (D().groups || []);
    pop.innerHTML = `<div class="emTabs" role="tablist" aria-label="Emoji categories"><button type="button" class="emTab" role="tab" data-g="recent" aria-label="Recent" data-nm="Recent">${CLOCK}</button>${groups.map(g => `<button type="button" class="emTab" role="tab" data-g="${esc(g.id)}" aria-label="${esc(g.name)}" data-nm="${esc(g.name)}"><span class="emTI"></span></button>`).join('')}</div>
      <div class="emBar"><label class="emSearch">${MAGNIFIER}<input type="search" class="emQ" placeholder="Search emoji" aria-label="Search emoji" autocomplete="off" autocapitalize="off" spellcheck="false" enterkeyhint="search"></label></div>
      <div class="emBody"><div class="emGrid" role="listbox" tabindex="0" aria-label="Emoji"><div class="emSpacer"></div></div><div class="emLoad" role="status" hidden><span class="emSpin" aria-hidden="true"></span><span>Loading emoji…</span></div></div>
      <div class="emFoot"><span class="emPv" aria-hidden="true"></span><span class="emNm" aria-live="polite"></span><span class="emCt"></span><div class="emStrip" role="group" aria-label="Skin tones of this emoji" hidden></div></div>`;
    d.body.appendChild(pop);
    S.pop = pop;
    S.el = { tabs: pop.querySelector('.emTabs'), input: pop.querySelector('.emQ'), grid: pop.querySelector('.emGrid'), spacer: pop.querySelector('.emSpacer'), load: pop.querySelector('.emLoad'), pv: pop.querySelector('.emPv'), nm: pop.querySelector('.emNm'), ct: pop.querySelector('.emCt'), strip: pop.querySelector('.emStrip') };
    const el = S.el;
    el.grid.addEventListener('scroll', () => { if (S.raf2) return; S.raf2 = root.requestAnimationFrame(() => { S.raf2 = 0; paint(); tabsOn(); }); }, { passive: true });
    el.grid.addEventListener('click', e => { const c = e.target.closest('.emCell'); if (c) { setAct(+c.dataset.i, false); insert(S.flat[+c.dataset.i].seq); } });
    el.grid.addEventListener('mouseover', e => { const c = e.target.closest('.emCell'); if (c) setAct(+c.dataset.i, false); });
    el.tabs.addEventListener('click', e => { const b = e.target.closest('.emTab'); if (b) jump(b.dataset.g); });
    el.tabs.addEventListener('mouseover', e => { const b = e.target.closest('.emTab'); if (b) foot(b.dataset.nm); });
    el.tabs.addEventListener('focusin', e => { const b = e.target.closest('.emTab'); if (b) foot(b.dataset.nm); });
    el.tabs.addEventListener('mouseleave', () => foot());
    el.strip.addEventListener('mouseover', e => { const b = e.target.closest('.emSt'); if (b) foot(b.dataset.nm); });
    el.strip.addEventListener('focusin', e => { const b = e.target.closest('.emSt'); if (b) foot(b.dataset.nm); });
    el.strip.addEventListener('mouseleave', () => foot());
    el.strip.addEventListener('click', e => { const b = e.target.closest('.emSt'); if (b) insert(b.dataset.seq); });
    el.input.addEventListener('input', () => {
      S.q = el.input.value.trim(); S.act = -1; layout(); el.grid.scrollTop = 0; paint(); tabsOn(); setAct(S.flat.length && S.q ? 0 : -1, false); foot();
    });
    pop.addEventListener('keydown', onKey);
    ['keyup', 'keypress'].forEach(t => pop.addEventListener(t, e => e.stopPropagation()));
    pop.addEventListener('focusin', () => { S.gone = 0; });
  }
  function onKey(e) {
    const t = e.target, k = e.key;
    e.stopPropagation();   // the card's single-key shortcuts (A approves, S skips) and the page's must not hear the picker
    if (k === 'Escape') { e.preventDefault(); close(true); return; }
    if (t === S.el.grid) {
      if (['ArrowLeft', 'ArrowRight', 'ArrowUp', 'ArrowDown', 'Home', 'End', 'PageUp', 'PageDown'].includes(k)) { e.preventDefault(); move(k); }
      else if (k === 'Enter' || k === ' ') { e.preventDefault(); if (S.act >= 0) insert(S.flat[S.act].seq); }
      else if (k.length === 1 && !e.ctrlKey && !e.metaKey && !e.altKey) { S.el.input.focus({ preventScroll: true }); }   // typing starts a search
      return;
    }
    if (t === S.el.input) {
      if (k === 'ArrowDown') { e.preventDefault(); S.el.grid.focus({ preventScroll: true }); if (S.act < 0) move('Home'); }
      else if (k === 'Enter') { e.preventDefault(); if (S.act < 0 && S.flat.length) setAct(0, true); if (S.act >= 0) insert(S.flat[S.act].seq); }
      return;
    }
    if (t.classList && (t.classList.contains('emTab') || t.classList.contains('emSt')) && (k === 'ArrowLeft' || k === 'ArrowRight')) {
      const list = [...t.parentElement.querySelectorAll(t.classList.contains('emTab') ? '.emTab' : '.emSt')], i = list.indexOf(t), n = list[(i + (k === 'ArrowRight' ? 1 : list.length - 1)) % list.length];
      e.preventDefault(); n.tabIndex = 0; n.focus({ preventScroll: true });
    }
  }
  function watch(on) {
    const d = doc();
    if (!on) { if (S.watch) { const w = S.watch; d.removeEventListener('pointerdown', w.down, true); d.removeEventListener('keydown', w.key, true); root.removeEventListener('resize', w.move); root.removeEventListener('scroll', w.move, true); if (root.visualViewport) { root.visualViewport.removeEventListener('resize', w.move); root.visualViewport.removeEventListener('scroll', w.move); } clearInterval(w.iv); S.watch = null; } return; }
    const w = S.watch = {
      down: e => { const t = e.target; if (S.pop.contains(t) || (S.btn && S.btn.contains(t))) return; const interactive = t.closest && t.closest('button,a,input,select,textarea,summary,label,[tabindex],[contenteditable]'); close(!interactive); },
      key: e => { if (e.key === 'Escape' && !e.defaultPrevented) { e.preventDefault(); e.stopPropagation(); close(true); } },   // (before the card hears it: Esc there closes the whole card)
      move: e => { if (e && e.target && S.pop.contains(e.target)) return; queuePlace(); },
      iv: setInterval(() => { if (!S.btn || !S.btn.isConnected || !S.btn.getClientRects().length) { if (++S.gone >= 3) close(false); } else S.gone = 0; }, 250)
    };
    d.addEventListener('pointerdown', w.down, true); d.addEventListener('keydown', w.key, true);
    root.addEventListener('resize', w.move); root.addEventListener('scroll', w.move, true);
    if (root.visualViewport) { root.visualViewport.addEventListener('resize', w.move); root.visualViewport.addEventListener('scroll', w.move); }
  }
  function open(key, ta, btn) {
    const d = D(); if (!d || !d.groups) return;
    if (S.open) close(false);
    S.key = key; S.ta = ta; S.btn = btn; S.sel = { s: ta.selectionStart || 0, e: ta.selectionEnd || 0 };
    build(); index(); S.q = ''; S.el.input.value = ''; S.recent = loadRecent(); S.act = -1; S.gone = 0;
    S.open = true; S.pop.hidden = false;
    if (S.fontState === 'failed') S.fontState = 'idle';   // (a font that did not load is asked for again each time the picker opens)
    btn.setAttribute('aria-expanded', 'true'); btn.classList.add('on');
    S.stripItem = null; S.stripFor = ''; clearTimeout(S.stripTimer);
    place(); measure(); layout(); S.el.grid.scrollTop = 0; paint(); tabsOn(); paintTabs(); foot();
    loading(S.fontState === 'loading');
    watch(true);
    ensureFont();
    (coarse() ? S.el.grid : S.el.input).focus({ preventScroll: true });
  }
  function close(refocus) {
    if (!S.open) return;
    S.open = false; S.pop.hidden = true; watch(false);
    if (S.btn) { S.btn.setAttribute('aria-expanded', 'false'); S.btn.classList.remove('on'); }
    const ta = S.ta;
    if (refocus && ta && ta.isConnected) { try { ta.focus({ preventScroll: true }); const n = ta.value.length; ta.setSelectionRange(Math.min(S.sel.s, n), Math.min(S.sel.e, n)); } catch (_) {} }
  }
  function rebind(ta, btn) {   // the card was rebuilt behind an open picker: same words box, new elements
    S.ta = ta; S.btn = btn; S.gone = 0;
    try { const n = ta.value.length; if (doc().activeElement !== ta) ta.setSelectionRange(Math.min(S.sel.s, n), Math.min(S.sel.e, n)); } catch (_) {}
    btn.setAttribute('aria-expanded', 'true'); btn.classList.add('on');
    queuePlace();   // (the new card is not in the page yet: its button has no place until the caller has put the card there)
  }

  /** Wire one words box. textarea: the words; button: the "Emoji" button (already in the card); key: names this words box, so the
   *  picker that is open follows it when the card is rebuilt. Returns false (and takes the button away) when there is no emoji data. */
  function attach(o) {
    const ta = o && o.textarea, btn = o && o.button;
    if (!ta || !btn) return false;
    if (!D() || !(D().groups || []).length) { try { btn.remove(); } catch (_) {} return false; }
    if (o.fonts) S.provider = o.fonts;
    const key = String(o.key == null ? '' : o.key);
    btn.setAttribute('aria-haspopup', 'dialog'); btn.setAttribute('aria-expanded', 'false');
    btn.addEventListener('click', e => { e.preventDefault(); e.stopPropagation(); if (S.open && S.btn === btn) close(true); else open(key, ta, btn); });
    const mark = () => { if (S.ta === ta) S.sel = { s: ta.selectionStart, e: ta.selectionEnd }; };
    ['keyup', 'mouseup', 'select', 'input', 'blur', 'touchend'].forEach(ev => ta.addEventListener(ev, mark));
    if (S.open) { if (S.key === key) rebind(ta, btn); else close(false); }
    return true;
  }

  root.CNEmojiPicker = { attach, close: refocus => close(!!refocus), isOpen: () => S.open, insert, setFonts: f => { S.provider = f; }, state: () => ({ open: S.open, q: S.q, font: S.fontState, cells: S.flat.length, recent: S.recent.slice(), cols: S.cols }) };
})(typeof window !== 'undefined' ? window : this);
