/* charm-nest-ce-text.js — THE CHARM EDITOR'S WORDS IN EVERY FONT, SCRIPT AND EMOJI THE BACK ENGRAVING HAS (Paul, 28 Oct 2026: "Users also need to be able to add any emoji and
 * any language that we already support as part of the back engraving."; the lead's D9: emoji are engraved as monochrome OUTLINES, a laser engraves outlines and fills, not colours).
 *
 * The back engraving already has all of it (charm-nest-bridge.js, Engrave): the shop's engraving fonts (charm-nest-orders.js ENGRAVING_FONTS, each loaded once by loadFontSet), every one
 * wrapped by charm-nest-text.js withEmoji, so a character is set, one cluster at a time, in the first of: the engraving font itself → Source Sans 3 and DejaVu Sans (a "ñ" a script face
 * lacks, ♡ ☆ ✓) → the Noto Sans font of its script (vendor/fonts/script-fonts.json; shaped by HarfBuzz, charm-nest-shape.js: Arabic joining, Indic conjuncts, marks, right to left by
 * UAX #9) → Noto Emoji (vendor/fonts/NotoEmoji-Regular.ttf, SIL Open Font License, its licence beside it: a MONOCHROME OUTLINE font, so an emoji is a filled vector shape the laser
 * hatches, never a colour picture). This file gives the editor's Text tool exactly that and nothing of its own (no second font file, no second loader, no second picker):
 *
 *   CharmNestCeText.fonts(job?)                -> Promise<[{ key, name, family, kind, bold, caps }]>  every engraving font, the piece's own first, then the shop's words for its fonts ("Stylish", "Ace" ...), then
 *                                                 the other installed fonts by name (a saved back may name them); each one loaded. (charm-nest-bridge.js charmEditFonts hands over to it)
 *   CharmNestCeText.textToPaths(text, fontKey, sizePt, { align, lineGap, bold, italic, smallCaps })    the editor's font engine (CharmEditor opts.textToPaths; the bridge's charmEditText hands over to it):
 *                                                 filled outlines in pt, y down, every curve a cubic, the anchor at 0,0. The anchor is the START of the first baseline, and the start of
 *                                                 words that read right to left (their first letter Hebrew, Arabic ...) is their RIGHT end, so they grow leftwards from where they were
 *                                                 put, as Illustrator's Middle Eastern composer does. Synchronous: a script font, the emoji outlines or the engraving font the words need
 *                                                 that is not loaded yet is asked for, the words are drawn without it meanwhile, and again when it comes (CharmEditor.current.refreshText)
 *   CharmNestCeText.prepare({ job, faces })    -> Promise<{ fonts, textToPaths }>  for any caller of CharmEditor.open (a master, a saved design): the fonts, and the script fonts the
 *                                                 words already in `faces` need, loaded before the editor opens, so a text opened again is drawn right at once
 *   CharmNestCeText.check(text, fontKey)       -> { missing: [clusters nothing can engrave], scripts: [{ id, name }], loading }  what the line under the words box says
 *   CharmEditor.open without a font engine of its own gets this one (and the fonts already loaded): no caller engraves the editor's stand-in font by mistake.
 * (CETEXT, Paul 28 Oct 2026: "I also don't see a way to change the text style and font style and alphabet style inside the charm editor as I had requested") the engine draws a text's style
 * as the editor's bar sets it: bold (the family's Semibold at any size; a family with one weight has none, and fonts() says so: bold false), italic (the outlines slanted 12° about their
 * baselines: no engraving font has an italic file) and small capitals (a lowercase letter as a capital at the font's own x-height); the other letter cases are the editor's own (the words
 * in capitals, lowercase or Title Case). fonts() names each font's CSS family (CharmNestEngravePreview's FontFace of the very file it engraves with) so the Font menu draws every name in its
 * own font, as the engraving card's drop-down does, and its kind (cursive, script, handwriting ...: charm-nest-orders.js FONT_STYLES).
 *
 * In the editor, while words are typed (the words box, the editor's one textarea): an Emoji button in the box (the back engraving's own picker, charm-nest-emoji-picker.js: search by
 * name, categories, recently used, each cell drawn as it will be engraved), the box reads right to left when the words do (dir=auto), and a line under it says what is being loaded,
 * which script font the words are set in and what cannot be engraved at all. An emoji typed or pasted is the same as one picked. The words stay words (words, font, size, weight can
 * change again); they become outlines when the editor hands its result back, as before.
 * No Firestore read or write, no poll, no timer: static font files, fetched on demand by the engraving's own loaders (from the browser cache after the first time). */
(function (root) {
  'use strict';
  const MM = 25.4 / 72, SEMI_MM = 2.2;   // (words whose capital is under 2.2 mm are cut in the Semibold, as the fit cuts them: charm-nest-engrave-fit.js semiboldBelowMm, the bridge's CHARM_EDIT_SEMI_MM)
  const E = () => root.Engrave || null, O = () => root.CharmNestOrders || null, G = () => root.CharmNestGeom || null, T = () => root.CharmNestText || null;
  const doc = () => root.document || null;
  // a letter of a script read right to left (Hebrew, Arabic, Syriac, Thaana, NKo ... and their presentation forms): the first strong letter of the words decides their direction (UAX #9 P2)
  const RTL_LETTER = /[֐-ࣿיִ-﷿ﹰ-﻿\u{10800}-\u{10FFF}\u{1E800}-\u{1EFFF}]/u, LETTER = /\p{L}/u;
  const PICTO = /\p{Extended_Pictographic}|\p{Regional_Indicator}/u;
  const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const graphemes = s => { try { return [...new Intl.Segmenter(undefined, { granularity: 'grapheme' }).segment(String(s))].map(x => x.segment); } catch (_) { return [...String(s)]; } };

  /** Do these words read right to left: is their first strong letter one of a right-to-left script. */
  function baseRtl(text) {
    for (const ch of String(text == null ? '' : text)) { if (!LETTER.test(ch)) continue; return RTL_LETTER.test(ch); }
    return false;
  }

  /* ═══ the fonts ═══ */
  /** The loaded set of a font id (Engrave's own): Source Sans 3 for the default, null for a font not loaded (yet). */
  const setOf = id => { const En = E(); if (!En || typeof En.fontSetFor !== 'function') return null; const s = En.fontSetFor(id); return s && s.Regular ? s : null; };
  /** Every engraving font as the editor lists it: the piece's own first, the shop's words next (its drop-down's order), then the other installed fonts by their names. */
  function fontIds(job) {
    const Or = O(); if (!Or) return [];
    return [...new Set([job && job.fontKey, ...(Or.FONT_WORDS || []).map(g => g.id), Or.DEFAULT_FONT, ...(Or.ENGRAVING_FONTS || []).map(f => f.id)].filter(Boolean))];
  }
  const fontName = id => { const Or = O(), g = Or && (Or.FONT_WORDS || []).find(x => x.id === id); return (g && g.words && g.words[0]) || ((Or && Or.fontById && Or.fontById(id)) || {}).name || String(id); };
  /* (CETEXT, Paul 28 Oct 2026: "I also don't see a way to change the text style and font style and alphabet style inside the charm editor as I had requested"): each font as the editor's Font menu
     draws it: its name in the font itself (the engraving card's drop-down does the same: CharmNestEngravePreview's FontFace of the very file the engraving fits with), its kind (the first style of
     charm-nest-orders.js FONT_STYLES that prefers it: cursive, script, handwriting ...), whether its family has a heavier cut (Bold) and whether it is capitals only (Ace). */
  const Pv = () => root.CharmNestEngravePreview || null;
  function fontEntry(id) {
    const Or = O(), P = Pv(), s = setOf(id), out = { key: id, name: fontName(id) };
    if (P && typeof P.fontFamilyOf === 'function') { try { if (typeof P.ensure === 'function') Promise.resolve(P.ensure(id)).catch(() => null); out.family = P.fontFamilyOf(id); } catch (_) { /* its name in the page's own font */ } }
    const row = Or && Array.isArray(Or.FONT_STYLES) ? Or.FONT_STYLES.find(r => Array.isArray(r.prefer) && r.prefer[0] === id && r.label) : null;
    if (row) out.kind = row.label;
    // (BOLDALL, Paul 28 Oct 2026: "The Bold option has no effect on the text, also some fonts don't have the option to be bolded": every font has Bold now: its heavier cut where that
    //  is not already the one engraved, else its own letters grown (textToPaths below). Only a page without the editor's offsets (CharmEditorOps) has none for a one-weight family.)
    if (s) out.bold = !!s.Semibold || canGrow();
    if (Or && Or.fontCaps && Or.fontCaps(id)) out.caps = true;
    return out;
  }
  /** The fonts the editor offers, each loaded (a font whose files cannot be had is left out, as the engraving's own list leaves it). */
  async function fonts(job) {
    const En = E(), ids = fontIds(job); if (!En || !ids.length) return [];
    await Promise.all(ids.map(id => Promise.resolve().then(() => En.loadFontSet(id)).catch(() => null)));
    return ids.filter(id => !!setOf(id)).map(fontEntry);
  }
  /** The fonts already loaded, at once (for a caller that gave the editor none). */
  const loadedFonts = () => fontIds(null).filter(id => !!setOf(id)).map(fontEntry);

  /* ═══ what is asked for while the words are typed: a script font, the emoji outlines, an engraving font. One ask at a time of each kind, the same words not asked again for a minute
     (the loaders keep their own once-a-minute rule for a file that failed); when something came, every text in the editor is drawn again ═══ */
  const inflight = new Map(), tried = new Map(), askedFonts = new Set();
  let lastSig = '';
  const fontSig = () => { const En = E(), Tx = T(); return [Tx && Tx.sharedScripts ? Tx.sharedScripts.version : 0, En && En.fonts && En.fonts.emoji ? 1 : 0, [...askedFonts].filter(k => !!setOf(k)).length].join('|'); };
  function want(kind, key, start, still) {
    if (inflight.has(kind)) return;
    const t = tried.get(key); if (t && Date.now() - t < 60000) return;
    tried.set(key, Date.now()); if (tried.size > 200) tried.delete(tried.keys().next().value);
    if (!lastSig) lastSig = fontSig();
    const p = Promise.resolve().then(start).catch(() => null).then(() => { inflight.delete(kind); arrived(); if (typeof still === 'function' && tryDo(still)) again(key); });
    inflight.set(kind, p); UI.note();
  }
  const redraw = () => { const cur = root.CharmEditor && root.CharmEditor.current; if (cur && typeof cur.refreshText === 'function') { try { cur.refreshText(); } catch (e) { console.warn('CE text: drawn again', e); } } };
  const tryDo = f => { try { return f(); } catch (_) { return false; } };
  /** (SCRIPTSLOW) After an ask: are some of these clusters still not in the font (its script fonts with it)? */
  const stillMissing = (font, joined) => () => { const Gm = G(); return !!(Gm && Gm.glyphCoverage(font, joined).missing.some(c => !/^\s+$/.test(c))); };
  /** Something asked for is back: when it changed what the fonts can draw, every text in the open editor is made again from its words. → whether it changed. */
  function arrived() {
    const sig = fontSig(), changed = sig !== lastSig;
    if (changed) { lastSig = sig; redraw(); }
    UI.note();
    return changed;
  }
  /* (SCRIPTSLOW, Paul 28 Oct 2026: "it's not letting me add any other language/alphabet fonts to the charm": one script font that did not come (a slow connection) left the words "not
     engraved" for good, as the editor draws the same words from its cache and never asked again. An ask that brought nothing is asked again by itself, three times (after 3, 10 and 30 s,
     the engraving's own memory of the failed files cleared first); after that the line under the words box offers "Try again", and the network coming back asks at once.) */
  const RETRY_MS = [3000, 10000, 30000], attempts = new Map();
  let againTimer = 0;
  function again(key) {
    const n = attempts.get(key) || 0; if (n >= RETRY_MS.length) { UI.note(); return; }
    attempts.set(key, n + 1); clearTimeout(againTimer);
    againTimer = setTimeout(() => retry(key), RETRY_MS[n]);
  }
  /** The words' fonts asked for again now: the failed files forgotten (here and in the engraving's loaders) and every text drawn again, so what is still missing is asked for. */
  function retry(key) {
    clearTimeout(againTimer); againTimer = 0;
    if (key) tried.delete(key); else { tried.clear(); attempts.clear(); }
    const En = E(); if (En && typeof En.retryScripts === 'function') { try { En.retryScripts(); } catch (_) { /* (asked again by its own minute rule) */ } }
    if (En && En.fonts && En.fonts.emojiError && typeof En.loadFonts === 'function') Promise.resolve().then(() => En.loadFonts(true)).catch(() => null);
    redraw(); UI.note();
  }
  if (root.addEventListener) root.addEventListener('online', () => retry());
  /** Ask for what `missing` (clusters the font cannot set now) needs: the script fonts, the emoji outlines. */
  function askFor(text, missing, font) {
    const En = E(), Tx = T(); if (!En || !missing.length) return;
    const joined = missing.join('');
    if (Tx && typeof Tx.scriptHint === 'function' && Tx.scriptHint(joined) && typeof En.ensureScriptFonts === 'function')
      want('scripts', 'scripts:' + [...new Set(missing)].sort().join(''), () => En.ensureScriptFonts(String(text).split(/\r?\n/), font), stillMissing(font, joined));
    if (PICTO.test(joined) && En.fonts && !En.fonts.emoji && typeof En.loadFonts === 'function') want('emoji', 'emoji', () => En.loadFonts(true), () => !(En.fonts && En.fonts.emoji));
  }

  /* ═══ the font engine ═══ */
  /** opentype Path commands (M L Q C Z; y down) as editor subpaths: every curve a cubic (a TrueType quadratic raised exactly), every subpath starting with its move. */
  function toSubpaths(cmds, out) {
    const ok = v => Number.isFinite(v);
    let sp = null, cur = null, start = null;
    const open = () => { if (!sp && start) { sp = [['m', start.slice()]]; out.push(sp); } return !!sp; };
    for (const c of cmds || []) {
      if (c.type === 'M') { if (!ok(c.x) || !ok(c.y)) continue; sp = [['m', [c.x, c.y]]]; out.push(sp); cur = [c.x, c.y]; start = cur.slice(); }
      else if (c.type === 'Z') { if (sp) sp.push(['h']); sp = null; cur = start; }
      else if (!open() || !cur || !ok(c.x) || !ok(c.y)) continue;
      else if (c.type === 'L') { sp.push(['l', [c.x, c.y]]); cur = [c.x, c.y]; }
      else if (c.type === 'C') { sp.push(['c', [c.x1, c.y1], [c.x2, c.y2], [c.x, c.y]]); cur = [c.x, c.y]; }
      else if (c.type === 'Q') { sp.push(['c', [cur[0] + 2 / 3 * (c.x1 - cur[0]), cur[1] + 2 / 3 * (c.y1 - cur[1])], [c.x + 2 / 3 * (c.x1 - c.x), c.y + 2 / 3 * (c.y1 - c.y)], [c.x, c.y]]); cur = [c.x, c.y]; }
    }
    return out;
  }
  /** The face the words are cut in: the set of `fontKey` (Source Sans 3 while it is not loaded: asked for, and the words drawn again when it comes), its Semibold for a small capital. */
  function faceFor(fontKey, size, bold) {
    const En = E(), Gm = G(), Or = O(); if (!En || !Gm) return null;
    const f = cutFor(fontKey, size, bold); if (!f) return null;
    // (BOLDALL: Bold is the heavier cut when the words would otherwise be in the regular; when they are already in the heavier one (a small capital) or the family has one weight, the letters
    //  themselves are grown: `grow`, the share of the size each outline grows by on every side)
    if (bold && (!f.set.Semibold || f.small) && canGrow()) f.grow = BOLD_GROW;
    return f;
  }
  const BOLD_GROW = 0.022;   // (each side, a share of the type size: about a third of a regular stroke, the weight Illustrator's faux bold adds)
  const ops = () => (root.CharmEditorOps && root.CharmEditorOps.kernel && typeof root.CharmEditorOps.kernel.offsetPath === 'function' ? root.CharmEditorOps.kernel : null);
  const canGrow = () => !!ops();
  function cutFor(fontKey, size, bold) {
    const En = E(), Gm = G(), Or = O(); if (!En || !Gm) return null;
    let set = setOf(fontKey);
    if (!set && fontKey && Or && Or.fontById && Or.fontById(fontKey)) { askedFonts.add(String(fontKey)); want('font:' + fontKey, 'font:' + fontKey, () => En.loadFontSet(fontKey)); }
    if (!set) set = En.fonts && En.fonts.Regular ? En.fonts : null;
    if (!set) return null;
    const small = size * Gm.capPerEm(set.Regular) * MM < SEMI_MM;
    const font = ((bold || small) && set.Semibold) || set.Regular;   // (CETEXT: Bold is the family's heavier cut at any size)
    return { set, font, small: small && !!set.Semibold, id: set === En.fonts ? (Or && Or.DEFAULT_FONT) : set.id };
  }
  /** Words as filled outlines in an engraving font (the editor's textToPaths). Lines are a capital and the fit's own gap apart unless the editor gives a gap (a factor of the size).
   *  A cluster nothing can set yet is left out of the outlines (and asked for); a cluster nothing can set at all is left out and said under the words box. */
  function textToPaths(text, fontKey, sizePt, o) {
    o = o || {};
    const size = +sizePt > 0 ? +sizePt : 10, f = faceFor(fontKey, size, !!o.bold), Gm = G(), Or = O(), CE = root.CharmEditor;
    if (!f) return { subpaths: [], bbox: null };
    let words = String(text == null ? '' : text); if (Or && Or.fontCaps && Or.fontCaps(f.id)) words = Or.capsText(words, f.id);
    const font = f.font, lead = Number.isFinite(+o.lineGap) && +o.lineGap > 0 ? size * +o.lineGap : size * (Gm.capPerEm(font) + 0.216);
    // the start of right-to-left words is their right end: 'left' (the start) and 'right' (the end) change sides, a centre stays the centre
    const rtl = baseRtl(words), al = o.align || 'left', side = rtl ? ({ left: 'right', right: 'left' }[al] || al) : al;
    const subpaths = [], gone = [];
    // (CETEXT, Paul 28 Oct 2026, "alphabet style": SMALL CAPITALS, a lowercase letter drawn as a capital at the height of the font's own small letters; words that read right to left have no small letters)
    const sc = o.smallCaps && !rtl ? smallCapScale(font) : 0;
    words.split(/\r?\n/).forEach((line, i) => {
      try { const miss = Gm.glyphCoverage(font, line).missing; for (const m of miss) { gone.push(m); line = line.split(m).join(''); } } catch (_) { /* drawn as it is */ }
      if (!line.trim()) return;
      let cmds;
      try {
        if (sc && LOWER.test(line)) {
          const runs = []; for (const g of graphemes(line)) { const low = LOWER.test(g) && g.toLocaleUpperCase() !== g, last = runs[runs.length - 1]; if (last && last.low === low) last.t += g; else runs.push({ low, t: g }); }
          const parts = runs.map(r => ({ t: r.low ? r.t.toLocaleUpperCase() : r.t, s: r.low ? size * sc : size })), w = parts.reduce((a, p) => a + font.getAdvanceWidth(p.t, p.s), 0);
          let x = side === 'center' ? -w / 2 : side === 'right' ? -w : 0; cmds = [];
          for (const p of parts) { for (const c of font.getPath(p.t, x, i * lead, p.s).commands) cmds.push(c); x += font.getAdvanceWidth(p.t, p.s); }
        } else { const w = font.getAdvanceWidth(line, size), x = side === 'center' ? -w / 2 : side === 'right' ? -w : 0; cmds = font.getPath(line, x, i * lead, size).commands; }
      } catch (_) { return; }
      toSubpaths(cmds, subpaths);
    });
    if (gone.length) askFor(words, gone, f.set.Regular);
    let subs = subpaths.filter(s => s.some(op => op[0] === 'l' || op[0] === 'c'));
    // (BOLDALL: the letters grown by `grow` of the size on every side, their joins mitred so a serif and a corner stay sharp; the counters (the holes of a, e, o) shrink with them)
    if (f.grow && subs.length) { try { const g = ops().offsetPath({ subpaths: subs, evenOdd: false }, size * f.grow, 'miter', 2, false, false); if (g && g.length) subs = g; } catch (e) { console.warn('[ce-text] bold', e && e.message); } }
    // (CETEXT: ITALIC, the letters slanted 12° to the right about their baselines (y down: up is -y), as outlines: none of the engraving fonts has an italic of its own)
    if (o.italic && subs.length) { const k = Math.tan(ITALIC_DEG * Math.PI / 180), sl = p => [p[0] - p[1] * k, p[1]]; subs = subs.map(sp => sp.map(op => (op[0] === 'h' ? op : [op[0]].concat(op.slice(1).map(sl))))); }
    return { subpaths: subs, bbox: subs.length && CE && CE.model ? CE.model.bboxOf(subs) : null };
  }
  const ITALIC_DEG = 12, LOWER = /\p{Ll}/u;
  /** The size of a small capital against a capital: the font's own x-height over its cap height (OS/2), 0.72 when the font does not say; held to 0.6 … 0.85. */
  function smallCapScale(font) {
    const t = font && font.tables && font.tables.os2, x = t && +t.sxHeight, c = t && +t.sCapHeight;
    return x > 0 && c > 0 ? Math.min(0.85, Math.max(0.6, x / c)) : 0.72;
  }
  textToPaths.engine = 'engrave';   // (how a caller tells this engine from the editor's stand-in)

  /** What the line under the words box says of `text` in `fontKey`: the clusters no font can engrave (none while a font for them is still coming), the script fonts the words are set in. */
  function check(text, fontKey) {
    const Gm = G(), Or = O(), f = faceFor(fontKey, 10), out = { missing: [], scripts: [], loading: inflight.size > 0 };
    if (!f || !Gm) return out;
    let words = String(text == null ? '' : text); if (Or && Or.fontCaps && Or.fontCaps(f.id)) words = Or.capsText(words, f.id);
    try { out.missing = Gm.glyphCoverage(f.set.Regular, words).missing.filter(c => !/^\s+$/.test(c)); } catch (_) { out.missing = []; }
    try { out.scripts = typeof f.set.Regular.scriptFonts === 'function' ? f.set.Regular.scriptFonts(words) : []; } catch (_) { out.scripts = []; }
    if (out.loading) { const Tx = T(); out.missing = out.missing.filter(c => !(inflight.has('scripts') && Tx && Tx.scriptHint(c)) && !(inflight.has('emoji') && PICTO.test(c))); }
    return out;
  }

  /** For any caller of CharmEditor.open: the fonts, and the script fonts the words already in the faces need (a text of a saved design opened again), loaded first. */
  async function prepare(opts) {
    opts = opts || {};
    const list = await fonts(opts.job || null), En = E(), texts = [];
    const walk = items => (items || []).forEach(it => { if (!it) return; if (it.kind === 'text' && it.text) texts.push(it); if (it.children) walk(it.children); });
    for (const fd of Object.values(opts.faces || {})) walk(fd && fd.items);
    const Tx = T();
    if (En && typeof En.ensureScriptFonts === 'function' && Tx) await Promise.all(texts.filter(t => Tx.scriptHint(t.text)).map(t => { const s = setOf(t.fontKey) || En.fonts; return En.ensureScriptFonts(String(t.text).split(/\r?\n/), s && s.Regular).catch(() => null); }));
    if (texts.some(t => PICTO.test(t.text)) && En && En.fonts && !En.fonts.emoji && En.loadFonts) await En.loadFonts(true).catch(() => null);
    lastSig = fontSig();
    return { fonts: list, textToPaths };
  }

  /* ═══ the words box in the editor: its Emoji button, its direction, the line under it ═══ */
  const SMILE = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><circle cx="12" cy="12" r="8.5"/><path d="M8.4 13.9c.9 1.5 2.1 2.2 3.6 2.2s2.7-.7 3.6-2.2"/><path d="M9.3 9.4v.9M14.7 9.4v.9"/></svg>';   // (the studio's icon hand: a 24-unit box, 1.6 strokes, round ends)
  function css() {
    const d = doc(); if (!d || d.getElementById('ceTextCss')) return;
    const s = d.createElement('style'); s.id = 'ceTextCss';
    s.textContent = 'html.ce-text-on .emPop{z-index:2147482600}'   // (the one emoji picker of the page, over the editor while it is open)
      + '.ce-tx-emoji{position:absolute;z-index:7;width:28px;height:28px;display:flex;align-items:center;justify-content:center;padding:0;border:0;border-left:1px solid var(--hair,#e6e4e0);background:#fff;color:var(--muted,#6d6a66);cursor:pointer;transition:color var(--in,.11s),background var(--in,.11s)}'
      + '.ce-tx-emoji svg{width:17px;height:17px}.ce-tx-emoji:hover,.ce-tx-emoji:focus-visible,.ce-tx-emoji.on{color:var(--ink,#1c1d1d);background:var(--hov,#f2eee4)}'
      + '.ce-tx-emoji.ce-tx-alpha{font:600 13px/1 "Noto Sans",system-ui,sans-serif;letter-spacing:-.02em}'   // (ALPHAFIND: the other alphabets' button, beside Emoji)
      + '.ce-tx-note{position:absolute;z-index:6;pointer-events:none;max-width:300px;padding:4px 9px;background:rgba(255,255,255,.97);border:1px solid var(--hair,#e6e4e0);font:600 10px/1.45 var(--sans,sans-serif);letter-spacing:.05em;color:var(--muted,#6d6a66)}'
      + '.ce-tx-note[hidden]{display:none}.ce-tx-note .ce-tx-retry{pointer-events:auto;margin-left:6px;padding:1px 7px;border:1px solid currentColor;background:#fff;color:inherit;font:inherit;letter-spacing:inherit;cursor:pointer}.ce-tx-note .ce-tx-retry:hover,.ce-tx-note .ce-tx-retry:focus-visible{background:#f7ebe5}'
      + '.ce-tx-note b{color:var(--ink,#1c1d1d);font-weight:700}.ce-tx-note.is-bad{color:#8a3a26;border-color:#e2c4b8}.ce-tx-note .ce-tx-spin{display:inline-block;width:8px;height:8px;margin-right:6px;border:1.5px solid var(--gold-lt,#cdb98a);border-top-color:var(--gold,#a58a52);border-radius:50%;animation:ceTxSpin .8s linear infinite;vertical-align:-1px}'
      + '@keyframes ceTxSpin{to{transform:rotate(360deg)}}@media (prefers-reduced-motion: reduce){.ce-tx-note .ce-tx-spin{animation:none}}';
    (d.head || d.documentElement).appendChild(s);
  }
  const UI = (() => {
    let L = null;   // { rootEl, stage, mo, ta, btn, note, taMo, onInput }
    const edited = () => { const cur = root.CharmEditor && root.CharmEditor.current, id = cur && cur.editing && cur.editing(); if (!id) return null; let hit = null; const walk = l => (l || []).forEach(i => { if (hit || !i) return; if (i.id === id) hit = i; else walk(i.children); }); walk(cur.items()); return hit; };
    function place() {
      if (!L || !L.ta) return;
      const ta = L.ta, x = ta.offsetLeft, y = ta.offsetTop, w = ta.offsetWidth, h = ta.offsetHeight;
      if (L.btn) { L.btn.style.left = (x + w - 29) + 'px'; L.btn.style.top = (y + 1) + 'px'; L.btn.style.height = Math.min(28, h - 2) + 'px'; }
      if (L.abtn) { L.abtn.style.left = (x + w - 29 - (L.btn ? 29 : 0)) + 'px'; L.abtn.style.top = (y + 1) + 'px'; L.abtn.style.height = Math.min(28, h - 2) + 'px'; }
      if (L.note && !L.note.hidden) {
        const nh = L.note.offsetHeight || 22, sh = L.stage.clientHeight, below = y + h + 4;
        L.note.style.left = x + 'px'; L.note.style.top = (below + nh <= sh - 4 ? below : Math.max(4, y - nh - 4)) + 'px';
      }
    }
    /** The line under the words box: what is coming, the script font the words are set in, what cannot be engraved. */
    function note() {
      if (!L || !L.ta || !L.note) return;
      const it = edited(), r = check(L.ta.value, it ? it.fontKey : null), parts = [];
      let bad = false;
      if (r.loading) parts.push('<span class="ce-tx-spin" aria-hidden="true"></span>' + (inflight.has('emoji') ? 'Loading the emoji outlines…' : inflight.has('scripts') ? 'Loading the letters of these words…' : 'Loading the font…'));
      if (r.scripts.length) parts.push('Set in <b>' + r.scripts.map(s => esc(s.name)).join('</b> + <b>') + '</b>');
      // (a letter of a script whose font could not be had, an emoji Noto Emoji has no outline for, a sign no font draws: left out of the outlines, said here, never engraved as a box)
      if (r.missing.length) {
        bad = true;
        const Tx = T(), wait = !!againTimer && r.missing.some(c => (Tx && Tx.scriptHint(c)) || PICTO.test(c));
        parts.push('Not engraved: ' + r.missing.slice(0, 6).map(esc).join(' ') + (r.missing.length > 6 ? ' …' : '') + (wait ? ' · their font did not come yet, asked again in a moment' : ' · no engraving font for ' + (r.missing.length > 1 ? 'them' : 'it') + ' could be had')
          + ' <button type="button" class="ce-tx-retry" data-ce-keeps-words>Try again</button>');
      }
      L.note.innerHTML = parts.join(' · '); L.note.hidden = !parts.length; L.note.classList.toggle('is-bad', bad);
      place();
    }
    /** The words box came (or went): wired, or let go. */
    function sync() {
      if (!L) return;
      const ta = L.stage.querySelector('textarea.ce-editor');
      if (ta === L.ta) return;
      if (L.ta) unwire();
      if (ta) wire(ta);
    }
    function wire(ta) {
      const d = doc(); L.ta = ta;
      ta.dir = 'auto';                        // (Hebrew and Arabic read right to left in the box as they will on the charm)
      ta.style.paddingRight = '64px';         // (room for the Emoji and the other alphabets' buttons inside the box)
      const btn = d.createElement('button'); btn.type = 'button'; btn.className = 'ce-tx-emoji'; btn.setAttribute('data-ce-keeps-words', '');
      btn.title = 'Emoji · every one the laser can engrave, as a one-colour outline (search by name; typed or pasted emoji work too)'; btn.setAttribute('aria-label', 'Emoji'); btn.innerHTML = SMILE;
      // (the box keeps its caret, and a press on the button is no press on the charm: the stage must not place words or select under it)
      btn.addEventListener('pointerdown', e => { e.preventDefault(); e.stopPropagation(); });
      for (const t of ['pointerup', 'dblclick', 'contextmenu', 'wheel']) btn.addEventListener(t, e => e.stopPropagation());
      const line = d.createElement('div'); line.className = 'ce-tx-note'; line.hidden = true; line.setAttribute('role', 'status'); line.setAttribute('aria-live', 'polite');
      // (SCRIPTSLOW: its "Try again" asks for the words' fonts at once; a press on it is no press on the charm and keeps the words box open)
      line.addEventListener('pointerdown', e => { if (e.target.closest('.ce-tx-retry')) { e.preventDefault(); e.stopPropagation(); } });
      line.addEventListener('click', e => { if (e.target.closest('.ce-tx-retry')) { e.preventDefault(); e.stopPropagation(); retry(); } });
      L.stage.appendChild(btn); L.stage.appendChild(line); L.btn = btn; L.note = line;
      const P = root.CNEmojiPicker;
      if (!(P && P.attach({ textarea: ta, button: btn, key: 'ce:' + ((root.CharmEditor && root.CharmEditor.current && root.CharmEditor.current.editing()) || '') }))) { btn.remove(); L.btn = null; ta.style.paddingRight = ''; }
      // (ALPHAFIND, Paul 28 Oct 2026: "the option to choose a alphabet/language letters/characters ... searchable for equivalent english letters and words": the letters, words and phrases of
      //  other alphabets found by their English, charm-nest-alphabet-picker.js, put in the words as typing puts them)
      { const ab = d.createElement('button'); ab.type = 'button'; ab.className = 'ce-tx-emoji ce-tx-alpha'; ab.setAttribute('data-ce-keeps-words', ''); ab.textContent = 'Aα';
        ab.title = 'Other alphabets · letters, words and phrases in Greek, Hebrew, Arabic, Chinese, Japanese, Korean, Latin and more, found by their English'; ab.setAttribute('aria-label', 'Other alphabets');
        ab.addEventListener('pointerdown', e => { e.preventDefault(); e.stopPropagation(); });
        for (const t of ['pointerup', 'dblclick', 'contextmenu', 'wheel']) ab.addEventListener(t, e => e.stopPropagation());
        const AP = root.CNAlphabetPicker;
        if (AP && AP.attach({ textarea: ta, button: ab, key: 'ce-alpha:' + ((root.CharmEditor && root.CharmEditor.current && root.CharmEditor.current.editing()) || '') })) { L.stage.appendChild(ab); L.abtn = ab; } }
      L.onInput = () => note();
      ta.addEventListener('input', L.onInput);
      if (root.MutationObserver) { L.taMo = new root.MutationObserver(place); L.taMo.observe(ta, { attributes: true, attributeFilter: ['style'] }); }
      note(); place();
    }
    function unwire() {
      if (!L || !L.ta) return;
      const P = root.CNEmojiPicker; try { if (P && P.isOpen && P.isOpen()) P.close(false); } catch (_) { /* fine */ }
      const AP = root.CNAlphabetPicker; try { if (AP && AP.isOpen && AP.isOpen()) AP.close(false); } catch (_) { /* fine */ }
      if (L.taMo) L.taMo.disconnect();
      L.ta.removeEventListener('input', L.onInput);
      if (L.btn) L.btn.remove(); if (L.abtn) L.abtn.remove(); if (L.note) L.note.remove();
      L.ta = L.btn = L.abtn = L.note = L.taMo = L.onInput = null;
    }
    /** The editor opened (CharmEditor.onOpen): its work area watched for the words box. Once per workspace. */
    function attach(ctx) {
      const d = doc(), rootEl = (ctx && ctx.root) || (d && d.querySelector('.ce-root')); if (!rootEl || !rootEl.isConnected || (L && L.rootEl === rootEl)) return;
      if (L) detach();
      const stage = (ctx && ctx.stage) || rootEl.querySelector('.ce-stage'); if (!stage) return;
      css(); d.documentElement.classList.add('ce-text-on');
      L = { rootEl, stage, mo: null, ta: null };
      if (root.MutationObserver) { L.mo = new root.MutationObserver(sync); L.mo.observe(stage, { childList: true }); }
      sync();
    }
    function detach() {
      if (!L) return;
      unwire(); if (L.mo) L.mo.disconnect(); L = null;
      const d = doc(); if (d) d.documentElement.classList.remove('ce-text-on');
    }
    return { attach, detach, note, sync, state: () => (L ? { ta: !!L.ta, btn: !!L.btn, note: L.note && !L.note.hidden ? L.note.textContent : '' } : null) };
  })();

  /** What the editor handed back, for the employee log (Paul, 28 Oct 2026: "all of this must be tracked by employee"): how many emoji its words carry and which script fonts set them. */
  function logResult(res) {
    if (!res || !res.faces || !root.CNDesignLog) return;
    let emoji = 0; const scripts = new Set(), texts = [];
    const walk = items => (items || []).forEach(it => { if (!it) return; if (it.kind === 'text' && it.text) texts.push(it); if (it.children) walk(it.children); });
    for (const fd of Object.values(res.faces)) walk(fd && fd.items);
    for (const t of texts) { for (const g of graphemes(t.text)) if (PICTO.test(g)) emoji++; for (const s of check(t.text, t.fontKey).scripts) scripts.add(s.id); }
    if (!emoji && !scripts.size) return;
    try { root.CNDesignLog.event('editorText', { texts: texts.length, emoji, scripts: [...scripts] }); } catch (_) { /* the log is a courtesy */ }
  }

  /** The editor's extension points (charm-nest-charm-editor.js EXTENSION POINTS): onOpen / onClose for the words box; and its open(), so that a caller that gives the editor no font
   *  engine of its own gets this one (the editor makes its geometry from opts.textToPaths as it opens; without it, its stand-in font would be engraved). Idempotent. */
  function hook() {
    const CE = root.CharmEditor; if (!CE || typeof CE.open !== 'function') return false;
    if (CE.__ceText) return true; CE.__ceText = true;
    (CE.onOpen || (CE.onOpen = [])).push(ctx => { lastSig = fontSig(); UI.attach(ctx); });
    (CE.onClose || (CE.onClose = [])).push((ctx, res) => { UI.detach(); logResult(res); });
    const open = CE.open;
    CE.open = function (opts) {
      if (!opts || typeof opts.textToPaths !== 'function') { const En = E(); if (En && En.fonts && En.fonts.Regular) opts = Object.assign({}, opts, { textToPaths, fonts: opts && Array.isArray(opts.fonts) && opts.fonts.length ? opts.fonts : loadedFonts() }); }
      return open.apply(this, [opts].concat([].slice.call(arguments, 1)));
    };
    return true;
  }
  if (!hook() && doc()) doc().addEventListener('DOMContentLoaded', hook, { once: true });

  root.CharmNestCeText = { fonts, textToPaths, prepare, check, baseRtl, hook, state: () => ({ inflight: [...inflight.keys()], ui: UI.state() }) };
})(typeof window !== 'undefined' ? window : this);
