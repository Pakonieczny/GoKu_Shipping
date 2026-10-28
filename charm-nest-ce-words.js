/* charm-nest-ce-words.js — THE ENGRAVING'S WORDS ARE TYPE IN THE CHARM EDITOR, AND STAY THE ENGRAVING'S (TYPEWORDS, Paul 28 Oct 2026, order 4203083230: "When I select existing text on the
 * charm that text is not recognized as text, it's just an object on the charm. The user also needs the ability to update existing text, even if that text came from back engraving modal.")
 *
 * What happened (the live sandbox records of TINY BAR_7427, order 4203083230, read-only): the front's words "33.47765N 86.80020W" (Mynerve) were approved on the card; in the Charm Editor they
 * were retyped ("…W愛") and saved. CETEXT then saved changed words as the EDITOR's words: an outline part of the piece's artwork record (Charm_Pool_Art …~front, added
 * "engrave-words-front-mvsjt11z", kind "text", its outlines in `d`) and took the card's own words off (charmEditWordsTaken: the face approved "as the editor drew it", no words). Opened again,
 * the face had no words to hand in and the record's part came back as what a record holds, a path: "Compound Path", never Type.
 *
 * Now (charm-nest-bridge.js openCharmEditorOnce with this file):
 *   1. a face's engraving words always open as ONE Type object linked to the face's job, from every way into the editor (its card, the order window's Open in Charm Editor and Edit Design),
 *      approved or not, shelved or not: its fit where it has one (charmEditWords), else its words at the middle of the face (fallback, `placed: false`)
 *   2. a Save gives the words back to the ENGRAVING, never to the drawing: the job's words, font and letter case (Engrave.saveSetup, as the card's own words box saves them: an approved face goes
 *      back to its Approve), fitted again, where the person put them when they were moved, turned or sized (refit)
 *   3. a record that already holds the words as outlines is repaired as the editor opens (REPAIR RULE, sortParts below): a part is dropped from the art only when it is PROVABLY a copy of the
 *      face's engraving words, and only by a Save; anything else is kept and said
 *
 * REPAIR RULE (sortParts): the parts of a face that keep words (a record part with `text`: the editor's words saved as outlines) are sorted, and NOTHING ELSE is ever touched:
 *   a. its id is "engrave-words-<this face>-…" (the id the card's words carry into the editor: only the CETEXT hand-back ever wrote a part with it) and the face has an engraving job:
 *        a1. the job has NO words (the hand-back took them off the card)        → the part IS the face's words: it opens as the face's Type, and Save gives the words back to the job and
 *                                                                                 drops the outlines from the art                                                        ('words', repair)
 *        a2. the job's words are the part's words (whitespace aside)             → a copy of the words on top of them: the outlines are dropped (by Save); the words stay the job's Type
 *                                                                                 (when the job's words could not be handed in, the part becomes their Type in its own place)      ('drop' / 'words')
 *        a3. the job has OTHER words                                            → an earlier engraving's words, outlined: kept, named "Outlined words · …" and said as the
 *                                                                                 editor opens (Type ▸ Create Outlines was used: retype to edit)                         ('keep')
 *   b. any other part with words (the editor's own Type, saved as outlines; a hand-back with no job): when its outlines ARE that text set in one engraving font (same number of outlines,
 *      the same box to 1 %, every letter's box in place to 3 % of the size: match) it opens as editable Type again in that font, size and place, its id kept; a Save that does not change
 *      it writes its saved outlines again, exactly (the bridge's outlinesWas)                                                                                         ('type')
 *      else it stays the outlines it is, as Illustrator keeps words after Type ▸ Create Outlines (a weight grown on them, a stretch: nothing proves the letters)      ('keep')
 *
 *   CharmNestCeWords.HANDBACK            /^engrave-words-(front|back)-/
 *   CharmNestCeWords.match(part, o)      → { fontKey, sizePt, align, tx, ty, exact, dev } | null    o: { text, engine(text, fontKey, sizePt, opts), fonts: [fontKey], aligns?, lineGap?, bboxOf?, loose? }
 *   CharmNestCeWords.sortParts(o)        → [{ act: 'words' | 'drop' | 'type' | 'keep', id, part, m?, repair?, name? }]   o: { face, items, job: { key, words: [lines], fontKey } | null, handed, match }
 *   CharmNestCeWords.typeOf(part, m, o)  the Type item a part becomes
 *   CharmNestCeWords.diff(given, now, cased) → { gone, text, font, place, style: [names], changed, words }   what a Save of the handed-in words gives the job
 *   CharmNestCeWords.placeOf(item, o)    → { centre: [x, y] (the job's view, y up), angle, size }   where the job's words go when the person moved them (charmEditWords' own sum, the other way)
 *   CharmNestCeWords.fontWordOf(fontKey, O)   the shop's word the setup takes for an engraving font ("Typewriter"), '' when none names it
 * UMD: window.CharmNestCeWords in the page, module.exports in Node (the tests). Pure: no network, no page state. */
(function (root, factory) {
  const api = factory(root);
  if (typeof module === 'object' && module.exports) module.exports = api;
  if (root) root.CharmNestCeWords = api;
})(typeof self !== 'undefined' ? self : typeof globalThis !== 'undefined' ? globalThis : this, function (root) {
  'use strict';
  const HANDBACK = /^engrave-words-(front|back)-/;
  const fin = v => typeof v === 'number' && Number.isFinite(v);
  const norm = s => String(s == null ? '' : s).replace(/\s+/g, ' ').trim();
  /** The box of every point of the subpaths (anchors and handles): the same rule on both sides of a comparison. */
  function cbox(subs) {
    let x0 = Infinity, y0 = Infinity, x1 = -Infinity, y1 = -Infinity;
    for (const sp of subs || []) for (const op of sp || []) for (let k = 1; k < (op || []).length; k++) { const p = op[k]; if (!p || !fin(+p[0]) || !fin(+p[1])) continue; x0 = Math.min(x0, +p[0]); y0 = Math.min(y0, +p[1]); x1 = Math.max(x1, +p[0]); y1 = Math.max(y1, +p[1]); }
    return x0 <= x1 && y0 <= y1 ? [x0, y0, x1, y1] : null;
  }
  const boxOf = (o, subs) => { try { const b = o && typeof o.bboxOf === 'function' ? o.bboxOf(subs) : null; return b && b.length === 4 && b.every(fin) ? b : cbox(subs); } catch (_) { return cbox(subs); } };
  const r6 = v => Math.round(v * 1e6) / 1e6, r9 = v => Math.round(v * 1e9) / 1e9;

  /** Is `part` (a path of the face: { subpaths }) the words `o.text` set in one of `o.fonts`? The size is read from the outlines' box (twice: a small size may be cut in the family's heavier face),
   *  then every outline is compared, in order, by its box. The best font wins. `o.loose`: when none matches, a guess in the first font (its size from the box's height, its middle on the part's). */
  function match(part, o) {
    o = o || {};
    const subs = part && Array.isArray(part.subpaths) ? part.subpaths : [], text = String(o.text == null ? (part && part.text) || '' : o.text), engine = o.engine;
    if (!subs.length || !norm(text) || typeof engine !== 'function') return null;
    const pb = boxOf(o, subs); if (!pb) return null;
    const pw = pb[2] - pb[0], ph = pb[3] - pb[1]; if (!(pw > 1e-6) || !(ph > 1e-6)) return null;
    const pm = [(pb[0] + pb[2]) / 2, (pb[1] + pb[3]) / 2], n = subs.length, pcs = subs.map(sp => cbox([sp]));
    const aligns = Array.isArray(o.aligns) && o.aligns.length ? o.aligns : /\n/.test(text) ? ['center', 'left', 'right'] : ['center'];
    const draw = (key, size, align) => { try { const r = engine(text, key, size, { align, ...(fin(+o.lineGap) && +o.lineGap > 0 ? { lineGap: +o.lineGap } : {}) }); return r && Array.isArray(r.subpaths) && r.subpaths.length ? r.subpaths : null; } catch (_) { return null; } };
    let best = null, guess = null;
    for (const key of [...new Set((o.fonts || []).filter(Boolean))]) for (const align of aligns) {
      let s = 10, rs = null, rb = null;
      for (let i = 0; i < 3; i++) {   // (the size read from the height, then from both sides: a small size can be cut in the heavier face, whose box differs)
        rs = draw(key, s, align); rb = rs && boxOf(o, rs); if (!rb || !(rb[3] - rb[1] > 1e-9) || !(rb[2] - rb[0] > 1e-9)) { rs = null; break; }
        s = i === 0 ? s * ph / (rb[3] - rb[1]) : s * ((pw / (rb[2] - rb[0])) + (ph / (rb[3] - rb[1]))) / 2;
      }
      if (!rs) continue;
      rs = draw(key, s, align); rb = rs && boxOf(o, rs); if (!rb) continue;
      const rm = [(rb[0] + rb[2]) / 2, (rb[1] + rb[3]) / 2], tx = pm[0] - rm[0], ty = pm[1] - rm[1];
      if (!guess && o.loose) guess = { fontKey: key, sizePt: r6(s), align, tx: r9(tx), ty: r9(ty), exact: false, dev: Infinity };
      if (rs.length !== n) continue;
      if (Math.abs(pw / (rb[2] - rb[0]) - 1) > 0.01 || Math.abs(ph / (rb[3] - rb[1]) - 1) > 0.01) continue;
      const tol = Math.max(0.02, 0.03 * s); let dev = 0;
      for (let i = 0; i < n && dev <= tol; i++) { const a = cbox([rs[i]]), b = pcs[i]; if (!a || !b) { dev = Infinity; break; } for (let k = 0; k < 4; k++) dev = Math.max(dev, Math.abs(a[k] + (k % 2 ? ty : tx) - b[k])); }
      if (dev > tol) continue;
      if (!best || dev / s < best.dev / best.sizePt) best = { fontKey: key, sizePt: r6(s), align, tx: r9(tx), ty: r9(ty), exact: true, dev };
    }
    return best || guess;
  }

  /** The Type item a part becomes (its words in the matched font, size and place; its paint, group and weight kept). `o.id`, `o.source`, `o.name`, `o.engraving` say what it is now. */
  function typeOf(part, m, o) {
    o = o || {};
    const it = { id: o.id != null ? String(o.id) : String(part.id), kind: 'text', role: o.role || part.role || 'hatch', source: o.source || 'added', name: o.name || part.name || norm(part.text).slice(0, 40),
      text: String(part.text == null ? '' : part.text), fontKey: m.fontKey, sizePt: m.sizePt, align: m.align || 'center', tx: m.tx, ty: m.ty, sx: 1, sy: 1, rot: 0 };
    if (!o.engraving) { if (part.stroke != null) it.stroke = part.stroke; if (part.fill != null) it.fill = part.fill; if (part.lwPt) it.lwPt = part.lwPt; if (part.group) it.group = part.group; }
    if (part.weightMm) it.weightMm = part.weightMm;
    if (o.engraving) it.engraving = o.engraving;
    return it;
  }

  /** The parts of one face that keep words, sorted by the REPAIR RULE (above). `items`: the face's parts as the editor is given them (top level, AE.itemsOf); `job`: the face's engraving job
   *  ({ key, words: its lines, fontKey }) or null; `handed`: the job's words were handed in as Type already; `match(part, fonts, loose)`: the glyph comparison (match above, with the page's engine). */
  function sortParts(o) {
    o = o || {};
    const face = o.face === 'front' ? 'front' : 'back', job = o.job || null, out = [];
    const parts = (o.items || []).filter(it => it && it.kind === 'path' && it.text != null && norm(it.text) && Array.isArray(it.subpaths) && it.subpaths.length);
    const jobWords = job ? norm((job.words || []).join(' ')) : '';
    // (several hand-backs of the same face: the newest is the words; the id ends with the moment it was handed in, base 36)
    const hb = parts.filter(p => { const h = HANDBACK.exec(String(p.id)); return h && h[1] === face; }), stamp = p => parseInt(String(p.id).split('-').pop(), 36) || 0;
    hb.sort((a, b) => stamp(b) - stamp(a));
    const say = (p, why) => `Outlined words · ${norm(p.text).slice(0, 40)}${why ? ` (${why})` : ''}`;
    let words = jobWords, taken = !!o.handed && !!jobWords;   // (words: what the face's engraving says once these are sorted; taken: they are the face's Type already)
    for (const p of hb) {
      if (!job) { out.push(sortOther(p, o, say)); continue; }
      const mine = norm(p.text);
      if (!words || (mine === words && !taken)) {   // (a1: the words the hand-back took off the card; a2 with no Type of them yet: the copy stands in their place)
        out.push({ act: 'words', id: String(p.id), part: p, m: o.match ? o.match(p, [job.fontKey], true) : null, repair: !words });
        words = mine; taken = true; continue;
      }
      if (mine === words) { out.push({ act: 'drop', id: String(p.id), part: p }); continue; }   // (a2: a copy of the words on top of them)
      out.push({ act: 'keep', id: String(p.id), part: p, name: say(p, "an earlier engraving's words: Type › Create Outlines was used; retype to edit") });   // (a3)
    }
    for (const p of parts) if (!hb.includes(p)) out.push(sortOther(p, o, say));
    return out;
  }
  function sortOther(p, o, say) {
    const m = o.match ? o.match(p, null, false) : null;
    return m && m.exact ? { act: 'type', id: String(p.id), part: p, m } : { act: 'keep', id: String(p.id), part: p, name: null };
  }

  /** What the handed-in words `given` became in `now` (the item the editor gave back, or null: deleted). `cased(text, letterCase)`: the editor's letter case (CharmEditor.model.casedText).
   *  text: the words (in their case) differ; font: another font; place: moved, turned or sized; style: what the engraving's fit does not take (bold, italic, small capitals, letter spacing,
   *  alignment, weight, a line or cut role, words on a path). words: the lines the job takes. */
  function diff(given, now, cased) {
    const C = typeof cased === 'function' ? cased : (t => t);
    if (!now || now.hidden) return { gone: true, changed: !!given, text: false, font: false, place: false, style: [], words: [] };
    const wordsOf = it => String(C(it.text == null ? '' : String(it.text), it.letterCase === 'small' ? '' : (it.letterCase || '')) || '');   // (small capitals are a style the fit cannot cut: the words stay as typed)
    const lines = wordsOf(now).split(/\r?\n/).map(s => s.trim()).filter(Boolean);
    const num = (it, k, d) => (fin(+it[k]) ? +it[k] : d), near = (a, b) => Math.abs(a - b) < 1e-6;
    const text = lines.join('\n') !== wordsOf(given).split(/\r?\n/).map(s => s.trim()).filter(Boolean).join('\n');
    const font = String(now.fontKey || '') !== String(given.fontKey || '');
    const place = ['tx', 'ty', 'rot'].some(k => !near(num(now, k, 0), num(given, k, 0))) || ['sx', 'sy'].some(k => !near(num(now, k, 1), num(given, k, 1))) || !near(num(now, 'sizePt', 0), num(given, 'sizePt', 0));
    const style = [];
    if (!!now.bold !== !!given.bold) style.push('bold');
    if (!!now.italic !== !!given.italic) style.push('italic');
    if (now.letterCase === 'small' && given.letterCase !== 'small') style.push('small capitals');
    if (!near(num(now, 'tracking', 0), num(given, 'tracking', 0))) style.push('letter spacing');
    if (String(now.align || 'left') !== String(given.align || 'left')) style.push('alignment');
    if (!near(num(now, 'weightMm', 0), num(given, 'weightMm', 0))) style.push('weight');
    if (String(now.role || '') !== String(given.role || '')) style.push(now.role === 'line' ? 'line engraving' : 'paint');
    if (now.pathD || now.pathAt != null) style.push('words on a path');
    return { gone: false, changed: text || font || place || style.length > 0, text, font, place, style, words: lines };
  }

  /** Where the job's words go when the person moved, turned or sized them: the editor turns a text about the middle of its own ink (c, the engine's box of the words) and moves that middle by
   *  tx, ty; charmEditWords put that middle where the fit's own layout middle (m, about the fit's centre, y down) lands. So the fit's centre is the ink middle less the turned m, and y is the job
   *  view's (up). `o`: { engine (as the editor draws this text: its case and style), bboxOf, m(size) | m (the fit's layout middle for the job's words at that size, y down, unturned),
   *  at (the ink middle when it is known otherwise: words inside a group) }. → { centre, angle, size, at } or null. */
  function placeOf(item, o) {
    o = o || {};
    if (!item) return null;
    const sx = fin(+item.sx) ? Math.abs(+item.sx) : 1, sy = fin(+item.sy) ? Math.abs(+item.sy) : 1, size = (fin(+item.sizePt) && +item.sizePt > 0 ? +item.sizePt : 0) * Math.sqrt(sx * sy);
    if (!(size > 0)) return null;
    let at = Array.isArray(o.at) && o.at.length === 2 && o.at.every(fin) ? o.at.slice() : null;
    if (!at) {
      if (typeof o.engine !== 'function') return null;
      let r; try { r = o.engine(item.text, item.fontKey, +item.sizePt, { align: item.align || 'left', ...(item.lineGap ? { lineGap: item.lineGap } : {}) }); } catch (_) { r = null; }
      const b = r && boxOf(o, r.subpaths || []); if (!b) return null;
      at = [(b[0] + b[2]) / 2 + (+item.tx || 0), (b[1] + b[3]) / 2 + (+item.ty || 0)];
    }
    const rot = +item.rot || 0, th = rot * Math.PI / 180, cs = Math.cos(th), sn = Math.sin(th), mm = typeof o.m === 'function' ? o.m(size) : o.m, m = Array.isArray(mm) && mm.length === 2 && mm.every(fin) ? mm : [0, 0];
    return { centre: [r9(at[0] - (cs * m[0] - sn * m[1])), r9(-(at[1] - (sn * m[0] + cs * m[1])))], angle: Math.abs(rot) < 1e-9 ? 0 : r9(-rot), size: r6(size), at };
  }

  /** The shop's word for an engraving font that the setup reads back as that very font (O.fontOfPhrase): its own shop word first ("Typewriter"), else its name; '' when none does. */
  function fontWordOf(fontKey, O) {
    if (!fontKey || !O) return '';
    const g = (O.FONT_WORDS || []).find(x => x && x.id === fontKey), f = O.fontById ? O.fontById(fontKey) : null;
    for (const w of [...((g && g.words) || []), f && f.name, ...((f && f.names) || [])].filter(Boolean)) { try { if (O.fontOfPhrase && O.fontOfPhrase(w) === fontKey) return w; } catch (_) { /* the next word */ } }
    return '';
  }

  return { HANDBACK, norm, cbox, match, typeOf, sortParts, diff, placeOf, fontWordOf };
});
