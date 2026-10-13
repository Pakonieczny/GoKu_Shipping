/* WHY a person changed what the system chose for an engraving (Paul, 12 Oct 2026, round 2, point 7: "If a user tries to override the auto selection engraving options then there should be a quick
 * prompt asking the user for a reason. There should be 3-4 quick clickable options for the user to choose from and a small input text box if the 3-4 shortcut buttons aren't sufficient. This needs to be
 * recorded in firebase, be mindful of how much API usage we are using for firebase."). Plan: plans/engrave-ui-1012/round2 (ENGWHY-findings.md).
 *
 * This file is the PURE half: what counts as an override, the reasons, and the record. The prompt itself is drawn by charm-nest-engrave-panel.js (inline, under the words, no pop-up).
 *
 * THE RECORD rides inside the line's existing `engravingOverride` (saved by Engrave.saveSetup -> CharmNestEngraveSetup.apply -> buildOverride -> env.store -> the run's ONE save, which runs
 * whether or not there is a reason): no new document, no new listener, no poll, no extra read, no extra write. Flat objects only (Firestore refuses an array inside an array):
 *   engravingOverride.reason = { code, text, by, at, of, slot? }
 *       code  asked | misread | listing | typo | other          (the four quick buttons and "something else")
 *       text  the button's words, or what the person typed (at most 120 characters)
 *       by    the signed-in login or the name bar (CharmNestEngraveSetup.apply: env.who()), never typed here
 *       at    the instant of the save
 *       of    what was changed: "mode+front+back+font+position" (any of them)
 *       slot  the piece (L, R, D2 ...) when the change was for one piece of a line
 *   engravingOverride.was    = { mode, front?: { text, fontWord?, font }, back?: {...} }     what the system had chosen (CharmNestEngraveSetup.autoChoice; font = the installed font id) and the person replaced
 * A record without a reason keeps working (nothing reads `reason` or `was` but the lines below). A save that ends equal to the system's choice carries `reason: null`: both fields are dropped.
 * Only the LATEST reason of a line is kept in the override (the order timeline's existing "Engraving edited by ..." line carries each one, see charm-nest-bridge.js wordsEvent).
 *
 *   REASONS                              [{ code, label }] the four quick buttons, in order
 *   labelOf(code)                        the button's words (or "Something else")
 *   changes(vm, draft, opts)             { any, of: [...], was } what a staged edit overrides (vs the SYSTEM's choice, CharmNestEngraveSetup.diffFromAuto, not vs the last save): opts { jobs, positionMoved }
 *   patchWith(patch, why, ch)            the patch to save: with { reason, was } when overridden, with `reason: null` when it is not
 *   fold(out, prev, patch, by, at)       buildOverride's one line: puts reason/was in the override being built
 *   reasonOf(row, slot?)                 read-only view of the reason in a line's override, for a marker's title or a quiet line: { code, label, text, by, at, of, was, line, title } | null
 *   say(reason)                          the short phrase the timeline line carries ("Customer asked" / the person's words)
 *
 * Pure. Loads in the page (window.CharmNestEngraveWhy) and in node. */
(function (root, factory) {
  const api = factory(root);
  if (typeof module === 'object' && module.exports) module.exports = api; else root.CharmNestEngraveWhy = api;
})(typeof self !== 'undefined' ? self : this, function (root) {
  'use strict';
  const dep = (name, file) => { const g = root && root[name]; if (g) return g; try { return typeof require === 'function' ? require(file) : null; } catch (_) { return null; } };
  const Setup = () => dep('CharmNestEngraveSetup', './charm-nest-engrave-setup.js');
  const str = x => String(x == null ? '' : x);
  const lineOf = row => (row && row.parentRow) || row || null;
  const tryDo = (f, d) => { try { return f(); } catch (_) { return d; } };
  const MAX_TEXT = 120;
  /** The quick buttons: the reasons this shop's own data shows (the buyer asked in a message after ordering; the intake read the note wrongly or could not tell; the listing's option names the wrong face or
   *  none; a typed word that is spelt wrong). Four at most: the text box is for everything else. */
  const REASONS = [
    { code: 'asked', label: 'Customer asked' },
    { code: 'misread', label: 'Misread the note' },
    { code: 'listing', label: 'Listing option wrong' },
    { code: 'typo', label: 'Spelling fix' }
  ];
  const CODES = REASONS.map(r => r.code).concat('other');
  const labelOf = code => { const r = REASONS.find(x => x.code === code); return r ? r.label : code === 'other' ? 'Something else' : ''; };
  const clip = (s, n) => str(s).replace(/\s+/g, ' ').trim().slice(0, n || MAX_TEXT);
  
  const flat = t => str(t).replace(/\s+/g, ' ').trim();
  /** The same words? Line breaks and runs of spaces are layout, not words (the engraver splits a long name over lines itself: "NAME\nONE" is "Name One"), and a capitals font (Ace) engraves capitals whatever was typed
   *  (CharmNestOrders.capsText), so "Name One" and "NAME ONE" are one engraving there; in a font with lowercase "name" and "Name" differ (a spelling fix). */
  function sameWords(a, b, fontId) {
    const O = dep('CharmNestOrders', './charm-nest-orders.js'), caps = t => (O && O.capsText && fontId ? tryDo(() => O.capsText(t, fontId), t) : t);
    return flat(caps(a)) === flat(caps(b));
  }
  /** What a staged edit overrides. `draft` = the panel's draft ({ mode, faces: { front: { text, font }, back: ... }, init: the faces as the panel opened them }). The decision is ENGAUTO's
   *  CharmNestEngraveSetup.diffFromAuto (the system's own choice, whatever a person saved before; only the faces the staged mode engraves; fonts by the installed font id), MINUS what only looks different:
   *   - words: tolerant (sameWords: line breaks, spaces, capitals of a capitals font), and a face the person did not touch while nothing was saved before is the system's own (the panel opens on the jobs' words,
   *     which the engraver has already laid out and put in capitals) and is never a words override;
   *   - font: a face whose font was not touched while nothing was saved before is the system's own;
   *   - a face the system gave NO words (it asked a person which words go where) is never an override: the person is answering, not replacing.
   *  opts: { jobs: the piece's jobs, positionMoved: the person moved the words on the charm }.
   *  -> { any, of: ['mode' | 'front' | 'back' | 'font' | 'position', ...], was }  (was = the system's choice as it is stored; null when nothing is known) */
  function changes(vm, draft, opts) {
    const none = { any: false, of: [], was: null }, S = Setup(), o = opts || {};
    if (!S || typeof S.diffFromAuto !== 'function' || !vm || !vm.line || !draft) return none;
    const faceNow = f => { const x = (draft.faces && draft.faces[f]) || {}; return { text: str(x.text), fontWord: str(x.font).trim() || null }; };
    const d = tryDo(() => S.diffFromAuto(vm.line, vm.slot || null, { mode: draft.mode, front: faceNow('front'), back: faceNow('back') }, o.jobs ? { jobs: o.jobs } : undefined), null);
    if (!d || !d.auto) return none;
    const auto = d.auto, of = [], saved = !!vm.edited, init = draft.init || {};
    const fontOf = f => tryDo(() => S.fontIdOf(vm.line, f, faceNow(f).fontWord, vm.slot || null), '') || (auto[f] && auto[f].fontId) || '';
    for (const part of d.parts || []) {
      const [face, what] = part.split('.');
      if (part === 'mode') of.push('mode');
      else if (what === 'text') {
        const mine = faceNow(face).text, was = init[face] ? str(init[face].text) : null;
        if (!auto[face] || !auto[face].text) continue;
        if (!saved && was !== null && flat(was) === flat(mine)) continue;   // (not touched: the system's own words, as the engraver laid them out)
        if (sameWords(mine, auto[face].text, fontOf(face))) continue;
        of.push(face);
      } else if (what === 'font') {
        const was = init[face] ? str(init[face].font).trim() : null;
        if (!saved && was !== null && was === str(faceNow(face).fontWord)) continue;   // (not touched)
        of.push('font');
      }
    }
    if (o.positionMoved) of.push('position');
    const uniq = of.filter((k, i) => of.indexOf(k) === i), was = { mode: auto.mode };
    for (const f of ['front', 'back']) { const a = auto[f]; if (a && a.text) was[f] = Object.assign({ text: a.text }, a.fontWord ? { fontWord: a.fontWord } : {}, a.fontId ? { font: a.fontId } : {}); }
    return { any: uniq.length > 0, of: uniq, was };
  }

  /** The patch Engrave.saveSetup takes, with the reason in it when the staged edit overrides the system, and `reason: null` when it does not (an older reason no longer describes the line). */
  function patchWith(patch, why, ch) {
    const p = Object.assign({}, patch || {});
    if (!ch) return p;
    if (ch.any && why && CODES.includes(why.code)) { p.reason = { code: why.code, text: clip(why.text) || labelOf(why.code), of: ch.of.join('+') }; if (ch.was) p.was = ch.was; }
    else if (!ch.any) { p.reason = null; p.was = null; }
    return p;
  }

  /** `was` as it is stored: flat, plain strings (no arrays at all). */
  function flatWas(w) {
    if (!w || typeof w !== 'object' || !['none', 'front', 'back', 'both'].includes(w.mode)) return null;
    const out = { mode: w.mode };
    for (const f of ['front', 'back']) { const x = w[f]; if (x && typeof x === 'object' && str(x.text)) { out[f] = { text: str(x.text).slice(0, 400) }; if (str(x.fontWord)) out[f].fontWord = str(x.fontWord).slice(0, 40); if (str(x.font)) out[f].font = str(x.font).slice(0, 60); } }
    return out;
  }
  /** buildOverride's one line. `out` is the override being built (by and at are already in it); `prev` the line's old override; `p` the patch. A patch with a reason sets it (and `was`); `reason: null` clears both;
   *  a patch that says nothing about it (any other caller) keeps what the line had. */
  function fold(out, prev, p) {
    if (!out || typeof out !== 'object') return out;
    const r = p && p.reason;
    if (r === null) return out;
    if (r && typeof r === 'object' && CODES.includes(str(r.code))) {
      out.reason = { code: str(r.code), text: clip(r.text) || labelOf(str(r.code)), by: str(out.by), at: out.at };
      if (str(r.of)) out.reason.of = clip(r.of, 60);
      if (p.slot && /^(L|R|D\d{1,2})$/.test(str(p.slot))) out.reason.slot = str(p.slot);
      const w = flatWas(p.was); if (w) out.was = w;
      return out;
    }
    if (prev && prev.reason && typeof prev.reason === 'object') out.reason = prev.reason;
    if (prev && prev.was && typeof prev.was === 'object') out.was = prev.was;
    return out;
  }

  /** The short phrase a timeline line carries for a reason. */
  const say = r => (r && typeof r === 'object' ? clip(r.text) || labelOf(str(r.code)) : '');

  /** A line's reason, read-only, for a quiet line or a marker's title. `slot`: the piece asked for; a reason written for another piece is not this piece's. null when the line has none. */
  function reasonOf(row, slot) {
    const line = lineOf(row), ov = line && line.engravingOverride, r = ov && typeof ov === 'object' ? ov.reason : null;
    if (!r || typeof r !== 'object' || !str(r.code)) return null;
    if (slot && r.slot && r.slot !== slot) return null;
    const text = say(r), by = str(r.by), at = +r.at || 0;
    const when = at ? tryDo(() => new Date(at).toLocaleString([], { month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit' }), '') : '';
    const line1 = `Changed: ${text}${by ? ' · ' + by : ''}`;
    const was = ov.was && typeof ov.was === 'object' ? ov.was : null;
    return { code: str(r.code), label: labelOf(str(r.code)), text, by, at, of: str(r.of), slot: str(r.slot), was, line: line1, title: `${line1}${when ? ' · ' + when : ''}` };
  }

  return { REASONS, CODES, MAX_TEXT, labelOf, changes, sameWords, sameWords, patchWith, fold, say, reasonOf, flatWas };
});
