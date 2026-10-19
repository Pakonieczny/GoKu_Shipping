/* charm-nest-order-seals.js — the approved engraving and monogram seals of ONE ORDER, wherever the order is shown as a whole: the Orders tab's card / list row, and the order window's Overview
 * (CARDSEAL, Paul, 19 Oct 2026: "I'm also not seeing the Approved Engraving/Monograms Seals in each order card in the Orders tab. All orders that are approved for engraving, or monograms by the user must have a clear seal that is
 * easily visible to the user whether they are inside the Detail Order view or looking at a list view in the Orders tab.")
 *
 * It makes NO seal of its own: each seal is the real markup the Engrave tab already draws (charm-nest-engraving-seals.js html, charm-nest-monogram-seals.js html), one `.sealRow` per approved piece and face, with its words beside it
 * (BACK ENGRAVING / FRONT ENGRAVING / MONOGRAM, the piece, who and when). Nothing here reads a record, calls the network, waits or listens: it reads what the page already holds (the Engrave jobs of a line, the monogram widget's
 * in-memory seals) and a page that has not got a seal in memory shows none for it (the monogram's `ensureSeals`, which asks the order's timeline, is NEVER called from here: the Engrave tab owns that).
 *
 *   CNOrderSeals.of(rows)   -> { units, total, approved, sig }   rows = the lines of one order, in line order (one line row is taken as [row]). A unit is one piece (or one face of a piece) that is to be engraved or is a monogram piece:
 *                              { kind: 'engrave' | 'monogram', key, line, face: 'back' | 'front' | '', piece, letters, done, seals: [the APPROVED seals], row, slot }
 *   CNOrderSeals.html(model) -> the strip ('' until a unit is approved)        CNOrderSeals.sig(model) -> the text that changes exactly when the strip does (a card paints again only then)
 *
 * WHAT IS "APPROVED". An engraving job is approved when it is in state approved / written (the state is published only after the stamp has come down) and its seals are its `engraveApproved` seals. A job that was decided "no engraving"
 * (state none / skipped: the CUT PLAIN seal) is NOT an approved engraving: it has no seal here and is not counted, so a bar cut plain beside an engraved one does not make the order look unfinished. A monogram piece is approved
 * when the widget has a seal for it and does not say it was changed since. NEVER feed a monogram to CNEngravingSeals (it invents a legacy BACK ENGRAVING seal for it): monogram lines only go to CNMonogramSeals. */
(function (root, factory) {
  const api = factory(root); api.make = factory;
  if (typeof module === 'object' && module.exports) module.exports = api; else if (root) root.CNOrderSeals = api;
})(typeof window !== 'undefined' ? window : null, function (root) {
  'use strict';
  const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const tryDo = (f, d) => { try { const v = f(); return v === undefined ? d : v; } catch (_) { return d; } };
  const lib = name => (root && root[name]) || null;
  const APPROVED = ['approved', 'written'], NOT_ENGRAVED = ['none', 'skipped'];
  const faceOf = j => (j && (j.face === 'front' || String(j.key || '').endsWith('~front')) ? 'front' : 'back');

  /** The units of one engraved line: every job of it (all faces, slot by slot), an approved one carrying its engraveApproved seals. [] for a line with no engraving at all. */
  function engraveUnits(r) {
    const E = lib('Engrave'), S = lib('CNEngravingSeals'); if (!E || !S || typeof E.jobsOf !== 'function') return [];
    const jobs = (tryDo(() => E.jobsOf(r), []) || []).filter(j => j && !j.editingBack && !j.archived);
    const out = [], approvedOf = x => (tryDo(() => S.list(x), []) || []).filter(s => s && s.how === 'engraveApproved');
    if (!jobs.length) {
      // (a line read from the records, Engrave holds no job for it: its own record says it)
      const e = r.engrave;
      if (e && typeof e === 'object') { if (e.needed && e.approved) { const seals = approvedOf(e); if (seals.length) out.push({ kind: 'engrave', key: r.key, line: r.key, face: faceOf(e), piece: '', letters: '', done: true, seals, row: r, slot: null }); } }
      return out;
    }
    const D = lib('CharmNestEngraveSides'), slots = jobs.filter(j => j.slot).map(j => j.slot), shp = D && D.shape && slots.length ? tryDo(() => D.shape(slots), null) : null;
    for (const j of jobs) {
      if (NOT_ENGRAVED.includes(j.state)) continue;   // ("no engraving" / cut plain: decided, not an engraving)
      const seals = APPROVED.includes(j.state) ? approvedOf(j) : [];
      const piece = j.slot && D && D.slotWord ? String(tryDo(() => D.slotWord(j, false, shp), '') || '') : '';
      out.push({ kind: 'engrave', key: j.key, line: r.key, face: faceOf(j), piece, letters: '', done: seals.length > 0, seals, row: r, slot: j.slot || null });
    }
    return out;
  }
  /** The units of one monogram line: its pieces, an approved one carrying the monogram's own seals. */
  function monoUnits(r) {
    const M = lib('CNMonogramSeals'); if (!M) return [];
    const spaced = v => String(v || '').split('').join(' ');
    return (tryDo(() => M.pieces(r), []) || []).map(p => {
      const seals = tryDo(() => M.list(r, p.slot), []) || [], st = seals.length ? tryDo(() => M.status(r, p.slot), null) : null;
      const done = seals.length > 0 && !(st && st.approved === false);   // (a piece changed since its approval waits for its new one: its old seals are history, not an approval)
      const letters = st && st.letters ? spaced(st.letters) : spaced((seals.map(s => s.letters).filter(Boolean).pop()) || '');
      return { kind: 'monogram', key: p.key, line: r.key, face: '', piece: p.of > 1 ? `Piece ${p.n} of ${p.of}` : '', letters, done, seals: done ? seals : [], row: r, slot: p.slot || null };
    });
  }
  /** The units of the lines of one order. */
  function of(rows) {
    const units = [];
    for (const r0 of Array.isArray(rows) ? rows : rows ? [rows] : []) {
      const r = r0 && (r0.parentRow || r0); if (!r) continue;
      const M = lib('CNMonogramSeals');
      const mono = !!(M && tryDo(() => M.isMonogram(r), false));
      for (const u of (mono ? monoUnits(r) : engraveUnits(r))) units.push(u);
    }
    const approved = units.filter(u => u.done).length, model = { units, total: units.length, approved, rows: units.length ? new Set(units.map(u => u.line)).size : 0 };
    model.sig = sig(model);
    return model;
  }
  /** The text that changes exactly when the strip does. */
  function sig(model) {
    return model && model.approved ? JSON.stringify([model.total, model.units.map(u => [u.kind, u.key, u.face, u.done, u.letters, u.seals.map(s => [s.id, s.at, s.by])])]) : '';
  }
  const WORD = { engrave: { back: 'Back engraving', front: 'Front engraving' }, monogram: { '': 'Monogram' } };
  const when = t => { try { const d = new Date(+t); return d.toLocaleString('en-US', { timeZone: 'America/Toronto', month: 'short', day: 'numeric' }) + ' · ' + d.toLocaleTimeString('en-US', { timeZone: 'America/Toronto', hour: 'numeric', minute: '2-digit' }); } catch (_) { return ''; } };
  /** One approved unit: its seal(s), as the Engrave tab draws them, and its words beside them. */
  function unitHtml(u, withLine) {
    const E = lib('CNEngravingSeals'), M = lib('CNMonogramSeals'), Sl = lib('Seal');
    const seals = u.kind === 'monogram' ? (M ? tryDo(() => M.html(u.row, u.slot), '') : '') : (E ? tryDo(() => E.html({ seals: u.seals }), '') : '');
    if (!seals) return '';   // (no seal in memory: nothing is drawn for it, and nothing is asked)
    const last = u.seals[u.seals.length - 1] || {}, by = String(last.by || '').trim(), at = +last.at > 0 ? when(last.at) : '';
    const say = [by ? `by ${by}` : '', at].filter(Boolean).join(' · ') || 'approval not recorded';
    const title = Sl && Sl.titleOf ? tryDo(() => Sl.titleOf(last), '') : '';
    const forWhat = [withLine ? esc(String(u.row && u.row.spec && (u.row.spec.designSku || '') || u.row && u.row.line && u.row.line.sku || '')) : '', esc(u.piece), u.letters ? `<span class="ordSealLetters">${esc(u.letters)}</span>` : ''].filter(Boolean).join(' · ');
    return `<span class="ordSeal" data-kind="${u.kind}"${u.face ? ` data-face="${u.face}"` : ''} data-testid="order-seal"${title ? ` title="${esc(title)}"` : ''}>${seals}<span class="ordSealWords"><b>${WORD[u.kind][u.face]} approved</b>${forWhat ? `<span class="ordSealFor">${forWhat}</span>` : ''}<span class="ordSealBy">${esc(say)}</span></span></span>`;
  }
  /** The strip: every approved piece and face of the order in line order, and "1 of 2 approved" when the order has more than one. '' when nothing is approved. */
  function html(model) {
    if (!model || !model.approved) return '';
    const done = model.units.filter(u => u.done), lines = new Set(model.units.map(u => u.line)), withLine = lines.size > 1;
    const parts = done.map(u => unitHtml(u, withLine)).filter(Boolean); if (!parts.length) return '';
    const all = model.approved === model.total, count = model.total > 1 ? `<span class="ordSealsCount" data-complete="${all ? 1 : 0}"><b>${model.approved} of ${model.total}</b> approved</span>` : '';
    return `<div class="ordSeals" data-testid="order-seals" data-approved="${model.approved}" data-total="${model.total}" role="group" aria-label="${esc(`${model.approved} of ${model.total} approved: engraving and monogram seals`)}">${count}<div class="ordSealList">${parts.join('')}</div></div>`;
  }
  return { of, html, sig, engraveUnits, monoUnits };
});
