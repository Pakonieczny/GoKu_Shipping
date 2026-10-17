/* charm-nest-monogram-seals.js — the monogram's approval seal(s), wherever a monogram PIECE is listed or described outside its own widget (MONOSEAL2, Paul, 17 Oct 2026: "ensure that the monogram stamp and seal animations appear in all the same
 * places as the engraving stamp and seal animations already do so that way everything is 100% consistent").
 *
 *   The Engrave tab's list row of a monogram piece (bridge monoPlacementRow, the `.egPlacementSeals` cell of the engraved row)  ·  the order window's Overview "Monogram" fact/chip (bridge factsOf)
 *   ·  the Sheet tab and the Sheet window's piece panel (bridge paintPanel, charm-nest-sheetwin.js renderEng: a monogram piece has no job, so it gets this panel and not CNEngravingSeals.panel).
 *
 * A monogram is NOT an Engrave job. NEVER feed a monogram (or its Engrave-list stand-in, bridge monoItemsNow) to CNEngravingSeals.list/html/keep/panel: it invents a legacy "BACK ENGRAVING" seal at time 0 for any object whose
 * `state` is approved / written / skipped and which carries no seals. Everything here reads the monogram's OWN seals and draws them with the reader's own markup (or Seal.html with the monogram family): the result never says BACK ENGRAVING.
 *
 * THE ADAPTER. The seals themselves are made by charm-nest-monogram-ui.js (the monogram family of charm-nest-motion.js Seal, the stamp { id: 'monogramApproved:<at>:<by>', how: 'monogramApproved', at, by, [n, of, letters] }, and the two readers
 * `CharmNestMonogramUI.sealsOf(row, slot)` -> [stamp] and `CharmNestMonogramUI.sealHtml(row, slot)` -> the html of a `.sealRow` of them). READ below is the ONE place that names them: a rename at merge time is a change of that one object.
 * A page without them (an older build, a test) shows no monogram seal anywhere and nothing else changes.
 *
 * Nothing here writes, stamps or animates anything: these are the seals AT REST (a freshly pressed seal is pressed in the monogram widget, at its Approve button). */
(function (root, factory) {
  const api = factory(root); api.make = factory;
  if (typeof module === 'object' && module.exports) module.exports = api; else if (root) root.CNMonogramSeals = api;
})(typeof window !== 'undefined' ? window : null, function (root) {
  'use strict';
  /** THE ONE PLACE the monogram widget's seal readers and its stamp are named. */
  const READ = { seals: 'sealsOf', html: 'sealHtml', how: 'monogramApproved', family: 'monogram' };
  const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const ui = () => (root && root.CharmNestMonogramUI) || null;
  const call = (name, ...args) => { const U = ui(), f = U && U[name]; if (typeof f !== 'function') return undefined; try { return f.apply(U, args); } catch (_) { return undefined; } };
  const lineOf = row => (row && row.parentRow) || row || null;
  /** Is this order line a monogram line (the orders module read it as one)? */
  const isMonogram = row => { const r = lineOf(row); return !!(r && r.spec && r.spec.special && r.spec.special.monogram); };
  /** The reader exists (the widget module has been merged with the monogram seals)? */
  const readable = () => { const U = ui(); return !!(U && typeof U[READ.seals] === 'function'); };

  /** The pieces of a monogram line, [{ key, slot, n, of, lineKey, poolId }]; a line of one piece is { slot: null, n: 1, of: 1, key: the line's key }; [] for a line that is no monogram. */
  function pieces(row) {
    const r = lineOf(row); if (!isMonogram(r)) return [];
    const U = ui(), svc = U && typeof U.service === 'function' ? (() => { try { return U.service(); } catch (_) { return null; } })() : null;
    let ps = null; try { ps = svc && typeof svc.pieces === 'function' ? svc.pieces(r) : null; } catch (_) { ps = null; }
    return ps && ps.length ? ps.map(p => ({ key: p.key, slot: p.slot || null, n: p.n || 1, of: p.of || 1, lineKey: p.lineKey || r.key, poolId: p.poolId || '' })) : [{ key: r.key, slot: null, n: 1, of: 1, lineKey: r.key, poolId: r.key + '_1' }];
  }
  /** The piece of a monogram line that a charm on a sheet is (by its pool id "<lineKey>_<n>"); null when the line is no monogram. A pool id that names no piece reads as the line's first piece. */
  function pieceFor(row, poolId) {
    const r = lineOf(row); if (!isMonogram(r)) return null;
    const ps = pieces(r), id = String(poolId || '');
    const pc = (id && ps.find(p => p.poolId === id)) || (id && ps.find(p => p.n === (+id.split('_').pop() || 0))) || ps[0];
    return Object.assign({ row: r }, pc);
  }
  /** The widget service's own status of a piece: { state, label, letters, approved, sent, ... } or null. */
  const status = (row, slot) => { const r = lineOf(row); if (!isMonogram(r)) return null; return call('status', r, slot || undefined) || null; };

  /** The seals of one piece, oldest first, from the reader: [{ id, how, at, by, [n, of, letters] }]. [] when there are none, or the page has no reader. Nothing is invented. */
  function list(row, slot) {
    const r = lineOf(row); if (!r || !isMonogram(r)) return [];
    const got = call(READ.seals, r, slot || undefined);
    if (!Array.isArray(got)) return [];
    const seen = new Set(), out = [];
    for (const s of got) {
      if (!s || typeof s !== 'object' || s.how !== READ.how) continue;
      const id = String(s.id || `${s.how}:${+s.at || 0}:${s.by || ''}`);
      if (seen.has(id)) continue; seen.add(id);
      out.push(Object.assign({}, s, { id, at: +s.at || 0, by: String(s.by || '') }));
    }
    return out.sort((a, b) => a.at - b.at || a.id.localeCompare(b.id));
  }
  /** A short text that changes exactly when the seals shown do (a list row paints again only then). */
  const signature = seals => JSON.stringify((seals || []).map(s => [s.id, s.at, s.by]));
  /** The time a monogram piece was last approved (0 when it has no seal): what the Engrave lists sort and date-filter it by, as a job's approvedAt. */
  const lastAt = seals => (seals || []).reduce((t, s) => Math.max(t, +s.at || 0), 0);

  /** The seals of one piece as markup: the reader's own `.sealRow` (kept inside one `.monoSeals` wrapper, which the page's CSS sizes like the engraving seals), '' when the piece has none. */
  function html(row, slot) {
    const seals = list(row, slot); if (!seals.length) return '';
    const r = lineOf(row), U = ui();
    let inner = '';
    if (U && typeof U[READ.html] === 'function') { try { inner = String(U[READ.html](r, slot || undefined) || ''); } catch (_) { inner = ''; } }
    // (a widget that has the family but not the html reader: the seals are drawn by Seal itself; a Seal without the monogram family would draw another kind of seal, so then nothing)
    if (!inner && root && root.Seal && root.Seal.FAMILY && root.Seal.FAMILY[READ.family] && typeof root.Seal.html === 'function') {
      const size = root.Seal.BASE_SIZE || 50;
      inner = `<span class="sealRow monogramSeals" data-seal-group data-seal-count="${seals.length}" role="group" aria-label="Monogram approval history">${seals.map(s => root.Seal.html(s, size, 'monogramSeal')).join('')}</span>`;
    }
    return inner ? `<span class="monoSeals" data-testid="mono-seals" data-seal-count="${seals.length}">${inner}</span>` : '';
  }

  /* ── the Overview's "Monogram" fact (the order window): the line's piece(s) and their seals, the way the Engraving fact says an engraved line ── */
  const spaced = v => String(v || '').split('').join(' ');
  /** { html, approved, text } for a monogram line that has at least one seal; null for any other line (a monogram nobody approved shows no fact, as an unengraved line shows none). */
  function facts(row) {
    const r = lineOf(row); if (!isMonogram(r)) return null;
    const ps = pieces(r), many = ps.length > 1;
    const rows = ps.map(p => ({ p, seals: list(r, p.slot), st: status(r, p.slot) }));
    if (!rows.some(x => x.seals.length)) return null;
    const isDone = x => (x.st ? !!x.st.approved : x.seals.length > 0), approved = rows.every(isDone);   // (the service's word when it has one, else the piece has a seal)
    const lines = rows.map(x => {
      const done = isDone(x), letters = x.st && x.st.letters ? spaced(x.st.letters) : '';
      const word = done ? 'approved' : String((x.st && x.st.label) || 'to approve').toLowerCase();
      return (many ? `Piece ${x.p.n} of ${x.p.of} · ` : '') + word + (letters ? ' · ' + letters : '');
    });
    return { approved, text: lines.join(' / '), html: rows.map((x, i) => `<span>${esc(lines[i])}</span>${html(r, x.p.slot)}`).join('') };
  }

  /* ── the piece panel of the Sheet tab and the Sheet window ── (the back engraving card's own look and words, charm-nest-engraving-seals.js panel(): the same `.swEng.egCard`, pill, title line, words, buttons and seal history) */
  /** The panel of one monogram piece: p = pieceFor(row, poolId). Approved: "Approved by X" + its seal(s); not yet: "Monogram waits for approval in Engraving"; both with a View in Engraving button. */
  function panel(p, o) {
    o = o || {};
    if (!p || !p.row) return '';
    const seals = list(p.row, p.slot), st = status(p.row, p.slot);
    const done = st ? !!st.approved : seals.length > 0, by = [...seals].reverse().map(s => s.by).find(Boolean) || '';
    const letters = st && st.letters ? spaced(st.letters) : (seals.map(s => s.letters).filter(Boolean).pop() || '');
    const pillText = done ? 'Approved' : (st && st.label) || 'Waiting', pillS = done ? 'approved' : st && ['letters', 'error'].includes(st.state) ? 'words' : 'approve';
    const why = done ? 'The monogram is approved' : 'Monogram waits for approval in Engraving';
    const sub = done ? (by ? `Approved by ${by}` : '') : '';
    const forPiece = p.of > 1 ? `<span class="egFor"><b>Piece ${p.n} of ${p.of}</b></span>` : '';
    const sealsHtml = html(p.row, p.slot), compact = !!o.compact;
    const whyHtml = `<div class="egWhy"><b>${esc(why)}</b>${sub ? `<span class="by egSub"><span>${esc(sub)}</span></span>` : ''}</div>`;
    // (an approved piece: its seal(s) stand beside "Approved by X" as a signature; a piece that waits keeps any earlier approval's seals below, as the engraving card does)
    return `${compact ? '' : '<span class="fLabel">Monogram</span>'}<div class="swEng egCard monoCard${compact ? ' egCompact' : ''}" data-state="${done ? 'approved' : 'approve'}" data-mono-panel="${esc(p.key || '')}" data-testid="mono-panel">`
      + `<div class="top egTop">${forPiece}<span class="egPill" data-s="${pillS}"><i aria-hidden="true"></i>${esc(pillText)}</span></div>`
      + (done && sealsHtml ? `<div class="egSealLine">${whyHtml}${sealsHtml}</div>` : whyHtml)
      + (letters ? `<div class="egWords"><span class="egLbl">The letters</span><div class="words">${esc(letters)}</div></div>` : '')
      + `<div class="acts"><button type="button" class="btn ghost sm egOpen" data-e="monogram">View in Engraving <span aria-hidden="true">→</span></button></div>`
      + (!done && sealsHtml ? `<div class="egHistory">${sealsHtml}</div>` : '') + '</div>';
  }
  /** The View in Engraving button of a panel: the host's own `open(button)` runs (EngraveLink.openMonogram); the wait is said on the button with a small spinner, as the engraving card's button does. */
  function wire(host, h) {
    h = h || {};
    const btn = host && host.querySelector ? host.querySelector('[data-e=monogram]') : null; if (!btn) return;
    btn.addEventListener('click', () => {
      if (btn.disabled || !h.open) return;
      const was = btn.innerHTML; let run;
      try { run = h.open(btn); } catch (err) { if (root && root.console) root.console.warn('monogram panel: open', err); return; }
      if (!run || typeof run.then !== 'function') return;
      btn.disabled = true; btn.setAttribute('aria-busy', 'true'); btn.innerHTML = '<span class="spin"></span>Opening Engraving…';
      const back = () => { if (btn.isConnected) { btn.disabled = false; btn.removeAttribute('aria-busy'); btn.innerHTML = was; } };
      run.then(back, err => { if (root && root.console) root.console.warn('monogram panel: open', err); back(); });
    });
  }
  /** The way into the Engraving tab for a piece: EngraveLink.openMonogram({ rid, key }) (the order window / Sheet window close first, `closeFirst`), else Engrave.openMonogram(key). Resolves true when it was asked. */
  function openIn(p, o) {
    o = o || {};
    const link = root && root.EngraveLink, E = root && root.Engrave, rid = String(o.rid || (p.row && p.row.order && p.row.order.receiptId) || '');
    if (link && typeof link.openMonogram === 'function') return Promise.resolve(link.openMonogram(Object.assign({ rid, key: p.key }, typeof o.closeFirst === 'function' ? { closeFirst: o.closeFirst } : {})));
    if (E && typeof E.openMonogram === 'function') return Promise.resolve(typeof o.closeFirst === 'function' ? o.closeFirst() : null).then(() => { E.openMonogram(p.key); return true; });
    return Promise.resolve(false);
  }

  return { READ, isMonogram, readable, pieces, pieceFor, status, list, signature, lastAt, html, facts, panel, wire, openIn };
});
