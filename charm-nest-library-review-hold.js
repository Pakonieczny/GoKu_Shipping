/* Charm Nest · a held sheet, told where the app already tells what holds a sheet back (window.LibraryReviewHold).
   Paul, 12 Oct 2026: "If an order goes for review and is already on a sheet then keep that order on that sheet and just hold the sheet until the user
   resolves the issue within the Review tab ... you will need to inform the user whenever this happens ... especially in the library tab if a sheet is being
   held back. The user needs to be informed as to exactly what order is holding it back so the user can easily fix it."

   What is said, and where (one sentence, the same everywhere: "Held by order 4100000001 (left earring) in Review"; the piece is only named when the order
   has more than one; several orders: "Held by orders 4100000001 (left earring) and 4100000002 in Review"; a press on an order number opens that order in
   the Review tab, the one-tap way to fix it):
     · the Library's Approve button: its grey reason line (charm-nest-bridge.js approveCase / setCase ask why(sheetIds); the line is the one tap to Review when
       one order holds the sheet, else it opens the '!' list as every other reason does)
     · the Library's '!' list: the order is a row of the list that already exists (charm-nest-library-issues.js), its chip says "In Review" and the piece, a press
       opens the Review tab (that file asks rows(sheetId) for the piece, and lists the order itself if the readiness did not)
     · under a held sheet's small rail in the Library when its own Approve line does not say it (a sheet of a set, a sheet in Laser cutting): decorate(card)
     · the sheet window, above its orders: paint(host, sheetId)
     · the Nest sheet card, on the row where it already says what is wrong with the sheet (nestHtml(sheetId), drawn by renderCard)
   It clears at once: the hold's own change callback (CharmNestReviewHold.onChange, REVHOLD) and the Review mark's event ('cn-review-mark', REVMARK) redraw the
   Library's cards and its '!' list, the sheet window's line and the Nest cards; nothing here keeps a poller, a listener on the cloud or a copy of the hold.

   What it does not do. It never decides what is held: that is CharmNestReviewHold.heldBy(sheetId) -> [{ rid, name, reason, piece, poolIds, sheetId }], derived from
   the Review mark every time (so it is also true after a reload and on a second computer). Without that API on the page every function here answers "nothing is
   held" and draws nothing. It never writes: no order, sheet, seal or record is changed here, and no network call is made. */
(function (root) {
  'use strict';
  const doc = root.document;
  if (!doc || root.LibraryReviewHold) return;

  const esc = t => String(t == null ? '' : t).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const warn = (what, e) => { try { console.warn('Review hold: ' + what, e); } catch (_) {} };

  /* ═══ the hold, as the rest of the page reads it ═══ */
  const api = () => { const H = root.CharmNestReviewHold; return H && typeof H.heldBy === 'function' ? H : null; };
  const tidyPiece = t => String(t == null ? '' : t).replace(/\s+/g, ' ').replace(/^[\s(:·\-–—]+|[\s):·\-–—.]+$/g, '').slice(0, 40);
  /** The orders holding one sheet back: [{ rid, piece, name, reason }], one per order, [] when it is not held (or the hold API is not on the page). */
  function rows(sheetId) {
    const H = api(); if (!H || !sheetId) return [];
    let list = null;
    try { list = H.heldBy(String(sheetId)); } catch (e) { warn('read', e); }
    if (!Array.isArray(list)) return [];
    const seen = new Set(), out = [];
    for (const x of list) {
      const rid = x && x.rid != null ? String(x.rid).trim() : ''; if (!rid || seen.has(rid)) continue;
      seen.add(rid); out.push({ rid, swap: x.swap === true, piece: tidyPiece(x.piece), name: String(x.name || ''), reason: String(x.reason || '') });
    }
    return out;
  }
  /** The orders holding any of these sheets (a set's sheets): one row per order, in the order the sheets are given; each knows its first sheet. */
  function ofSheets(ids) {
    const seen = new Set(), out = [];
    for (const id of [].concat(ids || [])) for (const r of rows(id)) if (!seen.has(r.rid)) { seen.add(r.rid); out.push(Object.assign({ sheetId: String(id) }, r)); }
    return out;
  }

  /* ═══ the words ═══ */
  /* Two kinds of hold carry the same words but one ending (REVHOLD): the order is in the Review tab ("... in Review"), or a person's pick answered it and the new drawing is not on the
     sheet yet (row.swap: "...: new drawing chosen, swap it on the sheet"). A sheet held by both says the in-Review ones first; the swap ones are said once those are resolved. */
  const SWAP_WORDS = 'new drawing chosen, swap it on the sheet';
  const who = r => r.rid + (r.piece ? ` (${r.piece})` : '');
  const kindOf = rs => { const rv = rs.filter(r => !r.swap); return rv.length ? { rows: rv, swap: false } : { rows: rs, swap: true }; };
  // the orders named: one; two "A and B"; three "A, B and C"; more "A, B and N more" (N counts the rest)
  const shownOf = rows => rows.length > 3 ? { rows: rows.slice(0, 2), more: rows.length - 2 } : { rows, more: 0 };
  const joinNames = (parts, more) => more ? `${parts.join(', ')} and ${more} more` : parts.length > 1 ? `${parts.slice(0, -1).join(', ')} and ${parts[parts.length - 1]}` : parts[0];
  /** "Held by order 4100000001 (left earring) in Review" (a swap: "Held by order 4100000001: new drawing chosen, swap it on the sheet") */
  function lineOf(rs) {
    if (!rs.length) return '';
    const k = kindOf(rs), sh = shownOf(k.rows);
    return `Held by ${k.rows.length === 1 ? 'order' : 'orders'} ${joinNames(sh.rows.map(who), sh.more)}${k.swap ? ': ' + SWAP_WORDS : ' in Review'}`;
  }
  /** The one line for a sheet ('' when it is not held). */
  const line = sheetId => lineOf(rows(sheetId));
  /** For the Approve button's reason line: { text, rids, sheetId } for the sheets given (a set's), or null. sheetId = the first sheet that is held (its '!' is the shortcut). */
  function why(ids) {
    const rs = ofSheets(ids); if (!rs.length) return null;
    const k = kindOf(rs);   // (the orders the line names: one of them is the one tap)
    return { text: lineOf(rs), rids: k.rows.map(r => r.rid), sheetId: k.rows[0].sheetId };
  }

  /* ═══ opening the order in the Review tab ═══ */
  function open(rid) {
    rid = String(rid == null ? '' : rid).trim(); if (!rid) return false;
    const H = root.CharmNestReviewHold, M = root.CharmNestReviewMark;
    try { if (H && typeof H.openInReview === 'function' && H.openInReview(rid) !== false) return true; } catch (e) { warn('open', e); }
    try { if (M && typeof M.open === 'function' && M.open(rid) !== false) return true; } catch (e) { warn('open', e); }
    try { if (typeof root.setMode === 'function') { root.setMode('review'); return true; } } catch (e) { warn('open', e); }
    return false;
  }

  /* ═══ the note: the line with its order numbers pressable ═══ */
  // one order: the whole line is the press; several: each order number is
  function inner(rs) {
    if (!rs.length) return '';
    const k = kindOf(rs);
    if (k.rows.length === 1) { const r = k.rows[0]; return `<button type="button" class="rvhGo whole" data-rvh-open="${esc(r.rid)}" title="${esc('Open order ' + r.rid + ' in Review')}">${esc(lineOf(rs))}</button>`; }
    const sh = shownOf(k.rows), part = r => `<button type="button" class="rvhGo" data-rvh-open="${esc(r.rid)}" title="${esc('Open order ' + r.rid + ' in Review')}">${esc(r.rid)}</button>${r.piece ? esc(` (${r.piece})`) : ''}`;
    return `Held by orders ${joinNames(sh.rows.map(part), sh.more)}${k.swap ? esc(': ' + SWAP_WORDS) : ' in Review'}`;
  }
  /** The Nest sheet card's inline note ('' when the sheet is not held). */
  function nestHtml(sheetId) {
    const rs = rows(sheetId); if (!rs.length) return '';
    css(); wire();
    return `<span class="rvhNest" data-rvh-nest="${esc(sheetId)}">${inner(rs)}</span>`;
  }
  /** A host element (the sheet window's line) shows the note for one sheet, hidden when there is none. It is redrawn when the hold changes. */
  function paint(host, sheetId) {
    if (!host) return;
    css();
    host.setAttribute('data-rvh-host', ''); host.dataset.rvhSheet = String(sheetId || '');
    const html = sheetId ? inner(rows(sheetId)) : '';
    if (host.dataset.rvhSig !== html) { host.dataset.rvhSig = html; host.innerHTML = html; }
    host.hidden = !html;
  }

  /* ═══ under a held sheet's rail in the Library ═══ */
  /* A sheet that has its own Approve button says it on that button's reason line (bridge.js marks the box data-hold); every other held sheet (a sheet of a set, whose set
     has one button for all, a sheet already in Laser cutting) says it here, directly under its small rail. The note goes when the hold does. */
  function decorate(card) {
    if (!card || !card.querySelectorAll) return;
    wire();
    const keep = new Set();
    for (const box of card.querySelectorAll('.flowBox')) {
      const m = /^sheet:(.+)$/.exec(box.dataset.flowFor || ''), host = box.parentElement; if (!m || !host) continue;
      const id = m[1], rs = rows(id), own = host.querySelector(':scope > .approveBox'), said = !!(own && own.dataset.hold && own.dataset.mode === 'blocked');
      let note = host.querySelector(':scope > .rvhNote[data-rvh-sheet="' + id.replace(/["\\]/g, '') + '"]');
      if (!rs.length || said) { if (note) note.remove(); continue; }
      css();
      const html = inner(rs);
      if (!note) { note = doc.createElement('div'); note.className = 'rvhNote'; note.dataset.rvhSheet = id; note.dataset.sig = ''; box.after(note); }
      else if (note.previousElementSibling !== box) box.after(note);
      if (note.dataset.sig !== html) { note.dataset.sig = html; note.innerHTML = html; }
      keep.add(note);
    }
    for (const n of card.querySelectorAll('.rvhNote')) if (!keep.has(n)) n.remove();
  }

  /* ═══ a press on an order number: the Review tab ═══ */
  doc.addEventListener('click', ev => {
    const t = ev.target && ev.target.closest ? ev.target.closest('[data-rvh-open]') : null; if (!t) return;
    ev.preventDefault(); ev.stopPropagation();
    open(t.getAttribute('data-rvh-open'));
  }, true);

  /* ═══ it clears at once: the hold's change, the Review mark's change ═══ */
  let wired = false, unsub = null, timer = 0, every = false;
  const pending = new Set();
  function wire() {
    if (wired) return;
    const H = root.CharmNestReviewHold; if (!H || typeof H.onChange !== 'function') return;
    wired = true;
    try { unsub = H.onChange(d => changed(d)); } catch (e) { wired = false; warn('onChange', e); }
  }
  function changed(d) {
    const ids = d && Array.isArray(d.sheetIds) ? d.sheetIds : null;
    if (ids && ids.length) ids.forEach(i => pending.add(String(i))); else every = true;
    if (!timer) timer = setTimeout(flush, 0);
  }
  function flush() {
    timer = 0;
    const ids = [...pending], all = every || !ids.length; pending.clear(); every = false;
    // the Library: its cards (rails, Approve lines, notes) and, with them, an open '!' list
    try { const L = root.LaserReview; if (L && typeof L.changed === 'function') { if (all) L.changed(); else L.changed(new Set(ids.map(i => 's:' + i))); } } catch (e) { warn('library', e); }
    try { if (root.LibraryIssues && typeof root.LibraryIssues.refresh === 'function') root.LibraryIssues.refresh(); } catch (e) { warn('issues', e); }
    // the sheet window's line
    for (const h of doc.querySelectorAll('[data-rvh-host]')) { try { paint(h, h.dataset.rvhSheet); } catch (e) { warn('sheet window', e); } }
    // the Nest sheet cards that say it now or said it before
    try {
      if (typeof allSheets === 'function' && typeof root.renderCard === 'function') {
        for (const sh of allSheets()) {
          if (!sh || !sh.el) continue;
          const id = sh.sheetId ? String(sh.sheetId) : '';
          if (all ? (sh.el.querySelector('.rvhNest') || (id && rows(id).length)) : (ids.includes(id) && (sh.el.querySelector('.rvhNest') || rows(id).length))) root.renderCard(sh);
        }
      }
    } catch (e) { warn('nest cards', e); }
  }
  doc.addEventListener('cn-review-mark', () => { wire(); changed(null); });
  wire();
  if (!wired && doc.readyState === 'loading') doc.addEventListener('DOMContentLoaded', wire, { once: true });
  root.addEventListener('load', wire, { once: true });

  /* ═══ the look: the Approve reason line's own type, a small clay dot, nothing louder ═══ */
  function css() {
    if (doc.getElementById('rvhCss')) return;
    const s = doc.createElement('style'); s.id = 'rvhCss';
    // (the small clay dot is the box's own background, not an inline piece: a one-tap line is a button, which cannot break across lines and would drop under a dot that stands in front of it)
    s.textContent = `.rvhNote,.rvhLine,.rvhNest{display:block;min-width:0;padding-left:12px;font:500 11.5px/1.35 var(--sans,system-ui,sans-serif);color:var(--ink70,#5b554c);background:radial-gradient(circle at 3px .74em,var(--clay,#b0563f) 0 3px,transparent 3.6px) no-repeat}
.rvhNote{margin:2px 0 4px;animation:rvhIn .22s ease both}
.rvhLine{margin:10px 14px 0}.rvhLine[hidden]{display:none}
.rvhNest{font-size:inherit;color:inherit}
.rvhGo{all:unset;box-sizing:border-box;cursor:pointer;font:inherit;color:inherit;border-radius:4px}
.rvhGo.whole{display:block}
.rvhGo.whole::after{content:"\\203A";margin-left:5px;color:var(--ink45,#938c80)}
.rvhGo:hover,.rvhGo:focus-visible{color:var(--ink,#1c1a17);text-decoration:underline;text-decoration-thickness:1px;text-underline-offset:3px}
.rvhGo:focus-visible{outline:2px solid var(--gold,#a9823f);outline-offset:2px}
@keyframes rvhIn{from{opacity:0;transform:translateY(-2px)}to{opacity:1;transform:none}}
@media (prefers-reduced-motion:reduce){.rvhNote{animation:none}}`;
    (doc.head || doc.documentElement).appendChild(s);
  }

  root.LibraryReviewHold = { rows, ofSheets, line, lineOf, why, open, decorate, paint, nestHtml, changed, isOn: () => !!api() };
})(typeof self !== 'undefined' ? self : this);
