/* charm-nest-order-rows.js — ONE ROW PER ORDER ITEM in the order window's "Its pieces" list (Paul, 12 Oct 2026, image 1: "Each order should only take up one line
 * and you need to amalgamate multi charm orders, including all earring orders, so that in the UI it is all showing under one line and not split up ... stay
 * minimalist ... hide options that are not immediately necessary behind hover states or drop-down menus").
 *
 * An earring pair (matching or mismatched) used to be a LEFT row and a RIGHT row; now it is ONE row (the name once, a small Left | Right marker, the metal dot, ONE status,
 * the six dots of its SLOWEST piece). A necklace of several discs is one row with its piece count, as it was. What belongs to one piece (its own status, its sheet,
 * its own dots) sits in a drawer under the row that ONE quiet chevron opens, and what is not the row's main action sits in a small "more" menu (three dots).
 * This file holds what that needs that is not the order window's own business, so the window's code stays small and these rules can be tested alone:
 *
 *   CharmNestOrderRows.kindOf(item)            "pair" | "counted" | "single"   (item: { sides, counted } as the order window's piecesOf tells it)
 *   CharmNestOrderRows.slowest(list)           index of the piece the row speaks for: the lowest step (a piece completed by hand is never the slowest unless all are), the first of a tie
 *                                              list: [{ step, hand }]
 *   CharmNestOrderRows.subLabel(kind, i, n, o) the words of a piece in the drawer: "Left" | "Right" | "Disc 2 of 3" | "Piece 2 of 3"  (o: { side, noun, qty })
 *   CharmNestOrderRows.countWord(n, noun)      "3 pieces" | "2 discs"
 *   CharmNestOrderRows.tidy(host, o)           host: the row's actions cell (.pcAct) holding the Review card's own buttons and seals (and the Hold button's slot) as the order window
 *                                              draws them. Keeps ONE primary action in the cell and moves the rest into a "more" menu (the same nodes: nothing is rebuilt, so every
 *                                              handler and data-cu-* hook still works). → { split, primary, menu }
 *       the primary one: the card's next step (Print QR label, else Complete Order); a completed piece keeps "Completed" and its seals; a piece sent to its sheet keeps "Open sheet";
 *       a piece with nothing but Hold has Hold. Whatever is in progress (a spinner, the name being asked, an Undo, a "not printed" note) stays where it is: never behind a menu.
 *       o: { label (what the menu button says it is for), icon (the three dots, as markup) }
 *   CharmNestOrderRows.mount(box, o)           once per list: the three-dots menu (opens under its button, Esc or a press elsewhere closes it, Up and Down move through it, a press on an
 *                                              item closes it) and the chevron's drawer (animated, quiet, remembered in o.open, a Set of "<order>|<line>" keys). o.rid() → the order shown.
 *   CharmNestOrderRows.apply(box)              puts every drawer as the Set says (no motion): after the list is drawn again
 * Pure but for tidy/mount/apply, which touch the nodes they are given. Loads in the page (window.CharmNestOrderRows) and in node. */
(function (root, factory) {
  const api = factory(root);
  if (typeof module === 'object' && module.exports) module.exports = api; else root.CharmNestOrderRows = api;
})(typeof self !== 'undefined' ? self : this, function (root) {
  'use strict';
  const doc = root && root.document ? root.document : null;
  const tryDo = (f, d) => { try { return f(); } catch (_) { return d; } };

  /** "pair" for a line with a Left and a Right, "counted" for a line that makes several pieces of ONE item (discs, letters, charms), else "single". */
  const kindOf = item => (item && Array.isArray(item.sides) && item.sides.length > 1 ? 'pair' : item && item.counted ? 'counted' : 'single');
  /** Which piece a row speaks for: the slowest, as the heading says "the order is where the slowest one is". A piece completed by hand waits on no step. */
  function slowest(list) {
    const ps = Array.isArray(list) ? list : []; let best = -1;
    const at = x => (x && x.hand ? Infinity : Number.isFinite(+x.step) ? +x.step : -1);
    ps.forEach((x, i) => { if (best < 0 || at(x) < at(ps[best])) best = i; });
    return best;
  }
  const countWord = (n, noun) => `${n} ${noun || 'piece'}${n === 1 ? '' : 's'}`;
  /** The drawer's words for a piece: Left, Right, Disc 2 of 3, Piece 2 of 3. */
  function subLabel(kind, i, n, o) {
    o = o || {};
    if (kind === 'pair') return (o.side === 'L' ? 'Left' : o.side === 'R' ? 'Right' : 'Piece') + (o.qty > 1 ? ` ×${o.qty}` : '');
    const noun = o.noun === 'disc' ? 'Disc' : o.noun === 'bar' ? 'Bar' : o.noun === 'star' ? 'Star' : 'Piece';   // (BARS, Paul 19 Oct 2026: a profiled listing's pieces are bars; MOONSTARS: stars)
    return `${noun} ${i + 1} of ${n}`;
  }

  /* ── the actions of a row: one primary, the rest behind the three dots ── */
  // (RELHOLD, Paul 18 Oct 2026: [data-rel-hold], the Review card's Release Hold where Complete Order stood on a piece of another Review tab's card, is a unit of its own, as Complete Order is)
  // (HOLDBTN, Paul 20 Oct 2026: the Send to Review button that stands where Hold stood, its slot or the button (ReviewSend), is the unit kind 'hold' keeps its place for; its line under it travels with it)
  const HEAD = '[data-cu-print],[data-cu-complete],[data-rel-hold],[data-cu-done],[data-cu-view-designs],[data-cu-open-sheet],[data-cu-history],[data-cu-more],[data-hold-slot],[data-hold-btn],[data-rev-slot],.revSendBtn';
  const BUSY = '.cuStat,.cuWho,[data-cu-name]';
  const headKind = n => n.matches('[data-cu-print]') ? 'print' : n.matches('[data-cu-complete]') ? 'complete' : n.matches('[data-rel-hold]') ? 'release' : n.matches('[data-cu-done]') ? 'done' : n.matches('[data-cu-view-designs]') ? 'designs'
    : n.matches('[data-cu-open-sheet]') ? 'sheet' : n.matches('[data-cu-history]') ? 'history' : n.matches('[data-cu-more]') ? 'more' : 'hold';
  /** The units of a cell: a button and the seals that rest after it travel together. */
  function unitsOf(host) {
    const out = []; let cur = null;
    for (const n of [...host.children]) {
      if (n.matches('.cuFail,.cuUndo')) { out.push({ kind: 'note', nodes: [n] }); cur = null; continue; }   // (a "not printed" note and the Undo offered for a few seconds stay in the cell, in sight)
      if (n.matches(HEAD)) { cur = { kind: headKind(n), nodes: [n] }; out.push(cur); continue; }
      if (cur) cur.nodes.push(n); else out.push({ kind: 'lead', nodes: [n] });
    }
    return out;
  }
  /** Which units stay in the cell, and which go behind the menu. */
  function planOf(units) {
    const has = k => units.find(u => u.kind === k) || null, take = (...ks) => units.filter(u => ks.includes(u.kind));
    const stay = take('note', 'lead');
    let main = [], rest = [];
    if (has('done')) { main = take('done', 'more'); rest = take('print', 'hold', 'complete', 'release', 'designs', 'sheet', 'history'); }
    else if (has('print') || has('complete') || has('release')) { const p = has('print') || has('complete') || has('release'); main = [p].concat(take('more')); rest = units.filter(u => !stay.includes(u) && !main.includes(u)); }
    else if (has('sheet') || has('designs') || has('history')) { main = take('sheet', 'more'); rest = take('designs', 'history', 'hold', 'print', 'complete', 'release'); }
    else { main = take('hold', 'more'); rest = []; }
    return { stay, main, rest };
  }
  function tidy(host, o) {
    o = o || {};
    if (!host || !host.children) return { split: false, primary: 0, menu: 0 };
    // (something is happening, a name is being asked: the cell is left exactly as the Review card's code drew it)
    if (host.querySelector(BUSY)) return { split: false, primary: host.children.length, menu: 0 };
    const units = unitsOf(host), plan = planOf(units);
    if (!plan.rest.length) return { split: false, primary: plan.main.length + plan.stay.length, menu: 0 };
    const d = host.ownerDocument, wrap = d.createElement('span'), btn = d.createElement('button'), menu = d.createElement('div');
    wrap.className = 'owPcMenuWrap'; wrap.setAttribute('data-no-select', '');
    btn.type = 'button'; btn.className = 'owPcMoreBtn'; btn.setAttribute('aria-haspopup', 'menu'); btn.setAttribute('aria-expanded', 'false');
    btn.setAttribute('aria-label', o.label ? 'More for ' + o.label : 'More actions'); btn.title = o.label ? 'More for ' + o.label : 'More actions';
    btn.innerHTML = o.icon || '<span aria-hidden="true">···</span>';
    menu.className = 'owPcMenu'; menu.setAttribute('role', 'menu'); menu.hidden = true;
    for (const u of plan.rest) {
      const item = d.createElement('div'); item.className = 'owPcMenuItem'; item.setAttribute('role', 'none'); item.dataset.unit = u.kind;
      for (const n of u.nodes) item.appendChild(n);
      menu.appendChild(item);
    }
    wrap.append(btn, menu);
    // (the three dots stand right after the main action, before anything that stays in the cell)
    const last = plan.main.length ? plan.main[plan.main.length - 1].nodes.slice(-1)[0] : null;
    if (last && last.parentNode === host) host.insertBefore(wrap, last.nextSibling); else host.appendChild(wrap);
    return { split: true, primary: plan.main.length + plan.stay.length, menu: plan.rest.length };
  }

  /* ── the page: the menu and the drawer, wired once on the list ── */
  const reduced = () => tryDo(() => root.matchMedia('(prefers-reduced-motion: reduce)').matches, false);
  function closeMenus(box, except) {
    for (const w of box.querySelectorAll('.owPcMenuWrap.open')) {
      if (w === except) continue;
      w.classList.remove('open', 'up', 'lft'); const b = w.querySelector('.owPcMoreBtn'), m = w.querySelector('.owPcMenu');
      if (b) b.setAttribute('aria-expanded', 'false'); if (m) m.hidden = true;
      const row = w.closest('.owPcRow'); if (row) row.classList.remove('menuOpen');
    }
  }
  function openMenu(box, w) {
    closeMenus(box, w);
    const b = w.querySelector('.owPcMoreBtn'), m = w.querySelector('.owPcMenu'); if (!b || !m) return;
    m.hidden = false; w.classList.add('open'); b.setAttribute('aria-expanded', 'true');
    const row = w.closest('.owPcRow'); if (row) row.classList.add('menuOpen');
    // (downward by default; upward when the list's scroller has no room below the button)
    let sc = null; for (let e = box.parentElement; e && e !== doc.body; e = e.parentElement) { const oy = getComputedStyle(e).overflowY; if ((oy === 'auto' || oy === 'scroll') && e.scrollHeight > e.clientHeight + 1) { sc = e; break; } }
    const br = b.getBoundingClientRect(), mr = m.getBoundingClientRect(), lim = sc ? sc.getBoundingClientRect().bottom : (root.innerHeight || 800);
    w.classList.toggle('up', br.bottom + mr.height + 6 > lim && br.top - mr.height - 6 > (sc ? sc.getBoundingClientRect().top : 0));
    // (its right edge is the button's; where that would put its left edge outside the window or the screen, as on a phone where the button is near the left, its left edge is the button's)
    w.classList.remove('lft'); const left = Math.max(0, sc ? sc.getBoundingClientRect().left : 0); if (m.getBoundingClientRect().left < left + 6) w.classList.add('lft');
  }
  const isOpen = (o, key) => !!(o && o.open && o.open.has(key));
  const keyOf = (box, o, line) => `${(o && o.rid ? o.rid() : box.dataset.rid) || ''}|${line}`;
  function paintDrawer(box, chev, drawer, open, quick) {
    chev.setAttribute('aria-expanded', open ? 'true' : 'false'); chev.classList.toggle('open', open);
    chev.title = open ? 'Hide each piece' : 'Show each piece';
    const row = chev.closest('.owPcRow'); if (row) row.classList.toggle('drawerOpen', open);
    if (drawer.getAnimations) drawer.getAnimations().forEach(a => a.cancel());
    if (quick || reduced() || !drawer.animate) { drawer.hidden = !open; drawer.style.height = ''; drawer.style.overflow = ''; return; }
    if (open) {
      drawer.hidden = false; drawer.style.overflow = 'hidden';
      const h = drawer.scrollHeight, a = drawer.animate([{ height: '0px', opacity: 0 }, { height: h + 'px', opacity: 1 }], { duration: 170, easing: 'cubic-bezier(.2,.8,.2,1)' });
      const done = () => { drawer.style.overflow = ''; drawer.style.height = ''; }; a.onfinish = done; a.oncancel = done;
    } else {
      drawer.style.overflow = 'hidden';
      const h = drawer.offsetHeight, a = drawer.animate([{ height: h + 'px', opacity: 1 }, { height: '0px', opacity: 0 }], { duration: 140, easing: 'ease-in' });
      a.onfinish = () => { drawer.hidden = true; drawer.style.overflow = ''; drawer.style.height = ''; };
      a.oncancel = () => { drawer.style.overflow = ''; drawer.style.height = ''; };
    }
  }
  /** Every drawer of the list as the Set of open ones says, with no motion. */
  function apply(box, o) {
    if (!box || !box.querySelectorAll) return;
    for (const ch of box.querySelectorAll('.owPcX[data-line]')) {
      const dr = box.querySelector(`#${ch.getAttribute('aria-controls')}`); if (!dr) continue;
      paintDrawer(box, ch, dr, isOpen(o || box._rowsOpts, keyOf(box, o || box._rowsOpts, ch.dataset.line)), true);
    }
  }
  function mount(box, o) {
    if (!box || box._rowsWired || !doc) return; box._rowsWired = true; box._rowsOpts = o || {};
    box.addEventListener('click', e => {
      const t = e.target; if (!t || !t.closest) return;
      const more = t.closest('.owPcMoreBtn');
      if (more && box.contains(more)) { e.stopPropagation(); const w = more.closest('.owPcMenuWrap'); if (w.classList.contains('open')) closeMenus(box); else { openMenu(box, w); if (e.detail === 0) { const f = w.querySelector('.owPcMenu button:not([disabled])'); if (f) f.focus({ preventScroll: true }); } } return; }   // (opened from the keyboard: the first item has the focus)
      const item = t.closest('.owPcMenu .owPcMenuItem');
      if (item && box.contains(item)) { if (t.closest('button, a')) setTimeout(() => closeMenus(box), 0); return; }
      const ch = t.closest('.owPcX[data-line]');
      if (ch && box.contains(ch)) {
        e.stopPropagation(); const dr = box.querySelector(`#${ch.getAttribute('aria-controls')}`); if (!dr) return;
        const key = keyOf(box, box._rowsOpts, ch.dataset.line), open = !isOpen(box._rowsOpts, key);
        if (box._rowsOpts.open) { if (open) box._rowsOpts.open.add(key); else box._rowsOpts.open.delete(key); }
        paintDrawer(box, ch, dr, open, false);
        if (box._rowsOpts.onToggle) tryDo(() => box._rowsOpts.onToggle(ch.dataset.line, open));
      }
    }, true);
    // the menu closes for a press anywhere else, and for Esc (which then does not close the window it is in)
    doc.addEventListener('pointerdown', e => { if (box.querySelector('.owPcMenuWrap.open') && !(e.target.closest && e.target.closest('.owPcMenuWrap'))) closeMenus(box); }, true);
    root.addEventListener('keydown', e => {
      const open = box.querySelector('.owPcMenuWrap.open'); if (!open) return;
      if (e.key === 'Escape') { e.preventDefault(); e.stopImmediatePropagation(); const b = open.querySelector('.owPcMoreBtn'); closeMenus(box); if (b) b.focus({ preventScroll: true }); return; }
      if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
        const items = [...open.querySelectorAll('.owPcMenu button:not([disabled])')]; if (!items.length) return;
        e.preventDefault(); const i = items.indexOf(doc.activeElement), j = e.key === 'ArrowDown' ? (i + 1) % items.length : (i <= 0 ? items.length - 1 : i - 1);
        items[j].focus({ preventScroll: true });
      }
    }, true);
    box.addEventListener('focusout', e => { const w = e.target.closest && e.target.closest('.owPcMenuWrap.open'); if (w && !(e.relatedTarget && w.contains(e.relatedTarget)) && !(e.relatedTarget && e.relatedTarget.closest && e.relatedTarget.closest('.owPcMenuWrap'))) setTimeout(() => { if (w.classList.contains('open') && !w.contains(doc.activeElement)) closeMenus(box); }, 0); });
  }
  return { kindOf, slowest, subLabel, countWord, tidy, mount, apply, closeMenus, _unitsOf: unitsOf, _planOf: planOf };
});
