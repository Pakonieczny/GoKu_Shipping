/* Charm Nest · the Library's sheet cards: the back engraving above a sheet folds away (Paul, 5 Oct 2026, 03:33 UTC: "Add a colapsable
   button functionality to hide/show all of the back engraving on top of a given sheet. Add this new feature to both the Nest tab
   sheets and the Library Tab Sheets. Make the UI small and elegant so not to crowd the existing UI. By default keep it collapsed.")

   What folds is the SHELF of back-engraving thumbnails that sits above a sheet's picture on every Library card
   ([data-back-sheet] > .sheetBacks, CharmNestBacks.markup: one figure per piece). The picture, the files, the saved previews, the
   approvals, the seals and the Front | Back · engraving view are never touched: this only shows or hides the shelf.

   The control is charm-nest-engraving-toggle.js's own (EngravingToggle: the very control the Nest tab's cards carry), mounted in
   the card's header between the sheet's name and its date, where it adds no height; the shelf host follows the sheet's choice
   through EngravingToggle.follow (data-eng="open" | "closed"). The sheet's choice is the component's, by sheet id, in this page's
   memory only: every load starts collapsed, a card drawn again (the Library's live read, a search, an Undo) is drawn in its
   sheet's state, and a sheet open on the Nest tab and in the Library is one choice. Where the component is not on the page there
   is no control and the shelf shows as it always did.

   Cost: nothing per frame and nothing per card that has no engraving. A card is looked at when the Library draws it
   (LibraryDone.decorate, called by the Library for every list it draws) and when Engrave.refreshBacks rewrites a shelf
   (LibraryEngraving.sync); there is no request, no redraw of a card and no observer.

   window.LibraryEngraving: decorate(root) · sync(shelfHost) · release(card) · size() */
(function () {
  'use strict';
  const W = window, doc = document;
  const CARD = '.libCard[data-id]', SHELF = ':scope > [data-back-sheet]';
  const live = new Set();                                  // the cards that carry a control (so a card that has left the page lets go of it)
  const toggle = () => { const t = W.EngravingToggle; return t && typeof t.mount === 'function' && typeof t.follow === 'function' ? t : null; };
  const pieces = (shelf, T) => !shelf ? 0 : T && typeof T.countIn === 'function' ? T.countIn(shelf) : shelf.querySelectorAll('.backPieces figure').length;   // (one figure per piece)

  const CSS = `
.libCard>.h>.engLibHost{flex:0 0 auto;display:inline-flex;align-items:center;margin-block:-4px}
.libCard>.h>.engLibHost[hidden]{display:none}
.libCard>.h:has(>.engLibHost:not([hidden])){flex-wrap:nowrap;gap:4px}
.libCard>.h:has(>.set):has(>.engLibHost:not([hidden])) .engChev{display:none}
.libCard>.h:has(>.engLibHost:not([hidden]))>:is(.nm,.set,.tm){min-width:0;overflow:hidden;text-overflow:ellipsis}
.libCard>[data-back-sheet]{transition:margin-bottom .2s ease}
.libCard>[data-back-sheet][data-eng="closed"]{margin-bottom:-6px!important}
@media (prefers-reduced-motion:reduce){.libCard>[data-back-sheet]{transition:none}}`;
  let cssIn = false;
  function style() {
    if (cssIn || !doc.head) return; cssIn = true;
    if (doc.getElementById('engLibCss')) return;
    const s = doc.createElement('style'); s.id = 'engLibCss'; s.textContent = CSS; doc.head.appendChild(s);
  }

  /** Lets go of a card's control (the card left the page, or has no shelf any more): the shelf is shown as it always was. */
  function release(card) {
    const r = card && card._engLib; live.delete(card); if (!r) return;
    card._engLib = null;
    try { r.handle.destroy(); } catch (_) { /* gone with the card */ }
    try { r.follow.stop(); } catch (_) { /* gone with the card */ }
    if (r.host.parentNode) r.host.remove();
  }
  /** One card: its control mounted once (never when the sheet has no engraving), its count kept true, its shelf following the sheet's choice. */
  function attach(card, T) {
    const shelf = card.querySelector(SHELF); let r = card._engLib;
    if (r && (r.shelf !== shelf || !r.host.isConnected || r.host.parentElement !== card.querySelector(':scope > .h'))) { release(card); r = null; }
    if (r) {
      const n = pieces(shelf, T);
      if (n !== r.n) { r.n = n; r.handle.update(n); }
      r.follow = T.follow(shelf, r.key) || r.follow;       // (idempotent: the same host, the same sheet, nothing to do)
      return;
    }
    if (!shelf || !pieces(shelf, T)) return;                  // no engraving on this sheet: no control, the card is left as it is
    const head = card.querySelector(':scope > .h'); if (!head) return;
    style();
    const key = card.dataset.id;
    for (const old of head.querySelectorAll(':scope > .engLibHost')) old.remove();   // (a copy of a card flying somewhere brings its own, with nothing behind it)
    const host = doc.createElement('span'); host.className = 'engLibHost';
    // a press on the control opens nothing behind it: the card itself opens the sheet window; it is a button, so the grip's drag
    // (LibraryDnd) never starts from it either
    host.addEventListener('click', e => e.stopPropagation());
    head.insertBefore(host, head.querySelector(':scope > .tm'));
    const n = pieces(shelf, T);
    card._engLib = { host, handle: T.mount(host, { sheetKey: key, count: n }), follow: T.follow(shelf, key), shelf, n, key };
    live.add(card);
  }
  /** The Library has drawn (or redrawn) its cards: each sheet card whose shelf has pieces carries the control. */
  function decorate(root) {
    const T = toggle(); if (!T) return;
    root = root && root.querySelectorAll ? root : doc;
    for (const card of [...live]) if (!card.isConnected) release(card);
    const seen = new Set();
    for (const shelf of root.querySelectorAll(`${CARD}>[data-back-sheet]:not(:empty)`)) {
      const card = shelf.parentElement;
      if (card.closest('.mGhost, .dndLift')) continue;
      seen.add(card); attach(card, T);
    }
    for (const card of live) if (!seen.has(card) && card.isConnected && (root === doc || root.contains(card))) attach(card, T);   // (a shelf that has just emptied: its count goes to 0 and the control hides)
  }
  /** Engrave.refreshBacks wrote a card's shelf again: the count of its pieces follows. */
  function sync(shelfHost) {
    const T = toggle(), card = shelfHost && shelfHost.parentElement; if (!T || !card || !card.matches || !card.matches(CARD) || card.closest('.mGhost, .dndLift')) return;
    attach(card, T);
  }

  W.LibraryEngraving = { decorate, sync, release, size: () => live.size };
  // (the component may be loaded after the Library has drawn: the cards that are there get their control once it is)
  W.addEventListener('load', () => { try { decorate(doc); } catch (e) { console.warn('Library engraving', e); } });
})();
