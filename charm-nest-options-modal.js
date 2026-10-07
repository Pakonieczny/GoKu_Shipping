/* The Options Studio (Paul, 7 Oct 2026): "There is no point of having these two messy and crowded uncombined pop-up options. Completely
   redesign the options menu, make it a large almost full screen pop-up and consolidate all of the features and options so it is much
   easier to understand and there are no sub menus. Everything on one page, clearly visible, large card views. Also display the greyed
   out previously cut away pieces so I can see the full history of the sheet, with a very clearly visible green line in between each
   cut ... I should be able to search the history of all partial sheets in this pop-up. Beautifully designed, well spaced, organized;
   the UI philosophy similar to the other large modal pop-ups."

   What this file is: the window itself and the two cards that are new. The first two cards are not drawn here:
     1. THIS SHEET   the controls the Gate draws (charm-nest-bridge.js renderRelease: Include, Sheet dimensions, Merge sheets; charm-nest-rose-ui.js:
                     Cut contour allowance and the stock choice). They live in ONE element (the box, node._optBox, .solidOptions) that stays in
                     the sheet card's gate node while the window is closed and is MOUNTED in this window while it is open, so every hook
                     (data-solid, data-rose), every handler and every repaint is the very same code: a repaint replaces nothing, so it never
                     steals the focus or a typed value.
     2. PARTIAL SHEETS  the card charm-nest-partial-ui.js (PartialSheetsUI) keeps in that same box: the metal's available partial sheets, the answer
                     in place (Use this one / Cancel), the chain, the policy as two large cards.
     3. SHEET HISTORY    (here) the full original physical sheet with every earlier cut-away piece greyed and a green dashed line between the
                     cuts (OptionsHistory, charm-nest-options-history.js), the timeline of who cut when, from PartialSheets.history.
     4. ALL PARTIAL SHEETS  (here) one search over every partial sheet of every metal and status: ONE list (PartialSheets.searchAll), filtered in
                     the browser (OptionsHistory.filter); the results are PartialSheetsUI's own cards; a press shows that sheet's history in card 3.

   API:  OptionsStudio.open({ sh, node, box, opener, title }) -> { close, el }     (the Options button of a card: Gate.openOptions)
         OptionsStudio.close(metal, { focus }) -> Promise (settles once the window is gone)
         OptionsStudio.isOpen(metal?)   OptionsStudio.sync(sh, node, title)   (every draw of the controls: the title and the history follow)
         OptionsStudio.release(box)     (the controls are being drawn anew for another sheet: the window is put away at once)

   The look is the app's own: a native modal <dialog> (as the Sheet window and the shared-orders window are), their backdrop, radius, shadow, serif
   title, header with a metal swatch and a close x, the focus kept inside, Esc, the focus back on the Options button, a short rise (180 ms) and
   the cards coming in one after another; prefers-reduced-motion shows only a fade. A small labelled spinner for every wait. The page's toasts sit
   under a modal window, so what they say is also said here, in the window's own note.

   Cost (the Google bill): the window opens with the partial sheets list the card always read (one call, answered from the cache or a
   one-document probe), ONE history call for the sheet it shows (PartialSheets.history caches it by stock and revision) and ONE search list
   (PartialSheets.searchAll, a revision probe), read when the search card comes into view. Typing, the chips and the "show more" read nothing:
   they filter that list in the browser. No timer, no polling. It never writes anything; the buttons are the cards' own. */
(function (root) {
  'use strict';
  if (root.OptionsStudio) return;
  const doc = root.document;
  const EASE = 'cubic-bezier(.2,.8,.2,1)';
  const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const warn = (what, e) => { try { console.warn('[options window] ' + what + ':', e && e.message || e); } catch (_) {} };
  const reduced = () => { try { return !!((root.Motion && root.Motion.reduced && root.Motion.reduced()) || (root.matchMedia && root.matchMedia('(prefers-reduced-motion: reduce)').matches)); } catch (_) { return false; } };
  const plural = (n, one, many) => `${n} ${n === 1 ? one : many || one + 's'}`;
  const num = n => Math.round(n * 10) / 10;
  const CODE = { rose: 'RG', gold10k: '10K', gold14k: '14K' };
  const COLOR = { rose: '#c08578', gold10k: '#b08d2a', gold14k: '#d9b545' };
  const PS = () => root.PartialSheets || null;
  const PUI = () => root.PartialSheetsUI || null;
  const OH = () => root.OptionsHistory || null;
  const ICON = {
    close: '<svg viewBox="0 0 16 16" aria-hidden="true"><path d="M4 4l8 8M12 4l-8 8" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round"/></svg>',
    search: '<svg viewBox="0 0 16 16" aria-hidden="true"><circle cx="7" cy="7" r="4.3" fill="none" stroke="currentColor" stroke-width="1.5"/><path d="m10.3 10.3 3.2 3.2" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"/></svg>'
  };
  const spin = (text, block) => `<span class="osBusy${block ? ' center' : ''}" role="status"><span class="osSpin" aria-hidden="true"></span>${esc(text)}</span>`;
  const asHtml = v => typeof v === 'string' ? v : v && typeof v.outerHTML === 'string' ? v.outerHTML : '';

  let CUR = null;   // the window that is open (one at a time)
  /* the search's one list, kept for the page's life (PartialSheets keeps its own cache and revision, too: a second opening is a probe) */
  const ALL = { items: null, loading: false, error: '', more: false, q: '', metal: '', status: '', shown: 24 };
  const PAGE = 24;
  const METAL_CHIPS = [['', 'All'], ['rose', 'RG'], ['gold10k', '10K'], ['gold14k', '14K']];
  const STATUS_CHIPS = [['available', 'Available'], ['inUse', 'In use'], ['used', 'Used'], ['discarded', 'Discarded']];

  const colorOf = m => { try { const x = root.CN && root.CN.METALS && root.CN.METALS.find(k => k.key === m); if (x && x.color) return x.color; } catch (_) {} return COLOR[m] || '#938c80'; };
  const titleOf = sh => { try { return `${root.CN && root.CN.labelOf ? root.CN.labelOf(sh.metal) : CODE[sh.metal]} · Sheet ${sh.page || 1}`; } catch (_) { return 'Sheet options'; } };

  /* ── the history card's data: the stock this sheet holds (or was cut from) ── */
  const ownArg = sh => {   // (the revision is the hint PartialSheets.history caches by: a sheet that knows it is answered from the cache with no call)
    const id = (sh.roseStock && sh.roseStock.id) || (sh.recalled && sh.recalled.roseStockId), rev = sh.roseStock && Number.isFinite(+sh.roseStock.revision) ? +sh.roseStock.revision : Number.isFinite(+sh.roseRevision) ? +sh.roseRevision : null;
    if (id) return rev != null ? { stockId: id, revision: rev } : { stockId: id };
    if (sh.roseCutAt && sh.sheetId) return { sheetId: sh.sheetId };
    return null;
  };
  const argKey = a => (a ? JSON.stringify(a) : '');
  const stockOfItem = c => c.stockId || String(c.id || '').replace(/-\d+$/, '');

  /* ── the dialog ── */
  function openDialog(dlg) { if (typeof dlg.showModal === 'function') dlg.showModal(); else dlg.setAttribute('open', ''); }
  function closeDialog(dlg) { try { if (typeof dlg.close === 'function') { if (dlg.open) dlg.close(); } else dlg.removeAttribute('open'); } catch (_) { try { dlg.removeAttribute('open'); } catch (_) {} } }

  function build(M) {
    const dlg = M.dlg; dlg.className = 'osDlg'; dlg.setAttribute('data-no-grow', ''); dlg.dataset.metal = M.m; dlg.tabIndex = -1;
    dlg.setAttribute('aria-labelledby', 'osTitle-' + M.m);
    dlg.innerHTML = `<div class="osBox"><header class="osHead"><div class="osId"><span class="osMetal" style="--c:${esc(colorOf(M.m))}">${esc(CODE[M.m] || '')}</span><div class="osTitle"><h2 id="osTitle-${esc(M.m)}">${esc(M.title)}</h2><span>Options, partial sheets and sheet history</span></div></div><button type="button" class="osX" data-os="close" aria-label="Close options">${ICON.close}</button></header><div class="osBody"></div><div class="osNote" role="status" aria-live="polite"></div></div>`;
    M.body = dlg.querySelector('.osBody'); M.note = dlg.querySelector('.osNote'); M.titleEl = dlg.querySelector('.osTitle h2');
  }

  /** What a toast says is said here, too (a toast sits under a modal window). */
  function watchToasts(M) {
    const host = doc.getElementById('toasts'); if (!host || typeof root.MutationObserver !== 'function') return;
    const say = (text, kind) => {
      if (!text) return; M.note.textContent = text; M.note.dataset.kind = kind || ''; M.note.classList.add('on');
      clearTimeout(M.noteT); M.noteT = setTimeout(() => M.note.classList.remove('on'), kind === 'bad' ? 9000 : 5200);
    };
    const mo = new root.MutationObserver(recs => { for (const r of recs) for (const n of r.addedNodes) if (n.nodeType === 1 && n.classList && n.classList.contains('toast')) say(n.dataset.msg || n.textContent, n.dataset.kind); });
    mo.observe(host, { childList: true });
    M.offs.push(() => { try { mo.disconnect(); } catch (_) {} clearTimeout(M.noteT); });
  }

  function wire(M) {
    const dlg = M.dlg;
    dlg.addEventListener('pointerdown', e => { M.pressedInside = e.target !== dlg; }, true);
    dlg.addEventListener('click', e => {
      if (e.target === dlg) { if (!M.pressedInside) close(M.m); return; }
      const t = e.target.closest && e.target.closest('[data-os]'); if (!t || !dlg.contains(t)) return;
      const k = t.dataset.os;
      if (k === 'close') return void close(M.m);
      if (k === 'own') return void showOwn(M);
      if (k === 'hretry') return void (M.h && M.h.arg ? showHistory(M, M.h.arg, { own: M.h.own, item: M.h.item, force: true }) : showOwn(M));
      if (k === 'reload') return void loadAll(M, { force: true });
      if (k === 'more') { ALL.shown += PAGE; return void paintAll(M); }
      if (k === 'older') return void loadAll(M, { more: true });
      if (k === 'clear') { ALL.q = ''; ALL.metal = ''; ALL.status = ''; ALL.shown = PAGE; const i = M.cards.all.querySelector('input[type=search]'); if (i) i.value = ''; return void paintAll(M); }
      if (k === 'metal') { ALL.metal = t.dataset.v || ''; ALL.shown = PAGE; return void paintAll(M); }
      if (k === 'status') { ALL.status = ALL.status === t.dataset.v ? '' : t.dataset.v; ALL.shown = PAGE; return void paintAll(M); }
    });
    // a press on a card of the search shows that sheet's history in card 3 (the partial sheets card keeps its own press: it asks what Use this one does)
    M.cards.all.addEventListener('click', e => {
      const card = e.target.closest && e.target.closest('.psCard'); if (!card) return;
      const c = (ALL.items || []).find(x => String(x.id) === card.dataset.id); if (!c) return;
      showHistory(M, Number.isFinite(+c.revision) ? { stockId: stockOfItem(c), revision: +c.revision } : { stockId: stockOfItem(c) }, { item: c, select: c.revision });
      paintAll(M);
      try { M.cards.history.scrollIntoView({ block: 'start', behavior: reduced() ? 'auto' : 'smooth' }); } catch (_) {}
    });
    let typing = 0;
    M.cards.all.addEventListener('input', e => {
      const i = e.target; if (!i || !i.matches || !i.matches('input[type=search]')) return;
      clearTimeout(typing); typing = setTimeout(() => { ALL.q = i.value; ALL.shown = PAGE; paintAll(M); }, 110);   // (the browser filters the one list: nothing is read per keystroke)
    });
    M.cards.all.addEventListener('focusin', () => loadOnce(M));
    dlg.addEventListener('keydown', e => {
      if (e.key === 'Tab') {   // the focus stays in the window: Tab from the last control goes to the first, Shift+Tab from the first to the last
        const f = [...dlg.querySelectorAll('button:not([disabled]),input:not([disabled]),select:not([disabled]),[tabindex]:not([tabindex="-1"])')].filter(el => el.getClientRects().length && !el.closest('[hidden]'));
        if (f.length) {
          const a = doc.activeElement, first = f[0], last = f[f.length - 1], outside = !dlg.contains(a) || a === dlg;
          if (e.shiftKey && (a === first || outside)) { e.preventDefault(); last.focus(); }
          else if (!e.shiftKey && (a === last || outside)) { e.preventDefault(); first.focus(); }
        }
        return;
      }
      // Esc: an answer that waits (or the choosing of another partial sheet) is put away first (PartialSheetsUI handles it inside its card), then the window
      if (e.key === 'Escape' && !e.defaultPrevented) {
        e.preventDefault(); e.stopPropagation();
        const t = e.target;   // (Esc in the search box with words in it clears them first, as a search box does)
        if (t && t.matches && t.matches('input[type=search]') && t.value) { t.value = ''; ALL.q = ''; ALL.shown = PAGE; paintAll(M); return; }
        try { if (PUI() && PUI().escape && PUI().escape(M.m)) return; } catch (_) {}
        close(M.m);
      }
    });
    dlg.addEventListener('cancel', e => { e.preventDefault(); if (M.closing || M.done) return; try { if (PUI() && PUI().escape && PUI().escape(M.m)) return; } catch (_) {} close(M.m); });
    dlg.addEventListener('close', () => { if (!M.done) finish(M); });   // (closed by the browser itself)
  }

  /* ── card 3: the sheet history ── */
  function historyCard(M) {
    const c = doc.createElement('section'); c.className = 'osCard osHistory'; c.dataset.card = 'history'; c.setAttribute('aria-labelledby', 'osH-' + M.m);
    c.innerHTML = `<header class="osCardHead"><div><h3 class="osCardTitle" id="osH-${esc(M.m)}">Sheet history</h3><p class="osLead" data-os="hlead">Every cut made on a physical sheet, oldest first. The pieces cut away stay grey, and a green dashed line marks each cut.</p></div></header><div class="osHistWho" data-os="hwho" hidden></div><div data-os="hbody" aria-live="polite"></div>`;
    return c;
  }
  const q = (el, k) => el.querySelector(`[data-os="${k}"]`);

  /** Show the history of `arg` ({ stockId } or { sheetId }): one call (PartialSheets.history caches it by stock and revision). o: { own, item, select }. */
  function showHistory(M, arg, o = {}) {
    const seq = ++M.hseq, P = PS();
    M.h = { arg, key: argKey(arg), own: !!o.own, item: o.item || null, sel: o.select != null ? o.select : null, data: null, loading: true, error: '' };
    paintHistory(M);
    if (!P || typeof P.history !== 'function') { M.h.loading = false; M.h.error = 'The sheet history is not available on this page yet.'; paintHistory(M); return; }
    Promise.resolve().then(() => P.history(arg, { force: !!o.force })).then(d => {
      if (M.done || M.hseq !== seq) return;
      M.h.loading = false; M.h.data = d && d.stock ? d : d && d.history ? d.history : d; M.h.error = '';
      if (M.h.sel != null) { const cuts = (M.h.data && M.h.data.cuts) || [], at = cuts.findIndex(c => c.revision === M.h.sel); M.h.sel = at >= 0 ? at + 1 : null; }   // (a partial sheet picked below: the cut that left it is lit; the timeline counts 1, 2, 3 ...)
      paintHistory(M);
    }).catch(e => { if (M.done || M.hseq !== seq) return; M.h.loading = false; M.h.error = (e && e.message) || String(e); paintHistory(M); });
  }
  function showOwn(M) {
    const arg = ownArg(M.sh);
    if (arg) { showHistory(M, arg, { own: true }); return; }
    ++M.hseq; M.h = { own: true, none: true, key: '' }; paintHistory(M);
  }
  function paintHistory(M) {
    const card = M.cards.history, body = q(card, 'hbody'), who = q(card, 'hwho'), lead = q(card, 'hlead'), h = M.h; if (!h) return;
    const LEAD = 'Every cut made on a physical sheet, oldest first. The pieces cut away stay grey, and a green dashed line marks each cut.';
    who.hidden = true; who.innerHTML = '';
    if (h.none) { lead.textContent = LEAD; body.innerHTML = `<div class="osEmpty"><b>This sheet is not on a physical sheet yet.</b><span>When it takes one, every cut made on that sheet shows here. Pick any partial sheet in the search below to see its history now.</span></div>`; return; }
    if (h.loading) { lead.textContent = LEAD; body.innerHTML = spin('Reading the sheet history…', true); return; }
    if (h.error) { body.innerHTML = `<div class="osEmpty bad"><b>Could not read the sheet history: ${esc(h.error)}</b><button type="button" class="btn ghost xs" data-os="hretry">Try again</button></div>`; return; }
    const data = h.data || {}, stock = data.stock || {}, cuts = Array.isArray(data.cuts) ? data.cuts : [];
    const code = CODE[stock.metal] || stock.code || '', size = Number.isFinite(+stock.wMm) ? `${num(+stock.wMm)} × ${num(+stock.hMm)} mm` : '';
    lead.textContent = [code && size ? `${code} sheet, ${size}` : size, plural(cuts.length, 'cut'), stock.ownerSheetName ? `held by ${stock.ownerSheetName}` : cuts.length ? 'not held by a sheet now' : ''].filter(Boolean).join(' · ') + '. The pieces cut away stay grey; a green dashed line marks each cut.';
    if (!h.own) { who.hidden = false; who.innerHTML = `<span>The sheet picked in the search below${h.item ? ': <b>' + esc((h.item.sourceSheet || 'Partial sheet') + (h.item.sourceSet ? ' · ' + h.item.sourceSet : '')) + '</b>' : ''}.</span>${ownArg(M.sh) ? '<button type="button" class="osLinkBtn" data-os="own">Back to this sheet</button>' : ''}`; }
    const X = OH();
    if (!X || (typeof X.view !== 'function' && typeof X.svg !== 'function')) { body.innerHTML = `<div class="osEmpty"><b>The history picture is not ready on this page yet.</b><span>Reload the page to get it.</span></div>`; return; }
    try {
      // the drawing is about as wide as its column of the card (labels are sized for it); a narrow window stacks it over the timeline
      const cw = body.clientWidth || 0, width = Math.max(320, Math.min(900, cw ? (cw >= 900 ? Math.round((cw - 24) * 1.8 / 2.8) : cw) : 720));
      const o = { selected: h.sel, width };
      body.innerHTML = typeof X.view === 'function' ? X.view(data, o)
        : `<div class="osHist"><div class="osHistDraw">${asHtml(X.svg(data, o))}</div><div class="osHistSide">${typeof X.timeline === 'function' ? asHtml(X.timeline(data, o)) : ''}</div></div>`;
      if (typeof X.bind === 'function') X.bind(body.firstElementChild, data, { selected: h.sel, onSelect: n => { h.sel = n; } });
    } catch (e) { warn('history', e); body.innerHTML = `<div class="osEmpty bad"><b>The sheet history could not be drawn.</b></div>`; }
  }

  /* ── card 4: all partial sheets ── */
  function allCard(M) {
    const c = doc.createElement('section'); c.className = 'osCard osAll'; c.dataset.card = 'all'; c.setAttribute('aria-labelledby', 'osA-' + M.m);
    const chips = (k, list, label) => `<div class="osChips" role="group" aria-label="${label}"><span class="lbl">${label}</span>${list.map(([v, w]) => `<button type="button" class="osChip" data-os="${k}" data-v="${v}" aria-pressed="false">${w}</button>`).join('')}</div>`;
    c.innerHTML = `<header class="osCardHead"><div><h3 class="osCardTitle" id="osA-${esc(M.m)}">All partial sheets</h3><p class="osLead">Every partial sheet of every metal, whatever became of it. Search by sheet, set, person, date (oct 5), metal, status or size, then press one to see its history above.</p></div><button type="button" class="btn ghost xs" data-os="reload" title="Read the list again">Refresh</button></header>`
      + `<div class="osFilters"><label class="osSearch">${ICON.search}<input type="search" autocomplete="off" spellcheck="false" placeholder="Search partial sheets" aria-label="Search all partial sheets"></label>`
      + chips('metal', METAL_CHIPS, 'Metal') + chips('status', STATUS_CHIPS, 'Status') + `</div>`
      + `<p class="osCount" data-os="count" role="status"></p><div class="psList osResults" data-os="results" aria-live="polite"></div><button type="button" class="btn ghost osMore" data-os="more" hidden>Show more</button><button type="button" class="btn ghost osMore" data-os="older" hidden>Load older partial sheets</button>`;
    c.querySelector('input[type=search]').value = ALL.q;   // (the words typed last time are still the filter: the box shows them)
    return c;
  }
  function fallbackFilter(items, query, o = {}) {   // (OptionsHistory.filter is the real one; this keeps the card working until that file is on the page)
    const toks = String(query || '').toLowerCase().split(/\s+/).filter(Boolean);
    const hay = c => [c.sourceSheet, c.sourceSet, c.cutBy, c.lastUsedBy, c.lastUsedSheet, c.inUseBySheetName, c.usedBySheetName, c.metal, CODE[c.metal], c.status, num(+c.wMm), num(+c.hMm)].join(' ').toLowerCase();
    return items.filter(c => (!o.metal || c.metal === o.metal) && (!o.status || c.status === o.status) && toks.every(t => hay(c).includes(t)));
  }
  function filtered() {
    const X = OH(), items = ALL.items || [], o = { metal: ALL.metal, status: ALL.status };
    try { return X && typeof X.filter === 'function' ? X.filter(items, ALL.q, o) : fallbackFilter(items, ALL.q, o); } catch (e) { warn('filter', e); return fallbackFilter(items, ALL.q, o); }
  }
  function paintAll(M) {
    const card = M.cards.all, res = q(card, 'results'), count = q(card, 'count'), more = q(card, 'more'), older = q(card, 'older'), reload = q(card, 'reload');
    for (const b of card.querySelectorAll('[data-os="metal"]')) b.setAttribute('aria-pressed', String((b.dataset.v || '') === ALL.metal));
    for (const b of card.querySelectorAll('[data-os="status"]')) b.setAttribute('aria-pressed', String(b.dataset.v === ALL.status));
    reload.disabled = ALL.loading; more.hidden = true; older.hidden = true;
    if (!ALL.items) {
      count.textContent = '';
      res.innerHTML = ALL.error ? `<div class="osEmpty bad"><b>Could not read the partial sheets: ${esc(ALL.error)}</b><button type="button" class="btn ghost xs" data-os="reload">Try again</button></div>` : spin('Reading all partial sheets…', true);
      return;
    }
    const list = filtered(), total = ALL.items.length, active = !!(ALL.q || ALL.metal || ALL.status), shown = list.slice(0, ALL.shown), P = PUI();
    count.innerHTML = (ALL.loading ? spin('Reading again…') : '') + `<span>${active ? `${list.length} of ${plural(total, 'partial sheet')}` : plural(total, 'partial sheet')}${ALL.more ? ', the newest first' : ''}</span>${active ? '<button type="button" class="osLinkBtn" data-os="clear">Clear</button>' : ''}`;
    if (!list.length) {
      res.innerHTML = `<div class="osEmpty"><b>${total ? 'No partial sheet matches.' : 'No partial sheets have been made yet.'}</b><span>${total ? 'Try fewer words, another date or size, or clear the filters.' : 'When a green line is cut on a Rose Gold, 10K or 14K sheet, the metal that is left is saved here.'}</span></div>`;
      return;
    }
    const sel = M.h && M.h.item && M.h.item.id;
    const focused = doc.activeElement && res.contains(doc.activeElement) ? doc.activeElement.dataset.id : null;
    res.innerHTML = P && P.card ? shown.map((c, i) => P.card(c, { selected: c.id === sel }, i)).join('') : '';
    if (focused) { const f = [...res.querySelectorAll('.psCard')].find(x => x.dataset.id === focused); if (f) f.focus({ preventScroll: true }); }
    more.hidden = list.length <= shown.length; more.textContent = `Show ${Math.min(PAGE, list.length - shown.length)} more`;
    older.hidden = !(ALL.more && !ALL.loading && list.length <= shown.length);   // (every one read is on show: the server has older ones: one more call, on a press)
  }
  async function loadAll(M, o = {}) {
    const P = PS();
    if (!P || typeof P.searchAll !== 'function') { ALL.error = 'The list of all partial sheets is not available on this page yet.'; paintAll(M); return; }
    if (ALL.loading) return;
    ALL.loading = true; ALL.error = ''; paintAll(M);
    try { const r = await P.searchAll({ force: !!o.force, more: !!o.more }); ALL.items = Array.isArray(r) ? r : (r && r.items) || []; ALL.more = !!(r && r.more); }
    catch (e) { ALL.error = (e && e.message) || String(e); }
    finally { ALL.loading = false; if (CUR && !CUR.done) paintAll(CUR); }   // (the window that is open now: it may not be the one that asked)
  }
  /** The list is read once, when the search comes into view (or is touched): a window that is closed again at the top reads nothing for it. */
  function loadOnce(M) { if (M.allAsked || M.done) return; M.allAsked = true; if (M.io) { try { M.io.disconnect(); } catch (_) {} M.io = null; } loadAll(M); }

  /* ── open / close ── */
  function stagger(M) {
    if (reduced()) return;
    [...M.box.children].forEach((c, i) => { try { c.animate([{ opacity: 0, transform: 'translateY(12px)' }, { opacity: 1, transform: 'none' }], { duration: 320, delay: 70 + i * 70, easing: EASE, fill: 'backwards' }); } catch (_) {} });
  }
  function open(o) {
    try {
      o = o || {}; const sh = o.sh, box = o.box; if (!sh || !box || !doc) return null;
      if (CUR && !CUR.done) { if (CUR.box === box) { try { CUR.dlg.focus({ preventScroll: true }); } catch (_) {} return handle(CUR); } finish(CUR); }
      const M = { m: sh.metal, sh, node: o.node || null, box, home: box.parentNode, opener: o.opener || doc.activeElement, title: o.title || titleOf(sh), dlg: doc.createElement('dialog'), done: false, closing: null, hseq: 0, h: null, offs: [], cards: {}, allAsked: false, io: null };
      build(M);
      M.cards.history = historyCard(M); M.cards.all = allCard(M);
      doc.body.appendChild(M.dlg);
      M.body.appendChild(box); box.hidden = false;
      box.appendChild(M.cards.history); box.appendChild(M.cards.all);
      wire(M); watchToasts(M);
      CUR = M;
      try { openDialog(M.dlg); } catch (e) { M.dlg.setAttribute('open', ''); }
      try { M.dlg.focus({ preventScroll: true }); } catch (_) {}   // (it rests on the window itself: arrows or Tab go on from here)
      stagger(M);
      try { PUI() && PUI().open(M.m); } catch (e) { warn('partial sheets', e); }
      paintAll(M); showOwn(M);
      if (typeof root.IntersectionObserver === 'function') {
        try { M.io = new root.IntersectionObserver(es => { if (es.some(e => e.isIntersecting)) loadOnce(M); }, { root: M.body, rootMargin: '260px' }); M.io.observe(M.cards.all); }
        catch (_) { M.io = null; loadOnce(M); }
      } else loadOnce(M);
      return handle(M);
    } catch (e) { warn('open', e); return null; }
  }
  const handle = M => ({ el: M.dlg, close: () => close(M.m), isOpen: () => !M.done });

  function close(m, o = {}) {
    const M = CUR; if (!M || M.done || (m && M.m !== m)) return Promise.resolve();
    if (M.closing) return M.closing;
    M.focusBack = o.focus !== false;
    M.dlg.classList.add('closing');
    M.closing = new Promise(res => setTimeout(() => { finish(M); res(); }, reduced() ? 100 : 160));
    return M.closing;
  }
  function finish(M) {
    if (M.done) return; M.done = true;
    for (const off of M.offs.splice(0)) { try { off(); } catch (_) {} }
    if (M.io) { try { M.io.disconnect(); } catch (_) {} M.io = null; }
    try { PUI() && PUI().close(M.m); } catch (_) {}
    // the controls go back where the Gate drew them, hidden, exactly as they were (the Gate keeps repainting them there)
    for (const c of [M.cards.history, M.cards.all]) { try { c.remove(); } catch (_) {} }
    try { M.box.hidden = true; if (M.home) M.home.appendChild(M.box); } catch (_) {}
    try { const R = root.Gate && root.Gate.state && root.Gate.state(); if (R) (R.optionsOpen || (R.optionsOpen = {}))[M.m] = false; } catch (_) {}
    closeDialog(M.dlg); try { M.dlg.remove(); } catch (_) {}
    if (CUR === M) CUR = null;
    if (M.focusBack !== false) {
      let op = M.opener; if (!op || !op.isConnected || !op.focus || op === doc.body) op = M.node && M.node.querySelector && M.node.querySelector('.sheetOptionsBtn');
      try { if (op && op.isConnected) op.focus({ preventScroll: true }); } catch (_) {}
    }
  }
  /** The controls are being drawn anew (another sheet in this card): the window is put away at once; nothing in it was unsaved. */
  function release(box) { if (CUR && !CUR.done && CUR.box === box) finish(CUR); }
  /** Every draw of the controls: the title and the history of the sheet shown follow the sheet (no read unless the sheet's physical sheet changed). */
  function sync(sh, node, title) {
    const M = CUR; if (!M || M.done || M.node !== node) return;
    M.sh = sh;
    if (title && title !== M.title) { M.title = title; if (M.titleEl) M.titleEl.textContent = title; }
    if (M.h && M.h.own && !M.h.loading) { const a = ownArg(sh), k = argKey(a); if (k !== M.h.key && (a || !M.h.none)) showOwn(M); }
  }

  root.OptionsStudio = { open, close, isOpen: m => !!CUR && !CUR.done && (!m || CUR.m === m), sync, release, state: () => ({ cur: CUR, all: ALL }) };
})(window);
