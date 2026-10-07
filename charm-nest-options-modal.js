/* The Options Studio (Paul, 7 Oct 2026): "There is no point of having these two messy and crowded uncombined pop-up options. Completely
   redesign the options menu, make it a large almost full screen pop-up and consolidate all of the features and options so it is much
   easier to understand and there are no sub menus. Everything on one page, clearly visible, large card views ... Beautifully designed,
   well spaced, organized; the UI philosophy similar to the other large modal pop-ups."

   Round 2 (Paul, 7 Oct 2026, 17:10 UTC, after seeing it live): the two chip strips and the "Partial sheets" card are gone ("redundant and
   unnecessary complications"); "This sheet" and "All partial sheets" are ONE seamless menu with two ways to get metal: from the repository of
   partial sheets, or a NEW sheet at any size, "as many new sheets as they would like"; a sheet made by accident can be deleted, with a
   pop-up that asks why, "recorded and who deleted it", and it stays in the history with the DELETED stamp; the "Sheet history" card is gone
   too: its drawing lives in the thumbnail of each card (charm-nest-options-history.js, OptionsHistory.card).

   What this file is: the window itself and the Sheet source (the repository and the new sheet). One scrolling page of two parts:
     1. THE SHEET   the card the Gate draws (charm-nest-bridge.js renderRelease: the Sheet card; Cut contour (charm-nest-rose-ui.js) and Merge
                    sheets as small SETTINGS cards below). The "In current set" switch (osInclude) is carried up into this window's title row, top
                    right, beside the close button, while the window is open, and put back in the box when it closes (Paul, 7 Oct: the Sheet
                    dimensions card is gone, a sheet's size is still set in the app's Settings panel). They live in ONE element (the box,
                    node._optBox, .solidOptions) that stays in the sheet card's gate node while the window is closed and is MOUNTED in this
                    window while it is open, so every hook (data-solid, data-rose) and every handler is the very same code: a repaint replaces
                    nothing, so it never steals the focus or a typed value.
     2. SHEET SOURCE  (here, in that card) the rule as one compact switch (When a sheet needs more metal: reuse partial sheets automatically |
                    offer a brand new sheet at W x H: charm-nest-partial-ui.js), then two large options:
                      A. From partial sheets: the repository, ONE list of every sheet of every metal and status (PartialSheets.searchAll) filtered
                         in the browser (OptionsHistory.filter): search box, metal chips, status chips (default: this sheet's metal, Available), as
                         LARGE cards (OptionsHistory.card: the thumbnail IS the sheet's history; a press enlarges it in place). Each card: Use this one
                         (PartialSheetsUI: the answer in place, fit words, Use this one / Cancel, the re-seat) and Delete.
                      B. New sheet: width and height (5 to 500 mm), Make new sheet (PartialSheets.make): a new card in the repository, any number of them.
     DELETE asks WHY in a native pop-up above this window (the look of the app's ask pop-up): a required answer of 3 to 300 characters, who deletes (the
     signed-in person, never asked), Cancel / Delete; PartialSheets.remove(id, reason); the card then wears the red DELETED stamp under the Deleted chip.

   API:  OptionsStudio.open({ sh, node, box, opener, title }) -> { close, el }     (the Options button of a card: Gate.openOptions)
         OptionsStudio.close(metal, { focus }) -> Promise (settles once the window is gone)
         OptionsStudio.isOpen(metal?)   OptionsStudio.sync(sh, node, title)   (every draw of the controls: the title and the lock follow)
         OptionsStudio.release(box)     (the controls are being drawn anew for another sheet: the window is put away at once)

   The look is the app's own: a native modal <dialog> (as the Sheet window and the shared-orders window are), their backdrop, radius, shadow, serif
   title, header with a metal swatch and a close x, the focus kept inside, Esc, the focus back on the Options button, a short rise (180 ms) and
   the cards coming in one after another; prefers-reduced-motion shows only a fade. A small labelled spinner for every wait. The page's toasts sit
   under a modal window, so what they say is also said here, in the window's own note.

   Esc, in this order: the delete pop-up (its own dialog: it closes alone), an answer that waits (Cancel), a card enlarged in place (it folds back),
   then the window. Focus goes back to the Options button.

   Cost (the Google bill): the window opens with ONE list (PartialSheets.searchAll: a revision probe when it was read lately) and the rule
   (PartialSheets.loadPolicy: read once per page); history is read only when a card is enlarged (OptionsHistory, cached by stock and revision).
   Typing, the chips, the tabs and "show more" read nothing: they filter that list in the browser. No timer, no polling. The window writes
   only through Make new sheet (PartialSheets.make), Delete (PartialSheets.remove) and the buttons the cards already had (the claim). */
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
    search: '<svg viewBox="0 0 16 16" aria-hidden="true"><circle cx="7" cy="7" r="4.3" fill="none" stroke="currentColor" stroke-width="1.5"/><path d="m10.3 10.3 3.2 3.2" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"/></svg>',
    stack: '<svg viewBox="0 0 24 24" aria-hidden="true"><rect x="3" y="4" width="13" height="9" rx="1.6" fill="none" stroke="currentColor" stroke-width="1.6"/><path d="M8 17h11a2 2 0 0 0 2-2V8" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round"/><path d="M12 20.5h6" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" opacity=".55"/></svg>',
    plus: '<svg viewBox="0 0 24 24" aria-hidden="true"><rect x="3" y="3" width="18" height="18" rx="3" fill="none" stroke="currentColor" stroke-width="1.6" stroke-dasharray="3 2.4"/><path d="M12 8v8M8 12h8" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"/></svg>'
  };
  const spin = (text, block) => `<span class="osBusy${block ? ' center' : ''}" role="status"><span class="osSpin" aria-hidden="true"></span>${esc(text)}</span>`;

  let CUR = null;   // the window that is open (one at a time)
  /* the repository's one list, kept for the page's life (PartialSheets keeps its own cache and revision, too: a second opening is a probe) */
  const ALL = { items: null, groups: null, gen: 0, loading: false, error: '', more: false, q: '', metal: '', status: 'available', shown: 24, enlarged: null };
  const PAGE = 24;
  const METAL_CHIPS = [['', 'All'], ['rose', 'RG'], ['gold10k', '10K'], ['gold14k', '14K']];
  const STATUS_CHIPS = [['available', 'Available'], ['inUse', 'In use'], ['used', 'Used'], ['discarded', 'Discarded'], ['deleted', 'Deleted']];
  const MIN_MM = 5, MAX_MM = 500, WHY_MIN = 3, WHY_MAX = 300;

  const colorOf = m => { try { const x = root.CN && root.CN.METALS && root.CN.METALS.find(k => k.key === m); if (x && x.color) return x.color; } catch (_) {} return COLOR[m] || '#938c80'; };
  const titleOf = sh => { try { return `${root.CN && root.CN.labelOf ? root.CN.labelOf(sh.metal) : CODE[sh.metal]} · Sheet ${sh.page || 1}`; } catch (_) { return 'Sheet options'; } };
  const stockOfItem = c => c.stockId || String(c.id || '').replace(/-\d+$/, '');
  const statusOf = c => c.status || 'available';
  /** The signed-in person, the way every other action gets it (nobody types a name). */
  const whoName = () => { try { return String((root.CNEmployee && root.CNEmployee.name && root.CNEmployee.name()) || (root.B && root.B.employee) || '').trim(); } catch (_) { return ''; } };
  const sizeWords = c => `${num(+c.sheetWMm || +c.wMm || 0)} × ${num(+c.sheetHMm || +c.hMm || 0)} mm`;
  const nameOf = c => (PUI() && PUI().cardName ? PUI().cardName(c) : c.kind === 'new' ? 'New sheet ' + sizeWords(c) : [c.sourceSheet, c.sourceSet].filter(Boolean).join(' · ') || 'Partial sheet');
  const toast = (m, k) => { try { root.CN && root.CN.toast && root.CN.toast(m, k); } catch (_) {} };

  /* ── the repository: groups (one per physical sheet, its newest record is the sheet as it is now), the browser's filter ── */
  const when = c => Math.max(0, ...[c.cutAt, c.lastUsedAt, c.usedAt, c.inUseAt, c.madeAt, c.deletedAt].map(v => +v || 0));
  function fallbackGroups(items) {   // (OptionsHistory.groups is the real one; this keeps the repository working until that file has it)
    const by = new Map();
    for (const c of items) {
      const k = stockOfItem(c), g = by.get(k);
      if (!g) by.set(k, { stockId: k, latest: c, history: null });
      else if ((+c.revision || 0) > (+g.latest.revision || 0) || ((+c.revision || 0) === (+g.latest.revision || 0) && when(c) > when(g.latest))) g.latest = c;
    }
    return [...by.values()];
  }
  function groupsOf() {
    if (ALL.groups) return ALL.groups;
    const X = OH(), items = ALL.items || []; let g = null;
    if (X && typeof X.groups === 'function') { try { g = X.groups(items); } catch (e) { warn('groups', e); } }
    if (!Array.isArray(g)) g = fallbackGroups(items);
    g = g.filter(x => x && x.latest).slice().sort((a, b) => when(b.latest) - when(a.latest));
    return (ALL.groups = g);
  }
  function fallbackFilter(items, query, o = {}) {   // (OptionsHistory.filter is the real one)
    const toks = String(query || '').toLowerCase().split(/\s+/).filter(Boolean);
    const hay = c => [c.sourceSheet, c.sourceSet, c.cutBy, c.lastUsedBy, c.lastUsedSheet, c.inUseBySheetName, c.usedBySheetName, c.madeBy, c.deletedBy, c.deletedReason, c.kind === 'new' ? 'new sheet' : '', c.metal, CODE[c.metal], c.status, num(+c.wMm), num(+c.hMm)].join(' ').toLowerCase();
    return items.filter(c => (!o.metal || c.metal === o.metal) && (!o.status || c.status === o.status) && toks.every(t => hay(c).includes(t)));
  }
  const filterItems = (items, query, o) => { const X = OH(); try { return X && typeof X.filter === 'function' ? X.filter(items, query, o) : fallbackFilter(items, query, o); } catch (e) { warn('filter', e); return fallbackFilter(items, query, o); } };
  /** The sheets on show: the metal and status chips judge the sheet as it is now (its newest record); the words may match any record of its life (a person, a set, a date that cut it). */
  function visible() {
    const groups = groupsOf(); let hit = null;
    if (ALL.q.trim()) hit = new Set(filterItems(ALL.items || [], ALL.q, {}).map(stockOfItem));
    return groups.filter(g => (!ALL.metal || g.latest.metal === ALL.metal) && (!ALL.status || statusOf(g.latest) === ALL.status) && (!hit || hit.has(g.stockId)));
  }
  const defaultView = M => !ALL.q && ALL.metal === M.m && ALL.status === 'available';
  /** A card the window changed itself (made, deleted): the list is updated in place from the answer, with no read. */
  function putItem(item) {
    if (!item || !item.id) return false;
    const list = (ALL.items || []).slice(), at = list.findIndex(x => x.id === item.id);
    if (at >= 0) list[at] = { ...list[at], ...item }; else list.unshift(item);
    ALL.items = list; ALL.groups = null; ALL.gen++;
    return true;
  }

  /* ── the dialog ── */
  function openDialog(dlg) { if (typeof dlg.showModal === 'function') dlg.showModal(); else dlg.setAttribute('open', ''); }
  function closeDialog(dlg) { try { if (typeof dlg.close === 'function') { if (dlg.open) dlg.close(); } else dlg.removeAttribute('open'); } catch (_) { try { dlg.removeAttribute('open'); } catch (_) {} } }

  function build(M) {
    const dlg = M.dlg; dlg.className = 'osDlg'; dlg.setAttribute('data-no-grow', ''); dlg.dataset.metal = M.m; dlg.tabIndex = -1;
    dlg.setAttribute('aria-labelledby', 'osTitle-' + M.m);
    dlg.innerHTML = `<div class="osBox"><header class="osHead"><div class="osId"><span class="osMetal" style="--c:${esc(colorOf(M.m))}">${esc(CODE[M.m] || '')}</span><div class="osTitle"><h2 id="osTitle-${esc(M.m)}">${esc(M.title)}</h2><span>Sheet, partial sheets and settings</span></div></div><div class="osIncSlot"></div><button type="button" class="osX" data-os="close" aria-label="Close options">${ICON.close}</button></header><div class="osBody"></div><div class="osNote" role="status" aria-live="polite"></div></div>`;
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

  const q = (el, k) => el.querySelector(`[data-os="${k}"]`);

  /* ── the Sheet source: the two options, the repository (A) and the new sheet (B) ── */
  function sourceEl(M) {
    const m = M.m, el = doc.createElement('section'); el.className = 'osSource'; el.dataset.os = 'source'; el.setAttribute('aria-label', 'Sheet source');
    const chips = (k, list, label) => `<div class="osChips" role="group" aria-label="${label}"><span class="lbl">${label}</span>${list.map(([v, w]) => `<button type="button" class="osChip" data-os="${k}" data-v="${v}" aria-pressed="false">${w}</button>`).join('')}</div>`;
    const P = PUI(), pol = (PS() && PS().policy ? PS().policy(m) : null) || { wMm: 100, hMm: 50 };
    el.innerHTML = (P && P.policyHtml ? P.policyHtml(m) : '')
      + `<h4 class="osLabel" id="osSrc-${esc(m)}">Sheet source</h4>`
      + `<div class="osTabs" role="tablist" aria-labelledby="osSrc-${esc(m)}">`
      + `<button type="button" class="osTab" role="tab" id="osTabA-${esc(m)}" aria-controls="osPanA-${esc(m)}" data-os="tab" data-v="a" aria-selected="true"><span class="osTabIcon">${ICON.stack}</span><span class="osTabText"><b>From partial sheets</b><small data-os="tabcount">Reading the repository…</small></span></button>`
      + `<button type="button" class="osTab" role="tab" id="osTabB-${esc(m)}" aria-controls="osPanB-${esc(m)}" data-os="tab" data-v="b" aria-selected="false" tabindex="-1"><span class="osTabIcon">${ICON.plus}</span><span class="osTabText"><b>New sheet</b><small>Make one at any size, as many as you like</small></span></button></div>`
      + `<div class="osPanel" role="tabpanel" id="osPanA-${esc(m)}" aria-labelledby="osTabA-${esc(m)}" data-os="panel" data-v="a">`
      + `<div class="osFilters"><label class="osSearch">${ICON.search}<input type="search" autocomplete="off" spellcheck="false" placeholder="Search by sheet, set, person, date (oct 5), size or status" aria-label="Search partial sheets"></label>`
      + chips('metal', METAL_CHIPS, 'Metal') + chips('status', STATUS_CHIPS, 'Status') + `</div>`
      + `<p class="osCount" data-os="count" role="status"></p><p class="osLock" data-os="lock" hidden></p><div class="osResults" data-os="results"></div>`
      + `<button type="button" class="btn ghost osMore" data-os="more" hidden>Show more</button><button type="button" class="btn ghost osMore" data-os="older" hidden>Load older sheets</button></div>`
      + `<div class="osPanel" role="tabpanel" id="osPanB-${esc(m)}" aria-labelledby="osTabB-${esc(m)}" data-os="panel" data-v="b" hidden>`
      + `<form class="osNew" data-os="newform" novalidate><div class="osNewHead"><h5>New ${esc(CODE[m] || '')} sheet</h5><p class="osLead">Set its width and height. It becomes a card in From partial sheets, ready to use at any time, like a partial sheet. Make as many as you need, each at its own size.</p></div>`
      + `<div class="osNewRow"><div class="solidSize osNewSize"><label>Width <span>mm</span><input type="number" min="${MIN_MM}" max="${MAX_MM}" step="0.1" inputmode="decimal" data-os="mw" value="${+(+pol.wMm || 100).toFixed(2)}" aria-label="Width of the new sheet, millimetres"></label><label>Height <span>mm</span><input type="number" min="${MIN_MM}" max="${MAX_MM}" step="0.1" inputmode="decimal" data-os="mh" value="${+(+pol.hMm || 50).toFixed(2)}" aria-label="Height of the new sheet, millimetres"></label></div>`
      + `<button type="submit" class="btn sage" data-os="make">Make new sheet</button></div>`
      + `<p class="osHelp" data-os="makehelp" role="status" aria-live="polite">${MIN_MM}–${MAX_MM} mm per side</p></form></div>`;
    return el;
  }

  function tabTo(M, v, o = {}) {
    if (M.tab === v && !o.force) return;
    M.tab = v;
    for (const t of M.source.querySelectorAll('[data-os="tab"]')) { const on = t.dataset.v === v; t.setAttribute('aria-selected', String(on)); t.tabIndex = on ? 0 : -1; }
    for (const p of M.source.querySelectorAll('[data-os="panel"]')) p.hidden = p.dataset.v !== v;
    if (o.focus) { const t = M.source.querySelector(`[data-os="tab"][data-v="${v}"]`); if (t) t.focus({ preventScroll: true }); }
    if (!reduced() && !o.quiet) { const p = M.source.querySelector(`[data-os="panel"][data-v="${v}"]`); try { p && p.animate([{ opacity: 0, transform: 'translateY(6px)' }, { opacity: 1, transform: 'none' }], { duration: 220, easing: EASE }); } catch (_) {} }
  }

  /** The buttons of a card: Use this one (PartialSheetsUI: the answer in place), and Delete (only an available sheet that no sheet holds). */
  function actionsOf(M, g) {
    const c = g.latest, st = statusOf(c), P = PUI(); let html = '';
    try { if (P && P.actions) html += P.actions(M.m, c) || ''; } catch (e) { warn('actions', e); }
    if (st === 'available') html += `<button type="button" class="btn ghost xs osDel" data-os="del" data-id="${esc(c.id)}" aria-label="Delete ${esc(nameOf(c))}">Delete</button>`;
    else if (st === 'inUse') html += `<button type="button" class="btn ghost xs osDel" disabled aria-disabled="true" data-os="del" data-id="${esc(c.id)}" title="A sheet holds this one now. Give it back before deleting it.">Delete</button><span class="osWhy">${esc(P && P.statusWords ? P.statusWords(c) : 'In use')}: it cannot be deleted</span>`;
    return html;
  }
  function cardOf(M, g, i) {
    const X = OH(), c = g.latest, actions = actionsOf(M, g);
    if (X && typeof X.card === 'function') {
      try { return X.card(g, { actions, selected: M.flash === c.id, enlarged: ALL.enlarged === g.stockId, now: Date.now() }); } catch (e) { warn('card', e); }
    }
    return PUI() && PUI().card ? PUI().card(c, { actions, selected: M.flash === c.id }, i) : '';
  }

  function paintCount(M, list) {
    const total = groupsOf().length, el = q(M.source, 'count'), mine = groupsOf().filter(g => g.latest.metal === M.m && statusOf(g.latest) === 'available').length;
    const tc = q(M.source, 'tabcount');
    if (tc) tc.textContent = !ALL.items ? (ALL.error ? 'Could not read the repository' : 'Reading the repository…') : mine ? `${mine} available ${CODE[M.m] || ''} · pick one to use` : `No available ${CODE[M.m] || ''} sheets yet`;
    if (!el) return;
    const dflt = defaultView(M);
    el.innerHTML = (ALL.loading ? spin('Reading again…') : '') + `<span>${dflt ? `${plural(list.length, 'sheet')} shown: ${esc(CODE[M.m] || '')}, Available` : `${list.length} of ${plural(total, 'sheet')}`}${ALL.more ? ', the newest first' : ''}</span>`
      + `<span class="osCountBtns">${dflt ? '' : '<button type="button" class="osLinkBtn" data-os="clear">Reset filters</button>'}<button type="button" class="osLinkBtn" data-os="reload" title="Read the repository again"${ALL.loading ? ' disabled' : ''}>Refresh</button></span>`;
  }
  /** Everything the drawing of the repository depends on: when it is the same, nothing is drawn again (the cards, the focus and an open answer stay as they are). */
  const keyOf = M => JSON.stringify([ALL.gen, ALL.q, ALL.metal, ALL.status, ALL.shown, ALL.loading, ALL.error, ALL.enlarged, M.flash, !!ALL.items, PUI() && PUI().sig ? PUI().sig(M.m) : '', !!OH() && typeof OH().card === 'function']);
  /** An answer is pending for the sheet (a pick that waits for Use this one / Cancel): Esc belongs to it first. */
  const pending = m => { try { const s = PUI() && PUI().state && PUI().state[m]; return !!(s && (s.ask || (s.chain && s.chain.length))); } catch (_) { return false; } };
  /** Draw the repository: the cards (large, with the history in their thumbnails), the answer in place after the row of the card picked. */
  function paintAll(M, o = {}) {
    if (!M || M.done || !M.source) return;
    const src = M.source, res = q(src, 'results'), more = q(src, 'more'), older = q(src, 'older'), lock = q(src, 'lock'), P = PUI();
    for (const b of src.querySelectorAll('[data-os="metal"]')) b.setAttribute('aria-pressed', String((b.dataset.v || '') === ALL.metal));
    for (const b of src.querySelectorAll('[data-os="status"]')) b.setAttribute('aria-pressed', String(b.dataset.v === ALL.status));
    const key = keyOf(M);
    if (!o.force && M.paintKey === key) return; M.paintKey = key;
    more.hidden = true; older.hidden = true;
    const why = P && P.lock ? P.lock(M.m) : ''; lock.hidden = !why; lock.textContent = why || '';
    const noCards = () => { try { M.cardsApi && M.cardsApi.destroy(); } catch (_) {} M.cardsApi = null; if (M.host) M.host.innerHTML = ''; };
    if (!ALL.items) {
      paintCount(M, []); noCards();
      res.innerHTML = ALL.error ? `<div class="osEmpty bad"><b>Could not read the repository: ${esc(ALL.error)}</b><button type="button" class="btn ghost xs" data-os="reload">Try again</button></div>` : spin('Reading the partial sheets…', true);
      return;
    }
    const list = visible(), shown = list.slice(0, ALL.shown);
    paintCount(M, list);
    // the focus and the place in the page survive a redraw (a pick, the answer coming in, a chip)
    const a = doc.activeElement, cardAt = a && a.closest ? a.closest('.ohc, .psCard') : null, mark = a && res.contains(a) ? { os: a.dataset.os, ps: a.dataset.ps, oh: a.hasAttribute && (a.hasAttribute('data-oh-thumb') ? 'thumb' : a.hasAttribute('data-oh-close') ? 'close' : ''), id: a.dataset.id, ph: cardAt && cardAt.dataset.id } : null;
    if (!list.length) {
      const dflt = defaultView(M); noCards();
      res.innerHTML = `<div class="osEmpty"><b>${ALL.items.length ? (dflt ? `No available ${esc(CODE[M.m] || '')} sheets yet.` : 'No sheet matches.') : 'No sheets have been made yet.'}</b><span>${ALL.items.length ? (dflt ? 'Use New sheet to make one, or change the metal and status chips to see the others.' : 'Try fewer words, another date or size, or reset the filters.') : 'When a green line is cut on a Rose Gold, 10K or 14K sheet, the metal that is left is saved here. A new sheet made here appears here too.'}</span></div>`;
      return;
    }
    // ONE list element for the window's life: the cards are bound to it again after every draw (OptionsHistory drops the wiring it made before on the same element)
    const X = OH(), rich = !!(X && typeof X.card === 'function'), host = M.host || (M.host = doc.createElement('div')); host.className = 'osCards ' + (rich ? 'ohcGrid' : 'psList');
    host.innerHTML = shown.map((g, i) => cardOf(M, g, i)).join('');
    // the answer to a pick, in place: it sits after the row of the card picked (it spans every column)
    const at = P && P.picked ? P.picked(M.m) : null, askHtml = at && P.ask ? P.ask(M.m) : ''; if (!askHtml) M.askPhase = null;
    if (askHtml) {
      const tpl = doc.createElement('template'); tpl.innerHTML = askHtml; const ask = tpl.content.firstElementChild, items = [...host.children], i = items.findIndex(x => x.dataset.id === at);
      if (ask && i >= 0) {
        let last = i; const top = el => { const r = el.getBoundingClientRect(); return { t: Math.round(r.top), h: r.height }; }, t0 = top(items[i]);
        if (t0.h > 0) while (last + 1 < items.length && top(items[last + 1]).t === t0.t) last++;
        items[last].after(ask);
        const phase = P.state && P.state[M.m] && P.state[M.m].ask && P.state[M.m].ask.phase;
        if (!reduced() && M.askPhase !== phase) { M.askPhase = phase; try { ask.animate([{ opacity: 0, transform: 'translateY(-4px)' }, { opacity: 1, transform: 'none' }], { duration: 200, easing: 'ease-out' }); } catch (_) {} }
      }
    }
    res.replaceChildren(host);
    try { M.cardsApi && M.cardsApi.destroy(); } catch (_) {} M.cardsApi = null;
    if (rich && typeof X.bindCards === 'function') {   // (a fresh list element every time: the cards are bound to it once; the card's own enlarge, grow, Esc and history read are OptionsHistory's)
      const byStock = new Map(shown.map(g => [g.stockId, g]));
      const setEnlarged = v => { ALL.enlarged = v; M.paintKey = keyOf(M); };   // (what the card did by itself is not drawn again over it)
      try {
        M.cardsApi = X.bindCards(host, {
          onEnlarge: g => setEnlarged(g && g.stockId || null), onCollapse: () => setEnlarged(null),
          groupOf: id => byStock.get(id) || null, holdEscape: () => pending(M.m),
          getHistory: g => { const P = PS(), rev = g && g.latest && Number.isFinite(+g.latest.revision) ? +g.latest.revision : null; return P && typeof P.history === 'function' ? P.history(rev != null ? { stockId: g.stockId, revision: rev } : { stockId: g.stockId }) : Promise.reject(new Error('The sheet history is not available on this page yet.')); }
        }) || null;
      } catch (e) { warn('cards', e); }
    }
    if (mark) {
      const sel = mark.ps ? `[data-ps="${mark.ps}"]` : mark.os ? `[data-os="${mark.os}"]` : mark.oh ? (mark.oh === 'thumb' ? '[data-oh-thumb]' : '[data-oh-close]') : '', item = mark.ph && [...host.children].find(x => x.dataset.id === mark.ph);
      const f = (item && sel && item.querySelector(sel)) || (sel && !mark.oh && host.querySelector(sel + (mark.id ? `[data-id="${mark.id}"]` : '')));
      if (f) { try { f.focus({ preventScroll: true }); } catch (_) {} }
    }
    if (M.flash) { const it = [...host.children].find(x => x.dataset.id === M.flash); if (it) { try { it.scrollIntoView({ block: 'nearest', behavior: reduced() ? 'auto' : 'smooth' }); } catch (_) {} } M.flashT = setTimeout(() => { if (M.flash) { M.flash = null; paintAll(M); } }, 2600); }
    more.hidden = list.length <= shown.length; more.textContent = `Show ${Math.min(PAGE, list.length - shown.length)} more`;
    older.hidden = !(ALL.more && !ALL.loading && list.length <= shown.length);   // (every one read is on show: the server has older ones: one more call, on a press)
    if (!reduced() && !M.cardsIn && shown.length) { M.cardsIn = true; [...host.children].slice(0, 8).forEach((n, i) => { try { n.animate([{ opacity: 0, transform: 'translateY(10px)' }, { opacity: 1, transform: 'none' }], { duration: 320, delay: i * 55, easing: EASE, fill: 'backwards' }); } catch (_) {} }); }
  }
  async function loadAll(M, o = {}) {
    const P = PS();
    if (!P || typeof P.searchAll !== 'function') { ALL.error = 'The list of partial sheets is not available on this page yet.'; paintAll(M, { force: true }); return; }
    if (ALL.loading) return;
    ALL.loading = true; ALL.error = ''; paintAll(M, { force: true });
    try { const r = await P.searchAll({ force: !!o.force, more: !!o.more }); ALL.items = Array.isArray(r) ? r : (r && r.items) || []; ALL.more = !!(r && r.more); ALL.groups = null; ALL.gen++; }
    catch (e) { ALL.error = (e && e.message) || String(e); }
    finally {
      ALL.loading = false;
      const W = CUR && !CUR.done ? CUR : null;   // (the window that is open now: it may not be the one that asked)
      if (W) { try { PUI() && PUI().items && PUI().items(W.m, ALL.items); } catch (_) {} paintAll(W, { force: true }); }
    }
  }

  /* ── New sheet (B): a physical sheet at the size the person sets, a card in the repository ── */
  async function makeSheet(M) {
    const form = q(M.source, 'newform'), wI = q(M.source, 'mw'), hI = q(M.source, 'mh'), help = q(M.source, 'makehelp'), btn = q(M.source, 'make'), P = PS();
    if (M.making) return;
    const w = +wI.value, h = +hI.value, ok = n => Number.isFinite(n) && n >= MIN_MM && n <= MAX_MM;
    const say = (html, bad) => { help.innerHTML = html; help.classList.toggle('bad', !!bad); };
    if (!ok(w) || !ok(h)) { say(`Use a width and a height between ${MIN_MM} and ${MAX_MM} mm.`, true); toast(`Use a width and a height between ${MIN_MM} and ${MAX_MM} mm`, 'bad'); (!ok(w) ? wI : hI).focus(); return; }
    if (!P || typeof P.make !== 'function') { say('Making a new sheet is not available on this page yet. Reload the page to get it.', true); return; }
    M.making = true; btn.disabled = true; wI.disabled = hI.disabled = true; say(spin('Making the new sheet…'));
    try {
      const r = await P.make({ metal: M.m, wMm: w, hMm: h });
      if (M.done) return;
      const item = r && r.item ? r.item : r && r.id ? r : null, made = item ? sizeWords(item) : `${num(w)} × ${num(h)} mm`;
      if (item) putItem(item); else await loadAll(M, { force: false });
      try { PUI() && PUI().items && PUI().items(M.m, ALL.items); } catch (_) {}
      M.madeId = item ? item.id : null; M.madeCount = (M.madeCount || 0) + 1;
      say(`<span class="osOkDot" aria-hidden="true"></span>New sheet ${esc(made)} is in the repository${M.madeCount > 1 ? ` (${M.madeCount} made now)` : ''}. <button type="button" class="osLinkBtn" data-os="showmade">Show it in From partial sheets</button>`);
      toast(`New sheet ${made} made`, 'ok'); paintAll(M, { force: true });
    } catch (e) { say(`Could not make the sheet: ${esc((e && e.message) || e)}`, true); toast('New sheet not made: ' + ((e && e.message) || e), 'bad'); }
    finally { M.making = false; if (!M.done) { btn.disabled = false; wI.disabled = hI.disabled = false; } }
  }
  function showMade(M) {
    ALL.q = ''; ALL.metal = M.m; ALL.status = 'available'; ALL.shown = PAGE; ALL.enlarged = null; M.flash = M.madeId || null; M.paintKey = null;
    const i = q(M.source, 'search'); if (i) i.value = '';
    tabTo(M, 'a', { focus: true }); paintAll(M, { force: true });
  }

  /* ── Delete: a pop-up above this window that asks WHY (a required answer), who deletes, Cancel / Delete ── */
  function askDelete(M, c, opener) {
    if (M.del) return;
    const P = PS(), who = whoName(), name = nameOf(c), D = { dlg: doc.createElement('dialog'), busy: false, done: false };
    M.del = D; const dlg = D.dlg; dlg.className = 'osAskDlg'; dlg.tabIndex = -1;
    const label = `${CODE[c.metal] || ''} ${c.kind === 'new' ? 'new sheet' : 'partial sheet'}, ${sizeWords(c)}`.trim();
    dlg.setAttribute('aria-labelledby', 'osAskT'); dlg.setAttribute('aria-describedby', 'osAskS');
    dlg.innerHTML = `<form class="osAskBox" novalidate><header class="osAskHead"><div class="osAskTitles"><h2 class="osAskTitle" id="osAskT">Delete this sheet?</h2><p class="osAskSub" id="osAskS"><b>${esc(name)}</b> · ${esc(label)}. It stays in the history with a DELETED stamp: nothing is removed.</p></div><button type="button" class="osX" data-ask="x" aria-label="Cancel">${ICON.close}</button></header>`
      + `<div class="osAskBody"><label class="osAskLabel" for="osAskWhy">Why are you deleting it?<span>Required: ${WHY_MIN} to ${WHY_MAX} characters</span></label>`
      + `<textarea id="osAskWhy" data-ask="why" rows="3" maxlength="${WHY_MAX}" placeholder="For example: made by accident, wrong size" autocomplete="off" spellcheck="true"></textarea>`
      + `<div class="osAskMeta"><span class="osAskErr" data-ask="err" role="alert"></span><span class="osAskCount" data-ask="count">0 / ${WHY_MAX}</span></div>`
      + `<p class="osAskWho">${who ? `Deleted by <b>${esc(who)}</b>, the signed-in person, with the date and time.` : 'Nobody is signed in on this page, so no name is recorded. The date and time are.'}</p>`
      + `<div class="osAskBtns"><span class="osAskBusy" data-ask="busy" role="status" hidden></span><button type="button" class="btn ghost" data-ask="cancel">Cancel</button><button type="submit" class="btn danger" data-ask="go">Delete</button></div></div></form>`;
    const why = dlg.querySelector('[data-ask="why"]'), err = dlg.querySelector('[data-ask="err"]'), cnt = dlg.querySelector('[data-ask="count"]'), go = dlg.querySelector('[data-ask="go"]'), cancel = dlg.querySelector('[data-ask="cancel"]'), xb = dlg.querySelector('[data-ask="x"]'), busy = dlg.querySelector('[data-ask="busy"]');
    const shut = () => { if (D.busy) return; closeDialog(dlg); finish(); };
    const finish = () => {
      if (D.done) return; D.done = true; if (M.del === D) M.del = null;
      try { dlg.remove(); } catch (_) {}
      const back = opener && opener.isConnected && !opener.disabled ? opener : M.source && M.source.isConnected ? M.source.querySelector(`[data-id="${(window.CSS && CSS.escape) ? CSS.escape(c.id) : c.id}"] [data-os="del"]`) : null;
      if (!M.done) { try { (back || M.dlg).focus({ preventScroll: true }); } catch (_) {} }
    };
    why.addEventListener('input', () => { cnt.textContent = `${why.value.length} / ${WHY_MAX}`; if (err.textContent) err.textContent = ''; why.removeAttribute('aria-invalid'); });
    xb.addEventListener('click', shut); cancel.addEventListener('click', shut);
    dlg.addEventListener('cancel', e => { e.preventDefault(); e.stopPropagation(); shut(); });   // (Esc: the pop-up alone; the window under it stays)
    dlg.addEventListener('close', () => { if (!D.done) { if (D.busy) return; finish(); } });
    dlg.addEventListener('keydown', e => {
      e.stopPropagation();   // (nothing in the window under it hears these keys)
      if (e.key === 'Escape') { e.preventDefault(); shut(); return; }
      if (e.key === 'Tab') {
        const f = [...dlg.querySelectorAll('button:not([disabled]),textarea:not([disabled])')].filter(el => el.getClientRects().length || true); if (!f.length) return;
        const a = doc.activeElement, first = f[0], last = f[f.length - 1], outside = !dlg.contains(a) || a === dlg;
        if (e.shiftKey && (a === first || outside)) { e.preventDefault(); last.focus(); } else if (!e.shiftKey && (a === last || outside)) { e.preventDefault(); first.focus(); }
      }
    });
    dlg.addEventListener('pointerdown', e => { D.pressedInside = e.target !== dlg; }, true);
    dlg.addEventListener('click', e => { if (e.target === dlg && !D.pressedInside) shut(); });
    dlg.querySelector('form').addEventListener('submit', async e => {
      e.preventDefault(); if (D.busy) return;
      const reason = why.value.trim();
      if (reason.length < WHY_MIN || reason.length > WHY_MAX) { err.textContent = reason.length ? `Say why in ${WHY_MIN} to ${WHY_MAX} characters.` : 'Say why you are deleting this sheet: an answer is required.'; why.setAttribute('aria-invalid', 'true'); why.focus(); return; }
      if (!P || typeof P.remove !== 'function') { err.textContent = 'Deleting is not available on this page yet. Reload the page to get it.'; return; }
      D.busy = true; go.disabled = true; cancel.disabled = true; xb.disabled = true; why.disabled = true; busy.hidden = false; busy.innerHTML = spin('Deleting the sheet…'); err.textContent = '';
      try {
        const r = await P.remove(c.id, reason);
        const item = r && r.item ? r.item : r && r.id ? r : null;
        if (item) putItem({ ...item, status: item.status || 'deleted' }); else putItem({ ...c, status: 'deleted', deletedAt: Date.now(), deletedBy: who, deletedReason: reason });
        D.busy = false; try { PUI() && PUI().items && PUI().items(M.m, ALL.items); } catch (_) {}
        toast(`${name} deleted. It stays in the history as DELETED.`, 'ok');
        closeDialog(dlg); finish(); if (!M.done) { M.paintKey = null; ALL.status = 'deleted'; ALL.metal = c.metal || ALL.metal; M.flash = c.id; paintAll(M, { force: true }); }
      } catch (er) {
        D.busy = false; go.disabled = false; cancel.disabled = false; xb.disabled = false; why.disabled = false; busy.hidden = true; busy.innerHTML = '';
        err.textContent = `Could not delete: ${(er && er.message) || er}`; why.focus();
      }
    });
    doc.body.appendChild(dlg);
    try { PUI() && PUI().picked && PUI().picked(M.m) === c.id && PUI().escape(M.m); } catch (_) {}   // (an answer about this very sheet is put away first)
    try { openDialog(dlg); } catch (e) { dlg.setAttribute('open', ''); }
    try { why.focus({ preventScroll: true }); } catch (_) {}
  }

  function wire(M) {
    const dlg = M.dlg, src = M.source;
    dlg.addEventListener('pointerdown', e => { M.pressedInside = e.target !== dlg; }, true);
    dlg.addEventListener('click', e => {
      if (e.target === dlg) { if (!M.pressedInside && !M.del) close(M.m); return; }
      const t = e.target.closest && e.target.closest('[data-os]'); if (!t || !dlg.contains(t)) return;
      const k = t.dataset.os;
      if (k === 'close') return void close(M.m);
      if (k === 'tab') return void tabTo(M, t.dataset.v);
      if (k === 'reload') return void loadAll(M, { force: true });
      if (k === 'more') { ALL.shown += PAGE; return void paintAll(M); }
      if (k === 'older') return void loadAll(M, { more: true });
      if (k === 'showmade') return void showMade(M);
      if (k === 'clear') { ALL.q = ''; ALL.metal = M.m; ALL.status = 'available'; ALL.shown = PAGE; const i = q(src, 'search'); if (i) i.value = ''; return void paintAll(M); }
      if (k === 'metal') { ALL.metal = t.dataset.v || ''; ALL.shown = PAGE; return void paintAll(M); }
      if (k === 'status') { ALL.status = ALL.status === t.dataset.v ? '' : t.dataset.v; ALL.shown = PAGE; return void paintAll(M); }
      if (k === 'del') { if (t.disabled) return; const c = (ALL.items || []).find(x => x.id === t.dataset.id); if (c) askDelete(M, c, t); return; }
    });
    src.addEventListener('submit', e => { if (e.target.closest('[data-os="newform"]')) { e.preventDefault(); makeSheet(M); } });
    let typing = 0;
    src.addEventListener('input', e => {
      const i = e.target; if (!i || !i.matches) return;
      if (i.matches('input[type=search]')) { clearTimeout(typing); typing = setTimeout(() => { ALL.q = i.value; ALL.shown = PAGE; paintAll(M); }, 110); }   // (the browser filters the one list: nothing is read per keystroke)
      if (i.matches('[data-os="mw"],[data-os="mh"]')) { const h = q(src, 'makehelp'); if (h && h.classList.contains('bad')) { h.classList.remove('bad'); h.textContent = `${MIN_MM}–${MAX_MM} mm per side`; } }
    });
    src.querySelector('input[type=search]').setAttribute('data-os', 'search');
    dlg.addEventListener('keydown', e => {
      if (e.key === 'Tab') {   // the focus stays in the window: Tab from the last control goes to the first, Shift+Tab from the first to the last
        const f = [...dlg.querySelectorAll('button:not([disabled]),input:not([disabled]),select:not([disabled]),textarea:not([disabled]),[tabindex]:not([tabindex="-1"])')].filter(el => el.getClientRects().length && !el.closest('[hidden]'));
        if (f.length) {
          const a = doc.activeElement, first = f[0], last = f[f.length - 1], outside = !dlg.contains(a) || a === dlg;
          if (e.shiftKey && (a === first || outside)) { e.preventDefault(); last.focus(); }
          else if (!e.shiftKey && (a === last || outside)) { e.preventDefault(); first.focus(); }
        }
        return;
      }
      if ((e.key === 'ArrowRight' || e.key === 'ArrowLeft' || e.key === 'Home' || e.key === 'End') && e.target.matches && e.target.matches('[data-os="tab"]')) {
        e.preventDefault(); tabTo(M, e.key === 'ArrowRight' || e.key === 'End' ? 'b' : 'a', { focus: true }); return;
      }
      // Esc, in order: an answer that waits (PartialSheetsUI cancels it inside the window), a card enlarged in place (it folds back), then the window
      // (the delete pop-up is its own dialog on top: it takes its own Esc and the window never hears it)
      if (e.key === 'Escape' && !e.defaultPrevented) {
        e.preventDefault(); e.stopPropagation();
        const t = e.target;   // (Esc in the search box with words in it clears them first, as a search box does)
        if (t && t.matches && t.matches('input[type=search]') && t.value) { t.value = ''; ALL.q = ''; ALL.shown = PAGE; paintAll(M); return; }
        escStep(M);
      }
    });
    dlg.addEventListener('cancel', e => { e.preventDefault(); if (M.closing || M.done || M.del) return; escStep(M); });
    dlg.addEventListener('close', () => { if (!M.done) finish(M); });   // (closed by the browser itself)
  }
  /** One step of Esc: an answer that waits, then a card enlarged in place, then the window. */
  function escStep(M) {
    try { if (PUI() && PUI().escape && PUI().escape(M.m)) return; } catch (_) {}
    try { if (M.cardsApi && M.cardsApi.collapse()) return; } catch (e) { warn('collapse', e); }
    close(M.m);
  }

  /* ── open / close ── */
  function stagger(M) {
    if (reduced()) return;
    [...M.box.children].forEach((c, i) => { try { c.animate([{ opacity: 0, transform: 'translateY(12px)' }, { opacity: 1, transform: 'none' }], { duration: 320, delay: 70 + i * 70, easing: EASE, fill: 'backwards' }); } catch (_) {} });
  }
  function open(o) {
    try {
      o = o || {}; const sh = o.sh, box = o.box; if (!sh || !box || !doc) return null;
      if (CUR && !CUR.done) { if (CUR.box === box) { try { CUR.dlg.focus({ preventScroll: true }); } catch (_) {} return handle(CUR); } finish(CUR); }
      const M = { m: sh.metal, sh, node: o.node || null, box, home: box.parentNode, opener: o.opener || doc.activeElement, title: o.title || titleOf(sh), dlg: doc.createElement('dialog'), done: false, closing: null, offs: [], tab: 'a', paintKey: null, flash: null, del: null, cardsApi: null };
      build(M);
      M.source = sourceEl(M);
      doc.body.appendChild(M.dlg);
      M.body.appendChild(box); box.hidden = false;
      M.inc = box.querySelector('.osInclude'); const slot = M.dlg.querySelector('.osIncSlot');   // (the In current set switch rides in the title row, top right: the same element, so its handler and hooks are the Gate's own)
      if (M.inc && slot) slot.appendChild(M.inc); else if (slot) slot.remove();
      const sheetCard = box.querySelector('[data-card="sheet"]') || box; sheetCard.appendChild(M.source);
      ALL.q = ''; ALL.metal = M.m; ALL.status = 'available'; ALL.shown = PAGE; ALL.enlarged = null;   // (the view it opens on: this sheet's metal, the available ones)
      wire(M); watchToasts(M);
      CUR = M;
      try { PUI() && PUI().open(M.m); PUI() && PUI().bind(M.m, { el: M.source, repaint: () => paintAll(M) }); } catch (e) { warn('partial sheets', e); }
      try { openDialog(M.dlg); } catch (e) { M.dlg.setAttribute('open', ''); }
      try { M.dlg.focus({ preventScroll: true }); } catch (_) {}   // (it rests on the window itself: arrows or Tab go on from here)
      stagger(M);
      if (ALL.items) { try { PUI() && PUI().items && PUI().items(M.m, ALL.items); } catch (_) {} }
      paintAll(M, { force: true });
      loadAll(M);   // (ONE list for this opening: a revision probe when it was read lately)
      try { const P = PS(); if (P && typeof P.loadPolicy === 'function') Promise.resolve(P.loadPolicy()).then(() => { if (!M.done) PUI() && PUI().paintPolicy && PUI().paintPolicy(M.m); }, () => {}); } catch (_) {}   // (the rule: read once per page)
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
    clearTimeout(M.flashT); try { M.cardsApi && M.cardsApi.destroy(); } catch (_) {} M.cardsApi = null;
    if (M.del) { const D = M.del; M.del = null; D.done = true; try { closeDialog(D.dlg); D.dlg.remove(); } catch (_) {} }
    try { PUI() && PUI().unbind(M.m); PUI() && PUI().close(M.m); } catch (_) {}
    // the controls go back where the Gate drew them, hidden, exactly as they were (the Gate keeps repainting them there)
    try { M.source.remove(); } catch (_) {}
    try { if (M.inc) M.box.prepend(M.inc); } catch (_) {}   // (the switch goes back into the box first: the Gate keeps repainting it there)
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
  /** Every draw of the controls: the title follows the sheet; the cards say whether this sheet can take a partial sheet now (no read). */
  function sync(sh, node, title) {
    const M = CUR; if (!M || M.done || M.node !== node) return;
    M.sh = sh;
    if (title && title !== M.title) { M.title = title; if (M.titleEl) M.titleEl.textContent = title; }
    paintAll(M);
  }

  root.OptionsStudio = { open, close, isOpen: m => !!CUR && !CUR.done && (!m || CUR.m === m), sync, release, state: () => ({ cur: CUR, all: ALL }) };
})(window);
