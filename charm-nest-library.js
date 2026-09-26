/* The Library's two tabs, Current | Completed (Paul, 25 Sep).
   The laser operator marks a sheet, or a whole set, completed with the check at its corner. It leaves Current (the
   Laser cutting and In progress lists) and is filed under Completed: every sheet or set the laser has done, newest first,
   by the day it was done, read a page at a time as the list is scrolled. A set there opens in place into the same card
   Current shows, and a sheet opens as it does in Current. A number in the search box is looked up as an order and as a
   listing, however old the sheet: the sheets that hold it are lit and brought into view, in either tab.
   The page's own Library (charm-nest-1.html: loadLibrary, renderLibrary; the bridge's Sets view) draws Current and asks
   this file which records are completed (isDone), what a search found (focus, rows) and to finish its cards (decorate).
   window.LibraryDone.mark(kind, id, done) marks a sheet or a set (kind "sheet" | "set"), or takes the mark back. */
(function () {
  'use strict';
  const PAGE = 60, CHUNK = 60, MAX_ROWS = 2000, FRESH = 60000, KEEP_LIST = 10 * 60000, AWAY = 5 * 60000, MAX_LISTS = 4;
  const TAB_KEY = 'cn.libTab';
  const doc = document, byId = id => doc.getElementById(id);
  const stage = () => byId('stage');
  const reduced = () => !!(window.matchMedia && matchMedia('(prefers-reduced-motion: reduce)').matches);
  const realDay = t => CharmNestOrders.localDay(new Date(t));
  const plural = (n, one, many) => `${n} ${n === 1 ? one : many || one + 's'}`;
  const pct = v => Math.round((+v || 0) * 100) + '%';
  const ICON = {
    check: '<svg viewBox="0 0 16 16" aria-hidden="true"><path d="M3.4 8.5l3 3 6.2-6.4" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>',
    undo: '<svg viewBox="0 0 16 16" aria-hidden="true"><path d="M5.6 3.6L2.4 6.8l3.2 3.2" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/><path d="M2.8 6.8h6.4a3.9 3.9 0 0 1 0 7.8H6.6" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"/></svg>',
    done: '<svg viewBox="0 0 16 16" aria-hidden="true"><circle cx="8" cy="8" r="7" fill="currentColor" opacity=".16"/><path d="M4.8 8.3l2.2 2.2 4.2-4.4" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/></svg>',
    chev: '<svg class="ldChev" viewBox="0 0 16 16" aria-hidden="true"><path d="M4 6l4 4 4-4" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/></svg>'
  };

  const L = {
    tab: 'current',
    counts: null, countsAt: 0, countsBusy: false,
    marks: new Map(),       // "sheet:<id>" | "set:<id>" → { done, at, by, t }: what this page marked, ahead of its lists
    extra: new Map(),       // sheet id → { r, t }: a sheet moved back to Current that Current's list has not loaded
    finds: new Map(),       // number → { q, res, error, at, promise }: the order / listing searches, kept a few minutes
    lists: new Map(),       // list key → a Completed list (its rows, its cursor and its drawn rows, kept while switching)
    list: null,             // the Completed list on screen
    scroll: { current: 0, done: 0 },
    currentQ: null, located: '', flash: null, entered: '',
    timer: 0, leftAt: 0, busy: new Set(), io: null
  };

  /* ── which tab ── */
  function readTab() {
    const h = (location.hash || '').slice(1).split('/');
    if (h[0] === 'library' && h[1] === 'completed') return 'done';
    if (h[0] === 'library' && h[1] === 'current') return 'current';
    try { return localStorage.getItem(TAB_KEY) === 'done' ? 'done' : 'current'; } catch (_) { return 'current'; }
  }
  function paintTabs() {
    const bar = byId('libTab'); if (!bar) return;
    for (const b of bar.querySelectorAll('button[data-t]')) { const on = b.dataset.t === L.tab; b.classList.toggle('on', on); b.setAttribute('aria-selected', on ? 'true' : 'false'); b.tabIndex = on ? 0 : -1; }
    fitPlaceholder();
    const view = byId('libView'); if (view) view.dataset.tab = L.tab;
  }
  /** The search box says which tab it searches, and as much of what it finds as its width holds (never cut off). */
  let measure = null;
  function fitPlaceholder() {
    const input = byId('libSearch'); if (!input) return;
    const w = L.tab === 'done' ? 'completed' : 'current';
    const says = [`Search ${w} · order, listing, SKU`, `Search ${w} · order, listing`, `Search ${w} · order or listing`, `Search ${w}`, 'Search'];
    const cs = getComputedStyle(input), room = input.clientWidth - (parseFloat(cs.paddingLeft) || 0) - (parseFloat(cs.paddingRight) || 0) - 4;
    try { measure = measure || doc.createElement('canvas').getContext('2d'); measure.font = `${cs.fontStyle} ${cs.fontWeight} ${cs.fontSize} ${cs.fontFamily}`; } catch (_) { measure = null; }
    input.placeholder = measure && room > 0 ? says.find(t => measure.measureText(t).width <= room) || says[says.length - 1] : says[1];
  }
  function writeHash() {
    if (S.mode !== 'library') return;
    const want = '#library' + (L.tab === 'done' ? '/completed' : '');
    if (location.hash !== want) try { history.replaceState(null, '', want); } catch (_) {}
  }
  function panels() {
    const cur = byId('libBody'), done = byId('libDone');
    if (cur) cur.hidden = L.tab === 'done';
    if (done) done.hidden = L.tab !== 'done';
  }
  function setTab(tab, o = {}) {
    tab = tab === 'done' ? 'done' : 'current';
    if (tab === L.tab) { if (!o.fromHash) writeHash(); return; }
    if (S.mode === 'library') L.scroll[L.tab] = stage().scrollTop;
    L.tab = tab;
    try { localStorage.setItem(TAB_KEY, tab); } catch (_) {}
    paintTabs(); if (!o.fromHash) writeHash();
    if (S.mode !== 'library') { panels(); return; }
    const want = L.scroll[tab] || 0;
    panels();
    const el = byId(tab === 'done' ? 'libDone' : 'libBody');
    if (el && !reduced()) { el.classList.remove('ldSwap'); void el.offsetWidth; el.classList.add('ldSwap'); }
    if (tab === 'done') showDone();
    else if (L.currentQ !== query()) renderCurrent();   // the search changed while Completed was open
    else showLibrary();
    requestAnimationFrame(() => { if (L.tab === tab) stage().scrollTop = want; });
  }
  function fromHash() {
    const h = (location.hash || '').slice(1).split('/');
    if (h[0] === 'library') setTab(h[1] === 'completed' ? 'done' : 'current', { fromHash: true });
  }
  /** The Library is shown (a click on its tab, a metal chip, Sheets | Sets, the cloud back): the tab on screen. */
  function show() {
    panels(); paintTabs(); requestAnimationFrame(fitPlaceholder);
    if (L.tab !== 'done') return showLibrary();
    const want = L.scroll.done;
    showDone();
    if (want) requestAnimationFrame(() => { if (L.tab === 'done' && S.mode === 'library') stage().scrollTop = want; });
  }

  /* ── completed or not ── */
  const keyOf = r => {
    if (!r) return '';
    if (r.kind === 'set' || (r.setId && Array.isArray(r.sheetIds))) return 'set:' + r.setId;     // a set (its record, its row)
    const id = r.id || r.sheetId; return id ? 'sheet:' + id : r.setId ? 'set:' + r.setId : '';
  };
  /** A sheet (a Library record, a run's page, a Completed row) or a set: marked on this page, or by its record. */
  function isDone(r) {
    if (!r) return false;
    const m = L.marks.get(keyOf(r));
    return m ? m.done : +(r.laserDoneAt || (r.kind ? r.at : 0)) > 0;       // (a Completed row says when as `at`)
  }
  function note(key, done, at, by) { L.marks.delete(key); L.marks.set(key, { done, at: at || null, by: by || null, t: Date.now() }); }
  /** A run's pages that are these sheets: a sheet the laser has done is closed to more charms (layoutFixed, LiveNest). */
  function pages(ids, at) {
    if (typeof allSheets !== 'function' || !ids || !ids.length) return;
    const want = new Set(ids);
    for (const p of allSheets()) if (p.sheetId && want.has(p.sheetId)) {
      if (at) p.laserDoneAt = at; else delete p.laserDoneAt;
      try { if (activePage(p.metal) === p) renderCard(p); } catch (_) { /* its card is drawn at the next change */ }
    }
  }
  /** The Library's own records of these sheets say so too, until they are read again. */
  function records(ids, at) {
    const want = new Set(ids);
    for (const r of S.library.rows || []) if (want.has(r.id)) r.laserDoneAt = at || null;
    for (const x of L.extra.values()) if (want.has(x.r.id)) x.r.laserDoneAt = at || null;
    S.library.loadedAt = 0;     // Current is read again when it is next shown
  }
  /** The sheets of a set, as this page knows them (its card, a Completed row). */
  function setSheets(setId) {
    for (const c of doc.querySelectorAll('.setCard')) if (c._laserSet && c._laserSet.setId === setId && c._laserSheets) return c._laserSheets.slice();
    for (const st of L.lists.values()) { const r = st.byKey.get('set:' + setId); if (r) return (r.sheets || []).map(s => s.id); }
    return [];
  }
  function recordOf(id) {
    const hit = (S.library.rows || []).find(r => r.id === id) || (L.extra.get(id) || {}).r;
    if (hit) return hit;
    for (const c of doc.querySelectorAll('.setCard')) { const r = (c._sheets || []).find(x => x.id === id); if (r) return r; }
    for (const st of L.lists.values()) { const r = st.byKey.get('sheet:' + id); if (r) return r; }
    for (const f of L.finds.values()) { const r = f.res && [...(f.res.sheets || []), ...(f.res.rows || [])].find(x => x.id === id); if (r) return r; }
    return null;
  }
  function nameOf(kind, id) {
    if (kind === 'set') {
      for (const c of doc.querySelectorAll('.setCard')) if (c._laserSet && c._laserSet.setId === id && c._laserSet.seq) return 'Set ' + c._laserSet.seq;
      for (const st of L.lists.values()) { const r = st.byKey.get('set:' + id); if (r && r.seq) return 'Set ' + r.seq; }
      return 'The set';
    }
    const r = recordOf(id); if (!r) return 'The sheet';
    return `${CODE[r.metal] ? CODE[r.metal] + ' ' : ''}Sheet ${r.sheetIndex || (typeof sheetNo === 'function' ? sheetNo(r) : 1)}`;
  }

  /* ── marking ── */
  /** Marks a sheet or a set completed (done), or takes the mark back, with who and when. The card leaves at once and the
      cloud is told; if it says no, the card comes back and the reason is shown. Resolves to the cloud's answer. */
  async function mark(kind, id, done = true, o = {}) {
    kind = kind === 'set' ? 'set' : 'sheet'; done = done !== false; id = String(id || '');
    if (!id) throw new Error('No sheet or set to mark');
    const key = kind + ':' + id;
    if (L.busy.has(key)) return null;               // a second click while the first is on its way
    const staff = window.CNEmployee || { name: () => '', ask: () => '' };
    let by = o.by || staff.name();
    if (done && !by) by = staff.ask();
    if (done && !by) { toast('Nothing was marked: say who you are first', 'bad', 5000); throw new Error('No name given'); }
    if (!S.cloud.ok) { toast('Nothing was marked: the cloud is offline', 'bad', 5000); throw new Error('The cloud is offline'); }
    L.busy.add(key);
    const ids = kind === 'sheet' ? [id] : setSheets(id), was = new Map([key, ...ids.map(x => 'sheet:' + x)].map(k => [k, L.marks.get(k)]));
    const at = Date.now(), name = nameOf(kind, id), setOfSheet = kind === 'sheet' ? ((recordOf(id) || {}).setId || null) : null;
    note(key, done, at, by); for (const x of ids) note('sheet:' + x, done, at, by);
    pages(ids, done ? at : null); records(ids, done ? at : null);
    bump(kind === 'set' ? 'sets' : 'sheets', done ? 1 : -1);
    leave(kind, id, done, ids, setOfSheet);
    try {
      const r = await api('charmNestLibrary', { op: 'laserDone', kind, id, done, by: by || undefined }, { label: done ? 'Marking completed' : 'Moving back to Current' });
      const t = r.at || at;
      for (const x of r.sheetIds || []) note('sheet:' + x, done, t, r.by);
      if (r.setId && r.setDone != null) note('set:' + r.setId, !!r.setDone, t, r.by);
      pages(r.sheetIds || [], done ? t : null); records(r.sheetIds || [], done ? t : null);
      if (r.counts) setCounts(r.counts);
      // the Completed lists not on screen are read again when next shown; the one on screen was changed in place (leave)
      for (const [k, st] of L.lists) if (st !== L.list || L.tab !== 'done') dropList(k);
      if (!done && r.setId && r.setChanged && L.list && L.list.kind === 'sets') removeRows(L.list, ['set:' + r.setId]);
      if (!done) keepCurrent(r.sheetIds || ids);
      if (L.tab === 'done' && done && L.list) freshen(L.list, true);
      // a set's sheets this page did not know of until the cloud said: their cards leave as well
      if (done && L.tab !== 'done' && S.mode === 'library') for (const x of r.sheetIds || []) if (!ids.includes(x)) leave('sheet', x, true, [x], null);
      if (!o.undo) undoToast(kind, id, done, r, name, ids);
      else toast(done ? `${name} is completed again` : `${name} is back in Current`, 'ok', 3200, 'ld-undo');
      return r;
    } catch (e) {
      for (const [k, m] of was) { if (m) L.marks.set(k, m); else L.marks.delete(k); }
      const back = ids.filter(x => !(was.get('sheet:' + x) || {}).done);
      pages(done ? back : ids, done ? null : at); records(done ? back : ids, done ? null : at);
      bump(kind === 'set' ? 'sets' : 'sheets', done ? -1 : 1, true);
      if (L.tab === 'done') { dropList(L.list && L.list.key); showDone(true); } else renderCurrent();
      toast(`${done ? 'Not marked completed' : 'Not moved back'}: ${e.message}`, 'bad', 8000);
      throw e;
    } finally { L.busy.delete(key); }
  }
  /** "Sheet 2 marked completed · Undo". Undoing a set marks back only the sheets this mark changed. */
  function undoToast(kind, id, done, r, name, ids) {
    const setNote = kind === 'sheet' && r.setId && r.setChanged ? (r.setDone ? ' · its set is complete' : ' · its set is back in Current') : '';
    const text = done ? `${name} marked completed${setNote}` : `${name} moved back to Current${setNote}`;
    const undo = () => {
      if (kind === 'set' && done) {
        // (the set's sheets as they were when it was marked: its card has left since)
        const all = ids && ids.length ? ids : setSheets(id), touched = r.sheetIds || [];
        if (!touched.length || (all.length && touched.length >= all.length)) return mark('set', id, false, { undo: true }).catch(() => {});
        return touched.reduce((p, x) => p.then(() => mark('sheet', x, false, { undo: true })), Promise.resolve()).catch(() => {});
      }
      return mark(kind, id, !done, { undo: true }).catch(() => {});
    };
    const el = toast(text, 'ok', 8000, 'ld-undo'); if (!el) return;
    let b = el.querySelector('.toastUndo');
    if (!b) { b = doc.createElement('button'); b.type = 'button'; b.className = 'toastUndo'; b.textContent = 'Undo'; el.querySelector('.c').before(b); }
    b.onclick = e => { e.stopPropagation(); if (el._go) el._go(); undo(); };
  }
  /** A sheet taken back that Current's list may not hold (it reads the newest 300): its record is kept for Current. */
  async function keepCurrent(ids) {
    if (!ids || !ids.length) return;
    try {
      const r = await api('charmNestLibrary', { op: 'laserStatus', sheetIds: ids.slice(0, 200) }, { quiet: true });
      for (const s of r.sheets || []) { LaserReview.record(s); if (!isDone(s)) L.extra.set(s.id, { r: s, t: Date.now() }); }
      const body = byId('libBody'), shown = body && (r.sheets || []).every(s => isDone(s) || body.querySelector(`.libCard[data-id="${CSS.escape(s.id)}"]`));
      if (L.tab !== 'done' && S.mode === 'library' && !shown) renderCurrent();
    } catch (_) { /* Current reads it at its next refresh if it is among the newest */ }
  }

  /* ── cards leaving and coming back ── */
  function collapse(el, then) {
    if (!el || !el.isConnected) { if (then) then(); return; }
    if (el.dataset.leaving) return;                     // (already on its way out)
    el.dataset.leaving = '1';
    if (reduced() || !el.animate) { el.remove(); if (then) then(); return; }
    const parent = el.parentElement, pcs = parent ? getComputedStyle(parent) : null, box = el.getBoundingClientRect();
    const row = !!pcs && /flex/.test(pcs.display) && pcs.flexDirection.startsWith('row') && box.width < parent.clientWidth - 8;
    const gap = pcs ? parseFloat(row ? pcs.columnGap : pcs.rowGap) || 0 : 0;
    const from = row ? { width: box.width + 'px', flexBasis: box.width + 'px', minWidth: '0px', marginRight: '0px' } : { height: box.height + 'px', marginBottom: '0px' };
    const to = row ? { width: '0px', flexBasis: '0px', minWidth: '0px', marginRight: -gap + 'px' } : { height: '0px', marginBottom: -gap + 'px' };
    el.style.overflow = 'hidden'; el.style.pointerEvents = 'none'; el.style.boxSizing = 'border-box';
    const a = el.animate([Object.assign({ opacity: 1, transform: 'none' }, from), Object.assign({ opacity: 0, transform: 'scale(.96)', offset: .4 }, from), Object.assign({ opacity: 0, transform: 'scale(.96)' }, to)], { duration: 230, easing: 'ease-in-out', fill: 'forwards' });
    let over = false; const end = () => { if (over) return; over = true; el.remove(); if (then) then(); };
    a.onfinish = end; setTimeout(end, 600);
  }
  /** The card or row of what was just marked leaves the tab it is in; one taken back comes into view again. */
  function leave(kind, id, done, ids, setOfSheet) {
    if (S.mode !== 'library') return;
    if (L.tab !== 'done') {
      const body = byId('libBody'); if (!body) return;
      if (!done) { L.flash = ids; renderCurrent(); return; }
      const gone = [];
      if (kind === 'set') { for (const c of body.querySelectorAll('.setCard')) if (c._laserSet && c._laserSet.setId === id) gone.push(c); }
      else for (const c of body.querySelectorAll(`.libCard[data-id="${CSS.escape(id)}"]`)) {
        const item = c.closest('.librarySheet') || c, set = item.closest('.setCard');
        const left = set ? [...set.querySelectorAll('.librarySheet')].filter(x => x !== item && !gone.includes(x) && !x.dataset.leaving).length : 1;
        gone.push(left ? item : set);
      }
      gone.forEach(el => collapse(el, () => { partials(body); LaserReview.changed(); emptyCurrent(body); }));
      partials(body);
      return;
    }
    const st = L.list; if (!st) return;
    if (done) return;                                   // (an undo of a move back: freshen brings it in at the top)
    const keys = kind === 'set' ? ['set:' + id, ...ids.map(x => 'sheet:' + x)] : ['sheet:' + id];
    // a sheet moved back from an opened set takes its set out of Completed: the set is not complete any more
    if (kind === 'sheet') {
      const inSet = doc.querySelector(`#libDone .ldPanelIn .libCard[data-id="${CSS.escape(id)}"]`);
      const item = inSet && inSet.closest('.ldItem'); if (item && item.dataset.set) keys.push('set:' + item.dataset.set);
      if (setOfSheet) keys.push('set:' + setOfSheet);
    }
    removeRows(st, keys);
  }
  function emptyCurrent(body) {
    if (body.querySelector('.libCard')) return;
    const f = focus(); if (f) return decorate(body);
    if (!body.querySelector(':scope > .libEmpty')) body.insertAdjacentHTML('beforeend', '<div class="libEmpty ldIn">Nothing left here: every sheet is under Completed.</div>');
  }
  /** "2 of 5 sheets completed" on a set in Current whose laser work has begun. */
  function partials(root) {
    for (const card of root.querySelectorAll('.setCard')) {
      const head = card.querySelector(':scope > .sh'); if (!head) continue;
      const all = (card._sheets || []).filter(r => !r.archived), n = all.filter(isDone).length;
      let tag = head.querySelector('.ldPartial');
      if (!n || n >= all.length) { if (tag) tag.remove(); continue; }
      if (!tag) { tag = doc.createElement('span'); tag.className = 'ldPartial'; (head.querySelector('.ldMarkSet') || head.querySelector('.nm')).after(tag); }
      tag.textContent = `${n} of ${all.length} sheets completed`;
    }
  }
  /** The check at each card's corner, and the set's own at its head: Mark completed in Current, Move back in Completed. */
  function cards(root, mode) {
    const done = mode === 'done';
    for (const c of root.querySelectorAll('.libCard[data-id]')) {
      let b = c.querySelector(':scope > .ldMark');
      if (b && b.dataset.done === (done ? '1' : '0')) continue;
      if (!b) { b = doc.createElement('button'); b.type = 'button'; b.className = 'ldMark'; c.appendChild(b); }
      b.dataset.ld = 'sheet:' + c.dataset.id; b.dataset.done = done ? '1' : '0'; b.innerHTML = done ? ICON.undo : ICON.check;
      b.title = done ? 'Move back to Current' : 'Mark completed · the laser has cut this sheet'; b.setAttribute('aria-label', done ? 'Move this sheet back to Current' : 'Mark this sheet completed');
    }
    for (const card of root.querySelectorAll('.setCard')) {
      const st = card._laserSet, head = card.querySelector(':scope > .sh');
      if (!st || !st.setId || st.standalone || st.working || !head) continue;
      let b = head.querySelector(':scope > .ldMarkSet');
      if (b && b.dataset.done === (done ? '1' : '0')) continue;
      if (!b) { b = doc.createElement('button'); b.type = 'button'; b.className = 'ldMark ldMarkSet'; (head.querySelector('.nm') || head.firstChild).after(b); }
      b.dataset.ld = 'set:' + st.setId; b.dataset.done = done ? '1' : '0';
      b.innerHTML = (done ? ICON.undo : ICON.check) + `<span>${done ? 'Move set back' : 'Mark set completed'}</span>`;
      b.title = done ? 'Move the set and its sheets back to Current' : 'Mark the set and each of its sheets completed';
    }
  }
  function act(kind, id, done, btn) {
    if (btn) btn.classList.add('busy');
    return mark(kind, id, done).catch(e => console.warn('Library: not marked', e)).finally(() => { if (btn && btn.isConnected) btn.classList.remove('busy'); });
  }

  /* ── counts on the tab ── */
  function setCounts(c) { if (!c) return; L.counts = { sheets: +c.sheets || 0, sets: +c.sets || 0 }; L.countsAt = Date.now(); paintCount(); }
  function paintCount(pulse) {
    const n = byId('libDoneCount'); if (!n) return;
    const kind = S.library.kind === 'sets' ? 'sets' : 'sheets', v = L.counts ? L.counts[kind] : null;
    n.textContent = v == null ? '' : v > 9999 ? Math.round(v / 1000) + 'k' : String(v);
    n.setAttribute('aria-label', v == null ? 'completed' : `${v} completed ${kind}`);
    if (pulse && !reduced()) { n.classList.remove('pulse'); void n.offsetWidth; n.classList.add('pulse'); }
  }
  function bump(kind, d, quiet) { if (!L.counts) return; L.counts[kind] = Math.max(0, (L.counts[kind] || 0) + d); paintCount(!quiet); }
  async function refreshCounts(force) {
    if (L.countsBusy || !S.cloud.ok || (!force && Date.now() - L.countsAt < FRESH)) return;
    L.countsBusy = true;
    try { setCounts((await api('charmNestLibrary', { op: 'laserDoneList', countOnly: true }, { quiet: true })).counts); }
    catch (_) { L.countsAt = Date.now(); }
    finally { L.countsBusy = false; }
  }

  /* ── search ── */
  const query = () => ((byId('libSearch') || {}).value || '').trim();
  /** A search that is a number (4 to 20 digits, "#" and spaces allowed): an order or a listing. */
  const digits = q => { const d = String(q || '').replace(/^#/, '').replace(/[\s,]/g, ''); return /^\d{4,20}$/.test(d) ? d : ''; };
  const numeric = q => { const d = String(q || '').replace(/^#/, '').replace(/[\s,]/g, ''); return /^\d{1,20}$/.test(d) ? d : ''; };
  /* The number looked up in every sheet (findSheets, which may read a month of runs): one that looks whole (9 digits or
     more: Etsy's order and listing numbers have 10) once the typing pauses, a shorter one once Enter is pressed. Every
     prefix of a number being typed used to run the whole lookup. While a shorter one is typed, Current follows the box
     as it does for words and Completed keeps its whole list; neither asks the cloud. */
  const WHOLE = 9;
  const lookup = () => { const n = digits(query()); return n && (n.length >= WHOLE || L.entered === n) ? n : ''; };
  const typing = () => { const d = numeric(query()); return !!d && d.length < WHOLE && L.entered !== d; };
  const foldText = s => String(s == null ? '' : s).toLowerCase().replace(/[._\-/·,:]+/g, ' ').replace(/\s+/g, ' ').trim();
  function input() {
    clearTimeout(L.timer); searching(!typing());
    L.timer = setTimeout(apply, 500);
  }
  function enter() {
    L.entered = numeric(query()); clearTimeout(L.timer);
    apply();
  }
  function apply() {
    L.timer = 0; L.located = '';
    const n = lookup();
    if (n) startFind(n);
    if (L.tab === 'done') showDone(); else renderCurrent();
    syncSearching();
  }
  function renderCurrent() {
    const body = byId('libBody'); if (!body || L.tab === 'done') return;
    if (S.library.kind === 'sets' && window.Sets) return Sets.renderLibrary(body, { reuse: true });
    if (!S.library.rows || !S.library.rows.length || S.library.loadedFor !== S.library.metal) return loadLibrary().catch(() => {});
    renderLibrary();
  }
  function startFind(q) {
    let f = L.finds.get(q);
    if (f && ((f.res && Date.now() - f.at < 120000) || (!f.res && !f.error))) return f;
    f = { q, res: null, error: null, at: Date.now() };
    L.finds.delete(q); L.finds.set(q, f);
    while (L.finds.size > 12) L.finds.delete(L.finds.keys().next().value);
    f.promise = api('charmNestLibrary', { op: 'findSheets', q, today: realDay(Date.now()) }, { quiet: true, timeoutMs: 120000 }).then(res => {
      f.res = res; f.at = Date.now();
      for (const s of res.sheets || []) LaserReview.record(s);
    }, e => { f.error = e; f.at = Date.now(); }).then(() => {
      if (lookup() !== q || S.mode !== 'library') return;
      try { if (L.tab === 'done') showDone(); else renderCurrent(); } catch (e) { console.warn('Library search', e); }
    }).finally(syncSearching);
    return f;
  }
  function searching(on) { const i = byId('libSearch'); if (i) i.classList.toggle('searching', !!on); }
  function syncSearching() {
    if (L.timer && !typing()) return searching(true);
    const n = lookup(), f = n && L.finds.get(n);
    searching(!!(f && !f.res && !f.error) || !!(L.tab === 'done' && query() && L.list && L.list.loading && !L.list.rows.length));
  }
  /** The number searched for and what the search found, for the Library's two views (renderLibrary, Sets). */
  function focus() {
    const q = lookup(); if (!q) return null;
    const f = L.finds.get(q), res = f && f.res;
    const ids = new Set(res ? [...(res.sheets || []), ...(res.rows || [])].map(s => s.id) : []);
    const has = list => (list || []).some(v => String(v) === q);
    return { q, loading: !!(f && !res && !f.error), error: f && f.error || null, res: res || null, ids, rows: res ? res.sheets || [] : [], sets: res ? res.sets || [] : [],
      test: r => ids.has(r.id || r.sheetId) || has(r.orders) || has(r.listings) };
  }
  /** Current's records, with the sheets the search found and those moved back that its list does not hold. */
  function rows(base, f, o = {}) {
    base = base || [];
    const have = new Set(base.map(r => r.id)), add = [];
    const metal = !o.allMetals && S.library.metal && S.library.metal !== 'all' ? S.library.metal : null;
    const take = r => { if (!r || !r.id || have.has(r.id) || (metal && r.metal !== metal)) return; have.add(r.id); add.push(r); };
    if (f) f.rows.forEach(take);
    for (const x of L.extra.values()) take(x.r);
    if (!add.length) return base;
    return base.concat(window.Gate && Gate.projectLibraryRecords ? Gate.projectLibraryRecords(add) : add);
  }
  function clearSearch() {
    const i = byId('libSearch'); if (!i) return;
    i.value = ''; clearTimeout(L.timer); L.timer = 0; L.located = ''; L.entered = '';
    if (L.tab === 'done') showDone(); else renderCurrent();
    syncSearching(); i.focus();
  }
  function matchWord(res) {
    const m = (res && res.matches) || {};
    return m.order && !m.listing ? 'order' : m.listing && !m.order ? 'listing' : 'order or listing';
  }
  const metalNow = () => S.library.metal && S.library.metal !== 'all' ? S.library.metal : null;
  const metalOk = (r, metal) => !metal || (r.kind === 'set' ? (r.materials || []).includes(metal) || (r.sheets || []).some(s => s.metal === metal) : r.metal === metal);
  /** The first line of a list that answers a search: what was found, where the rest is, and the × that clears it. */
  function foundLine(tab, st) {
    const q = query(); if (!q) return null;
    const f = focus(), line = doc.createElement('div'); line.className = 'ldFound'; line.setAttribute('role', 'status');
    let html = '';
    const x = '<button type="button" class="ldClear" aria-label="Clear the search" title="Clear the search">×</button>';
    if (f) {
      if (f.loading) html = `<i class="spin" aria-hidden="true"></i><span class="ldFoundT">Looking through every sheet for order or listing <b>${esc(f.q)}</b>…</span>`;
      else if (f.error) html = `<span class="ldFoundT">Could not look up <b>${esc(f.q)}</b>: ${esc(f.error.message)}</span><button type="button" class="ldGo" data-ld-refind="${esc(f.q)}">Retry</button>`;
      else {
        const metal = metalNow(), res = f.res, word = matchWord(res);
        const cur = (res.sheets || []).filter(r => !isDone(r) && metalOk(r, metal)), done = (res.rows || []).filter(r => isDone(r) && metalOk(r, metal));
        const mine = tab === 'done' ? done : cur, other = tab === 'done' ? cur : done;
        const sets = new Set(mine.map(r => r.setId).filter(Boolean)).size;
        const kind = tab === 'done' ? 'completed sheet' : 'sheet';
        html = mine.length
          ? `<span class="ldFoundT"><b>${plural(mine.length, kind)}</b> ${mine.length === 1 ? 'holds' : 'hold'} ${word} <b>${esc(f.q)}</b>${sets ? ` · in ${plural(sets, 'set')}` : ''}</span>`
          : `<span class="ldFoundT">No ${tab === 'done' ? 'completed' : 'current'} sheet${metal ? ` of ${esc(metalOf({ metal }).label || metal)}` : ''} holds order or listing <b>${esc(f.q)}</b></span>`;
        if (other.length) html += `<button type="button" class="ldGo" data-t="${tab === 'done' ? 'current' : 'done'}">${other.length} in ${tab === 'done' ? 'Current' : 'Completed'} ›</button>`;
        // older sheets record no listing: whenever their runs were read, the line says which days (found or not)
        const fb = res.fallback;
        if (fb && fb.window) html += `<span class="ldNote" title="Older sheets record no listing: their orders were read from the runs of ${esc(fb.window.from + ' to ' + fb.window.to)}${fb.capped ? ', and not all of them' : ''}">older sheets: since ${esc(dayShort(fb.window.from))}${fb.capped ? ', in part' : ''}</span>`;
        if (res.truncated) html += '<span class="ldNote">only the first shown</span>';
      }
    } else if (typing()) {
      // a short number is looked up on Enter (lookup): until then Current follows the box, and Completed shows its list
      const body = byId('libBody'), n = tab === 'done' || !body ? -1 : (S.library.kind === 'sets' ? body.querySelectorAll('.setCard').length : body.querySelectorAll('.libCard[data-id]').length);
      html = (n < 0 ? '' : `<span class="ldFoundT"><b>${plural(n, S.library.kind === 'sets' ? 'set' : 'sheet')}</b> ${n === 1 ? 'matches' : 'match'} “${esc(q)}”</span>`)
        + `<span class="${n < 0 ? 'ldFoundT' : 'ldNote'}">Enter looks up order or listing <b>${esc(numeric(q))}</b></span>`;
    } else if (tab === 'done') {
      if (!st) return null;
      const n = st.rows.length, what = st.kind === 'sets' ? 'set' : 'sheet';
      html = st.loading && !n ? `<i class="spin" aria-hidden="true"></i><span class="ldFoundT">Looking for completed ${what}s with “${esc(q)}”…</span>`
        : !n && st.end ? `<span class="ldFoundT">No completed ${what}${st.metal ? ` of ${esc(metalOf({ metal: st.metal }).label || st.metal)}` : ''} matches “${esc(q)}”</span>`
        : `<span class="ldFoundT"><b>${n}${st.end ? '' : '+'}</b> completed ${n === 1 && st.end ? what : what + 's'} ${n === 1 && st.end ? 'matches' : 'match'} “${esc(q)}”</span>`;
    } else {
      const body = byId('libBody'), n = body ? (S.library.kind === 'sets' ? body.querySelectorAll('.setCard').length : body.querySelectorAll('.libCard[data-id]').length) : 0;
      html = `<span class="ldFoundT"><b>${plural(n, S.library.kind === 'sets' ? 'set' : 'sheet')}</b> ${n === 1 ? 'matches' : 'match'} “${esc(q)}”</span>`;
    }
    line.innerHTML = html + x;
    return line;
  }
  /** The sheets a number was found on are lit, and the first is brought into view, once for each answer. */
  function locate(root) {
    const f = focus(); if (!f || !f.res) return;
    const k = f.q + '|' + L.tab + '|' + S.library.kind + '|' + (S.library.metal || '');
    if (L.located === k) return;
    const hits = [...root.querySelectorAll('.libCard[data-id], .ldItem[data-kind="sheet"]')].filter(c => f.ids.has(c.dataset.id)).map(c => c.classList.contains('ldItem') ? c.querySelector('.ldLine') : c);
    for (const it of root.querySelectorAll('.ldItem[data-kind="set"]')) { const r = L.list && L.list.byKey.get('set:' + it.dataset.set); if (r && (r.sheets || []).some(s => f.ids.has(s.id))) hits.push(it.querySelector('.ldLine')); }
    if (!hits.length) return;
    L.located = k;
    glow(hits);
    requestAnimationFrame(() => { if (hits[0].isConnected) hits[0].scrollIntoView({ block: 'nearest', behavior: reduced() ? 'auto' : 'smooth' }); });
  }
  function glow(els) {
    for (const el of els) { el.classList.remove('ldHit'); void el.offsetWidth; el.classList.add('ldHit'); }
    setTimeout(() => els.forEach(el => el.classList.remove('ldHit')), 4200);
  }
  /** Finishes Current once it is drawn (renderLibrary, the Sets view): the marks, the search's line and its lit cards. */
  function decorate(body) {
    body = body || byId('libBody'); if (!body) return;
    L.currentQ = query();
    cards(body, 'current'); partials(body);
    body.querySelectorAll(':scope > .ldFound').forEach(n => n.remove());
    const empty = body.querySelector(':scope > .libEmpty'); if (empty && !empty.textContent.trim()) empty.remove();
    const line = foundLine('current'); if (line) body.prepend(line);
    locate(body);
    if (L.flash) { const ids = new Set(L.flash); L.flash = null; for (const c of body.querySelectorAll('.libCard[data-id]')) if (ids.has(c.dataset.id)) { c.classList.add('ldIn'); setTimeout(() => c.classList.remove('ldIn'), 400); } }
    paintCount(); refreshCounts(); syncSearching(); fitPlaceholder();
  }

  /* ── Completed: the list ── */
  const rowKey = r => r.kind === 'set' ? 'set:' + r.setId : 'sheet:' + r.id;
  function listKey() {
    const q = query(), n = lookup(), m = metalNow() || 'all';
    return `${S.library.kind === 'sets' ? 'sets' : 'sheets'}|${m}|${n ? '#' + n : typing() ? '' : foldText(q)}`;
  }
  function newList(key) {
    const [kind, metal, q] = key.split('|');
    const el = doc.createElement('div'); el.className = 'ldList';
    el.innerHTML = '<div class="ldRows"></div><div class="ldMore" aria-live="polite"></div>';
    return { key, kind, metal: metal === 'all' ? null : metal, q: q.startsWith('#') ? '' : q, find: q.startsWith('#') ? q.slice(1) : '', el, rowsEl: el.firstChild, moreEl: el.lastChild,
      rows: [], byKey: new Map(), next: null, end: false, loading: false, error: null, empties: 0, scanned: 0, at: 0, seq: 0, pending: null, scroll: 0, dropped: false };
  }
  function dropList(key) {
    const st = key && L.lists.get(key); if (!st) return;
    st.dropped = true; L.lists.delete(key);
    if (L.list === st) { L.list = null; if (L.io) L.io.disconnect(); }
  }
  function showDone(force) {
    const host = byId('libDone'); if (!host) return;
    panels();
    if (!S.cloud.ok) {
      if (!L.list) host.innerHTML = `<div class="libEmpty">${S.cloud.ok === false ? 'Cloud offline — the Completed list needs the Netlify functions.' : 'Connecting to the cloud…'}</div>`;
      syncSearching(); return;
    }
    const key = listKey();
    let st = L.lists.get(key);
    if (st && (force || Date.now() - st.at > KEEP_LIST && st.at)) { dropList(key); st = null; }
    if (!st) { st = newList(key); L.lists.set(key, st); while (L.lists.size > MAX_LISTS) dropList(L.lists.keys().next().value); }
    else { L.lists.delete(key); L.lists.set(key, st); }
    if (L.list !== st || !host.contains(st.el)) {
      if (L.list && L.list !== st) { L.list.scroll = stage().scrollTop; if (!st.rows.length && S.mode === 'library') stage().scrollTop = 0; }
      host.replaceChildren(st.el); L.list = st;
      settle(st.el); observe(st);
      if (st.rows.length) requestAnimationFrame(() => { if (L.list === st) stage().scrollTop = st.scroll || 0; });
    }
    paintFound(st);
    if (!st.rows.length && !st.loading && !st.end && !st.error) loadMore(st);
    else if (st.at && Date.now() - st.at > FRESH) freshen(st);
    else more(st);
    locate(host); paintCount(); refreshCounts(); syncSearching();
  }
  function paintFound(st) {
    const old = st.el.querySelector(':scope > .ldFound'), line = foundLine('done', st);
    if (old && line) { if (old.innerHTML !== line.innerHTML) old.innerHTML = line.innerHTML; return; }   // (changed in place: no fade again)
    if (old) old.remove(); if (line) st.el.prepend(line);
  }
  function observe(st) {
    if (!window.IntersectionObserver) return;
    if (L.io) L.io.disconnect();
    L.io = new IntersectionObserver(es => { if (es.some(e => e.isIntersecting) && L.list === st) loadMore(st); }, { root: stage(), rootMargin: '0px 0px 900px 0px' });
    L.io.observe(st.moreEl);
  }
  function near(st) {
    if (L.list !== st || !st.el.isConnected || S.mode !== 'library' || L.tab !== 'done') return false;
    const a = st.moreEl.getBoundingClientRect(), b = stage().getBoundingClientRect();
    return a.top < b.bottom + 900;
  }
  async function loadMore(st) {
    if (st.loading || st.end || st.dropped || st.rows.length >= MAX_ROWS) return;
    st.loading = true; st.error = null; more(st); syncSearching();
    try {
      if (st.find) {
        if (!st.pending) {
          const f = startFind(st.find); await f.promise;
          if (f.error) throw f.error;
          const res = f.res, sets = st.kind === 'sets' ? (res.setRows || []) : [];
          const inSets = new Set(sets.map(r => r.setId));
          // Sets: the completed sets that hold it, and a completed sheet whose set is not complete (or that has none)
          st.pending = [...sets, ...(res.rows || []).filter(r => st.kind !== 'sets' || !inSets.has(r.setId))].filter(r => metalOk(r, st.metal)).sort((a, b) => (b.at || 0) - (a.at || 0));
        }
        if (st.dropped) return;
        append(st, st.pending.splice(0, CHUNK));
        st.end = !st.pending.length; st.at = st.at || Date.now();
      } else {
        const r = await api('charmNestLibrary', { op: 'laserDoneList', kind: st.kind, limit: PAGE, cursor: st.next || undefined, metal: st.metal || undefined, q: st.q || undefined }, { quiet: true });
        if (st.dropped) return;
        if (r.counts) setCounts(r.counts);
        const fresh = (r.rows || []).filter(x => !st.byKey.has(rowKey(x)));
        append(st, fresh);
        st.next = r.next || null; st.end = !r.next; st.scanned += +r.scanned || 0;
        st.empties = fresh.length ? 0 : st.empties + 1; st.at = st.at || Date.now();
      }
    } catch (e) { if (!st.dropped) st.error = e; }
    finally { st.loading = false; if (!st.dropped) { more(st); paintFound(st); if (L.list === st) locate(byId('libDone')); } syncSearching(); }
    // a search that matched little in what one call reads goes on while the end of the list is in view
    if (!st.error && !st.end && !st.dropped && st.empties < 8 && near(st)) requestAnimationFrame(() => loadMore(st));
  }
  /** Rows newer than the list's first (marked since it was read) come in at its top. */
  async function freshen(st, now) {
    if (st.find || st.freshening || st.dropped || (!now && Date.now() - st.at < FRESH)) return;
    st.freshening = true;
    try {
      const r = await api('charmNestLibrary', { op: 'laserDoneList', kind: st.kind, limit: PAGE, metal: st.metal || undefined, q: st.q || undefined }, { quiet: true });
      if (st.dropped) return;
      if (r.counts) setCounts(r.counts);
      const head = st.rows.length ? st.rows[0].at || 0 : 0, fresh = (r.rows || []).filter(x => !st.byKey.has(rowKey(x)) && (x.at || 0) >= head);
      st.at = Date.now();
      if (fresh.length && fresh.length === (r.rows || []).length && r.next) { dropList(st.key); if (L.tab === 'done') showDone(); return; }
      if (fresh.length) prepend(st, fresh);
    } catch (_) { /* the list stays as it is */ }
    finally { st.freshening = false; if (!st.dropped) more(st); }
  }
  function more(st) {
    const m = st.moreEl; let html = '';
    const what = st.kind === 'sets' ? 'set' : 'sheet';
    st.rowsEl.querySelectorAll(':scope > .skel').forEach(n => { if (st.rows.length || !st.loading) n.remove(); });
    if (st.loading && !st.rows.length) {
      if (!st.rowsEl.querySelector('.skel')) st.rowsEl.insertAdjacentHTML('beforeend', Array.from({ length: 6 }, () => '<div class="ldItem skel" aria-hidden="true"><div class="ldLine"><i></i><i></i><i></i><i></i></div></div>').join(''));
    } else if (st.loading) html = '<i class="spin" aria-hidden="true"></i> Loading older…';
    else if (st.error) html = `Could not load: ${esc(st.error.message)} <button type="button" class="btn ghost xs" data-ld-retry>Retry</button>`;
    else if (!st.rows.length && st.end) html = '';
    else if (st.rows.length >= MAX_ROWS) html = `The newest ${MAX_ROWS.toLocaleString()} are shown · search for an order, a listing or a date to find older ones`;
    else if (st.end) html = st.find ? '' : `${plural(st.rows.length, 'completed ' + what)}${st.q || st.metal ? ' shown' : ''} · that is all`;
    else if (st.empties >= 8) html = `Nothing more in the ${st.scanned.toLocaleString()} read <button type="button" class="btn ghost xs" data-ld-more>Keep looking</button>`;
    if (m.innerHTML !== html) m.innerHTML = html;
    const empty = st.rowsEl.querySelector(':scope > .libEmpty');
    if (!st.rows.length && st.end && !st.loading && !st.error) {
      const text = st.find || st.q ? '' : st.metal ? `No completed ${esc(metalOf({ metal: st.metal }).label || st.metal)} ${what}s yet.` : `Nothing is completed yet. <b>Mark a ${what} completed in Current with the check at its corner once the laser has cut it.</b>`;
      if (!empty && text) st.rowsEl.insertAdjacentHTML('beforeend', `<div class="libEmpty">${text}</div>`);
    } else if (empty) empty.remove();
  }
  function removeRows(st, keys) {
    const want = new Set(keys);
    for (const k of want) st.byKey.delete(k);
    st.rows = st.rows.filter(r => !want.has(rowKey(r)));
    for (const it of st.rowsEl.querySelectorAll('.ldItem')) {
      const k = it.dataset.kind === 'set' ? 'set:' + it.dataset.set : 'sheet:' + it.dataset.id;
      if (!want.has(k)) continue;
      const day = it.closest('.ldDay');
      collapse(it, () => { if (day && !day.querySelector('.ldItem')) collapse(day, () => more(st)); else if (day) countDay(day, st); more(st); paintFound(st); });
    }
  }

  /* ── Completed: the rows ── */
  const thumb = url => `<span class="ldThumb">${url ? `<img alt="" loading="lazy" decoding="async" crossorigin="anonymous" src="${esc(cors(url))}">` : ''}</span>`;
  function who(r) {
    const t = r.at ? new Date(r.at) : null;
    return `<span class="ldWho" title="${t ? esc('Completed ' + t.toLocaleString()) : ''}${r.by ? esc(' by ' + r.by) : ''}">${ICON.done}<span>${esc(r.by || '—')}</span>${t ? `<time datetime="${t.toISOString()}">${t.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}</time>` : ''}</span>`;
  }
  function rowHtml(r) {
    if (r.kind === 'set') {
      const sheets = r.sheets || [], per = new Map(); for (const s of sheets) per.set(s.metal, (per.get(s.metal) || 0) + 1);
      const mats = (r.materials && r.materials.length ? r.materials : [...per.keys()]).filter(Boolean);
      return `<div class="ldItem" data-kind="set" data-set="${esc(r.setId)}"><div class="ldLine" role="button" tabindex="0" aria-expanded="false" aria-label="${esc(`Set ${r.seq || ''}, ${plural(sheets.length, 'sheet')}: open`)}">`
        + `<span class="ldThumbs">${(sheets.length ? sheets.slice(0, 3) : [{}]).map(s => thumb(s.preview)).join('')}</span>`
        + `<span class="ldName"><b>Set ${esc(r.seq || '—')}</b><span class="sws">${mats.map(k => swatch(metalOf({ metal: k }), per.get(k) || 0)).join('')}</span><span class="ldDate" title="Set day">${esc(dayShort(r.day))}</span></span>`
        + `<span class="ldNums"><span><b>${sheets.length}</b> ${sheets.length === 1 ? 'sheet' : 'sheets'}</span><span><b>${+r.orders || 0}</b> orders</span><span><b>${+r.pieces || 0}</b> pcs</span><span><b>${pct(r.fill)}</b> full</span></span>`
        + who(r) + `<button type="button" class="ldBack" data-ld-back="set:${esc(r.setId)}" title="Move the set and its sheets back to Current">Move back</button>${ICON.chev}</div>`
        + '<div class="ldPanel"><div class="ldPanelIn"></div></div></div>';
    }
    const sn = !r.draft && r.setSeq ? r.setSeq : 0;
    return `<div class="ldItem" data-kind="sheet" data-id="${esc(r.id)}"><div class="ldLine" role="button" tabindex="0" data-m="${esc(r.metal || '')}" title="${esc(r.fileBase || r.id)}" aria-label="${esc(`${CODE[r.metal] || ''} Sheet ${r.sheetIndex || 1}${sn ? ', Set ' + sn : ''}: open`)}">`
      + thumb(r.preview)
      + `<span class="ldName">${swatch(metalOf(r), 0)}<b>Sheet ${esc(r.sheetIndex || 1)}</b>${sn ? `<span class="ldSet">Set ${esc(sn)}</span>` : ''}<span class="ldDate" title="Sheet day">${esc(dayShort(r.day))}</span></span>`
      + `<span class="ldNums"><span><b>${+r.orders || 0}</b> ${r.orders === 1 ? 'order' : 'orders'}</span><span><b>${+r.pieces || 0}</b> pcs</span><span><b>${pct(r.fill)}</b> full</span></span>`
      + who(r) + `<button type="button" class="ldBack" data-ld-back="sheet:${esc(r.id)}" title="Move back to Current">Move back</button>${ICON.chev}</div></div>`;
  }
  function dayHead(day, kind) {
    const t = realDay(Date.now()), y = realDay(Date.now() - 86400000), d = new Date(day + 'T12:00:00');
    const year = d.getFullYear() !== new Date().getFullYear() ? { year: 'numeric' } : {};
    const date = d.toLocaleDateString(undefined, Object.assign({ month: 'short', day: 'numeric' }, year));
    const title = day === t ? 'Today' : day === y ? 'Yesterday' : d.toLocaleDateString(undefined, { weekday: 'long' });
    return `<section class="ldDay" data-day="${esc(day)}"><h3 class="ldDayHead">${esc(title)}<span>${esc(date)}</span><i data-kind="${kind}"></i></h3><div class="ldDayRows"></div></section>`;
  }
  function countDay(sec) {
    const n = sec.querySelectorAll('.ldDayRows > .ldItem').length, i = sec.querySelector('.ldDayHead i');
    if (i) i.textContent = plural(n, i.dataset.kind === 'set' ? 'set' : 'sheet');
  }
  /** Rows drawn in one piece per day: a page added below never draws the list again. */
  function append(st, list) {
    if (!list.length) return;
    st.rowsEl.querySelectorAll(':scope > .skel, :scope > .libEmpty').forEach(n => n.remove());
    const groups = []; for (const r of list) { st.rows.push(r); st.byKey.set(rowKey(r), r); const day = r.at ? realDay(r.at) : 'undated'; const g = groups[groups.length - 1]; if (g && g.day === day) g.rows.push(r); else groups.push({ day, rows: [r] }); }
    for (const g of groups) {
      let sec = st.rowsEl.lastElementChild;
      if (!sec || sec.dataset.day !== g.day) { st.rowsEl.insertAdjacentHTML('beforeend', dayHead(g.day, st.kind === 'sets' ? 'set' : 'sheet')); sec = st.rowsEl.lastElementChild; }
      sec.lastElementChild.insertAdjacentHTML('beforeend', g.rows.map(rowHtml).join(''));
      countDay(sec);
    }
    settle(st.el);
  }
  function prepend(st, list) {
    st.rowsEl.querySelectorAll(':scope > .skel, :scope > .libEmpty').forEach(n => n.remove());
    const added = [];
    for (const r of list.slice().reverse()) {
      st.rows.unshift(r); st.byKey.set(rowKey(r), r);
      const day = r.at ? realDay(r.at) : 'undated'; let sec = st.rowsEl.firstElementChild;
      if (!sec || sec.dataset.day !== day) { st.rowsEl.insertAdjacentHTML('afterbegin', dayHead(day, st.kind === 'sets' ? 'set' : 'sheet')); sec = st.rowsEl.firstElementChild; }
      sec.lastElementChild.insertAdjacentHTML('afterbegin', rowHtml(r)); countDay(sec); added.push(sec.lastElementChild.firstElementChild);
    }
    for (const it of added) { it.classList.add('ldIn'); setTimeout(() => it.classList.remove('ldIn'), 400); }
    settle(st.el); paintFound(st);
  }
  /** A thumbnail already loaded (from the cache, or while its list was off screen) is shown without waiting. */
  function settle(root) { requestAnimationFrame(() => { for (const img of root.querySelectorAll('.ldThumb img:not(.on)')) if (img.complete && img.naturalWidth) img.classList.add('on'); }); }

  /* ── Completed: a set opened in place ── */
  async function toggleSet(item) {
    const open = !item.classList.contains('open'), row = item.querySelector('.ldLine'), inner = item.querySelector('.ldPanelIn');
    item.classList.toggle('open', open); row.setAttribute('aria-expanded', open ? 'true' : 'false');
    if (!open || (inner.firstElementChild && !inner.querySelector('.ldWait'))) { if (open) LaserReview.changed(); return; }
    const r = L.list && L.list.byKey.get('set:' + item.dataset.set); if (!r) return;
    inner.innerHTML = '<div class="ldWait"><i class="spin" aria-hidden="true"></i> Opening the set…</div>';
    try {
      const ids = (r.sheetIds && r.sheetIds.length ? r.sheetIds : (r.sheets || []).map(s => s.id)).slice(0, 300);
      const [st, got] = await Promise.all([api('charmNestLibrary', { op: 'laserStatus', sheetIds: ids, setIds: [r.setId] }, { quiet: true }), api('charmNestLibrary', { op: 'setGet', setId: r.setId }, { quiet: true })]);
      const sheets = (st.sheets || []).map(s => LaserReview.record(s)).sort((a, b) => String(a.metal || '').localeCompare(String(b.metal || '')) || (a.sheetIndex || 0) - (b.sheetIndex || 0));
      const set = Object.assign({ setId: r.setId, seq: r.seq, day: r.day, name: r.name, sheetIds: ids, materials: r.materials, status: r.status }, got.set || {}, { setId: r.setId });
      const card = Sets.libraryCard(set, sheets, sheets, { onUndo: () => { for (const k of [...L.lists.keys()]) dropList(k); showDone(true); } });
      const h0 = inner.offsetHeight;
      inner.replaceChildren(card); cards(inner, 'done'); LaserReview.changed();
      if (!reduced() && inner.animate && item.classList.contains('open')) { const h1 = inner.offsetHeight; inner.animate([{ height: h0 + 'px' }, { height: h1 + 'px' }], { duration: 240, easing: 'cubic-bezier(.2,.8,.2,1)' }); }
    } catch (e) {
      inner.innerHTML = `<div class="ldWait">Could not open the set: ${esc(e.message)} <button type="button" class="btn ghost xs" data-ld-retry>Retry</button></div>`;
    }
  }
  function openRow(row) {
    const item = row.closest('.ldItem'); if (!item || item.classList.contains('skel')) return;
    if (item.dataset.kind === 'set') toggleSet(item);
    else openLibrarySheet(item.dataset.id);
  }

  /* ── one listener for each tab's clicks ── */
  function onMarkClick(e) {
    const t = e.target; if (!(t instanceof Element)) return;
    const m = t.closest('.ldMark, [data-ld-back]');
    if (m) {
      e.preventDefault(); e.stopPropagation();
      const [kind, id] = String(m.dataset.ld || m.dataset.ldBack).split(/:(.*)/s);
      act(kind, id, m.classList.contains('ldMark') ? m.dataset.done !== '1' : false, m);
      return;
    }
    const x = t.closest('.ldClear'); if (x) { e.preventDefault(); e.stopPropagation(); clearSearch(); return; }
    const go = t.closest('.ldGo[data-t]'); if (go) { e.preventDefault(); e.stopPropagation(); setTab(go.dataset.t); return; }
    const refind = t.closest('[data-ld-refind]'); if (refind) { e.preventDefault(); e.stopPropagation(); L.finds.delete(refind.dataset.ldRefind); for (const [k, st] of L.lists) if (st.find === refind.dataset.ldRefind) dropList(k); apply(); return; }
  }
  function onDoneClick(e) {
    const t = e.target; if (!(t instanceof Element)) return;
    const retry = t.closest('[data-ld-retry]');
    if (retry) {
      const item = retry.closest('.ldItem');
      if (item) { item.querySelector('.ldPanelIn').innerHTML = ''; item.classList.remove('open'); toggleSet(item); return; }
      const st = L.list; if (!st) return;
      if (st.find) { L.finds.delete(st.find); st.pending = null; }
      st.error = null; loadMore(st); return;
    }
    if (t.closest('[data-ld-more]')) { const st = L.list; if (st) { st.empties = 0; loadMore(st); } return; }
    const row = t.closest('.ldLine'); if (!row || t.closest('a, button, details, input')) return;
    openRow(row);
  }
  function onDoneKey(e) {
    if ((e.key !== 'Enter' && e.key !== ' ') || !(e.target instanceof Element) || !e.target.classList.contains('ldLine')) return;
    e.preventDefault(); openRow(e.target);
  }

  /* ── bounded: what a station left on for days keeps ── */
  function upkeep() {
    const t = Date.now();
    for (const [k, m] of L.marks) if (t - m.t > 12 * 3600000) L.marks.delete(k);
    while (L.marks.size > 2000) L.marks.delete(L.marks.keys().next().value);
    for (const [k, x] of L.extra) if (t - x.t > 30 * 60000) L.extra.delete(k);
    for (const [k, f] of L.finds) if ((f.res || f.error) && t - f.at > 10 * 60000) L.finds.delete(k);
    if (S.mode === 'library') { L.leftAt = 0; return; }
    if (!L.leftAt) L.leftAt = t;
    else if (t - L.leftAt > AWAY && L.lists.size) {
      for (const k of [...L.lists.keys()]) dropList(k);
      const host = byId('libDone'); if (host) host.replaceChildren();
    }
  }

  function init() {
    const bar = byId('libTab'), body = byId('libBody'), done = byId('libDone'); if (!bar || !body || !done) return;
    L.tab = readTab(); paintTabs(); panels();
    bar.addEventListener('click', e => { const b = e.target.closest('button[data-t]'); if (b) setTab(b.dataset.t); });
    bar.addEventListener('keydown', e => {
      if (!['ArrowLeft', 'ArrowRight', 'Home', 'End'].includes(e.key)) return;
      e.preventDefault(); const next = e.key === 'Home' ? 'current' : e.key === 'End' ? 'done' : L.tab === 'done' ? 'current' : 'done';
      setTab(next); const b = bar.querySelector(`[data-t="${next}"]`); if (b) b.focus();
    });
    // Enter looks up a number however short (the box looks up one of 9 digits or more by itself)
    { const box = byId('libSearch'); if (box) box.addEventListener('keydown', e => { if (e.key === 'Enter' && !e.isComposing) { e.preventDefault(); enter(); } }); }
    body.addEventListener('click', onMarkClick, true); done.addEventListener('click', onMarkClick, true);
    done.addEventListener('click', onDoneClick); done.addEventListener('keydown', onDoneKey);
    done.addEventListener('load', e => { const i = e.target; if (i && i.tagName === 'IMG' && i.parentElement && i.parentElement.classList.contains('ldThumb')) i.classList.add('on'); }, true);
    done.addEventListener('error', e => { const i = e.target; if (i && i.tagName === 'IMG' && i.parentElement && i.parentElement.classList.contains('ldThumb')) i.remove(); }, true);
    stage().addEventListener('scroll', () => { if (S.mode === 'library') L.scroll[L.tab] = stage().scrollTop; }, { passive: true });
    // the count follows Sheets | Sets, in either tab
    for (const b of doc.querySelectorAll('#libKind button')) b.addEventListener('click', () => requestAnimationFrame(() => paintCount()));
    setInterval(upkeep, 60000);
    // the box's width changes with the window, the rail, the count on the tab: its words are fitted again
    { let queued = false; const fit = () => { if (queued) return; queued = true; requestAnimationFrame(() => { queued = false; fitPlaceholder(); }); };
      if (window.ResizeObserver) new ResizeObserver(fit).observe(byId('libSearch')); else addEventListener('resize', fit); }
    if (S.mode === 'library') { writeHash(); if (L.tab === 'done') showDone(); else { const b = byId('libBody'); if (b.querySelector('.libCard, .libEmpty')) decorate(b); } }
  }

  window.LibraryDone = { mark, isDone, tab: () => L.tab, setTab, show, focus, rows, decorate, input, fromHash, counts: () => L.counts && Object.assign({}, L.counts) };
  if (doc.readyState === 'loading') doc.addEventListener('DOMContentLoaded', init); else init();
})();
