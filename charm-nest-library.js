/* The Library's two tabs, Current | Completed (Paul, 25 Sep).
   The laser operator marks a sheet, or a whole set, completed with the check at its corner. It leaves Current (the
   Laser cutting and In progress lists) only once its entire set is finished. Until then its completion is a seal.
   Completed is grouped by completion day and read a page at a time as the list is scrolled. A set there opens in place into the same card
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
    timer: 0, leftAt: 0, busy: new Set(), io: null,
    flying: { sheets: 0, sets: 0 },   // marked, still on their way to the Completed tab: its count takes them as they land
    coming: new Map(),      // Completed row key → { from, until }: a row an Undo brings back, flying in from that tab
    flew: new Map(),        // "sheet:<id>" | "set:<id>" → when it was seen fly back in (the Undo's note is then not needed)
    pendingSeals: new Set(), presses: new Set()
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
    byId("libActivity")?.remove();
    CNListActivity.mount(byId("libView").querySelector(".libBar"),"library",()=>{ if(L.tab==="done")showDone();else renderCurrent(); });
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
  function isFiled(r) {
    if (!isDone(r)) return false;
    if (!r.setId || r.kind === 'set' || Array.isArray(r.sheetIds) || r.draft || r.solidIncluded === false) return true;
    const mark = L.marks.get('set:' + r.setId);
    return mark ? mark.done : !r.laserSetPending;
  }
  /** The cards of Current found once for a whole pass: canComplete asked each card's own question with a search of the whole list
      (300 cards, a few thousand elements each), which made a refresh frame slower the more sheets there were. */
  function cardIndex(root) {
    let sheets = null, sets = null;
    return {
      sheet(id) {
        if (!sheets) { sheets = new Map(); for (const c of root.querySelectorAll('.libCard[data-id]')) if (!sheets.has(c.dataset.id)) sheets.set(c.dataset.id, c); }
        return sheets.get(String(id)) || null;
      },
      set(id) {
        if (!sets) { sets = new Map(); for (const c of root.querySelectorAll('.setCard')) { const k = c._laserSet?.setId; if (k !== undefined && !sets.has(k)) sets.set(k, c); } }
        return sets.get(id) || null;
      }
    };
  }
  /** One pass over the cards (LaserReview.batch): what every order row says about its pieces is worked out once for it. */
  const pass = fn => LaserReview.batch ? LaserReview.batch(fn) : fn();
  function canComplete(kind, id, at) {
    const root = byId('libBody');
    if (S.mode !== 'library' || L.tab === 'done' || !root) return false;
    const card = at ? (kind === 'set' ? at.set(id) : at.sheet(id))
      : kind === 'set' ? [...root.querySelectorAll('.setCard')].find(c=>c._laserSet?.setId===id)
      : root.querySelector(`.libCard[data-id="${CSS.escape(id)}"]`);
    const group = card && (card.closest('[data-laser-card]') || card);
    if (!card || !card.closest('[data-laser-area="ready"]') || !group._laserSheets) return false;
    const sheets = group._laserSheets.map(recordOf).filter(Boolean);
    return group._laserSet ? LaserReview.group(group._laserSet,sheets).ready : sheets.length===1 && LaserReview.canCut(sheets[0]);
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
    const all = [...(S.library.rows || []), ...[...doc.querySelectorAll('.setCard')].flatMap(c=>c._sheets || [])];
    for (const r of all) if (want.has(r.id)) { r.laserDoneAt = at || null; CNListActivity.touch(r); LaserReview.record(r); }
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
  /** Records laser completion, or undoes it, with who and when. A sheet keeps its
      place until the server confirms that every member of its set is completed. */
  async function mark(kind, id, done = true, o = {}) {
    kind = kind === 'set' ? 'set' : 'sheet'; done = done !== false; id = String(id || '');
    if (!id) throw new Error('No sheet or set to mark');
    const key = kind + ':' + id;
    if (L.busy.has(key)) return null;
    if (done && !canComplete(kind,id)) {
      const message = 'Complete sheets from Laser cutting once every remaining sheet in the set is ready';
      toast(message,'bad',5000); throw new Error(message);
    }
    const staff = window.CNEmployee || { name: () => '', ask: () => '' };
    let by = o.by || staff.name();
    if (done && !by) by = typeof staff.need === 'function' ? await staff.need('Kept with this completed mark.') : staff.ask();
    if (done && !by) throw new Error('Say who marked it completed');
    if (!S.cloud.ok) throw new Error('The cloud is offline');
    L.busy.add(key);
    const ids = kind === 'sheet' ? [id] : setSheets(id), name = o.name || nameOf(kind,id);
    try {
      const via = o.via || (o.undo ? 'undo' : done ? 'Laser cutting' : 'Library Completed');
      // Nothing disappears or gains a seal until the server accepts the laser-stage check.
      const r = await api('charmNestLibrary',{op:'laserDone',kind,id,done,by:by || undefined,stage:done?'laser':undefined,device:'charm-nest-1',via},{label:done?'Marking completed':'Returning to Laser cutting'});
      const t = r.at || Date.now();
      // the efficiency record (station-activity.js, through charm-nest-laser-act.js): the laser operator's check, or taking it back (an undo).
      // It is the Laser station's work, not the sorter's: one event for each order on the sheets this mark changed, with the sheet and its charms
      try { window.CNLaserAct && window.CNLaserAct.cut(done, { name, ids: r.sheetIds && r.sheetIds.length ? r.sheetIds : ids, rec: recordOf }); } catch (_) {}
      for (const x of r.sheetIds || []) note('sheet:' + x,done,t,r.by);
      if (kind === 'set') note(key,done,t,r.by);
      if (r.setId && r.setDone != null) note('set:' + r.setId,!!r.setDone,t,r.by);
      pages(r.sheetIds || [],done?t:null); records(r.sheetIds || [],done?t:null);
      LaserReview.acceptProcess(r.process || []);
      for(const p of r.process || []){
        if(p.kind==='sheet'){
          const rec=recordOf(p.id);if(rec)Object.assign(rec,p.patch);
          for(const c of doc.querySelectorAll('.setCard'))for(const s of c._sheets || [])if(s.id===p.id)Object.assign(s,p.patch);
        }else for(const c of doc.querySelectorAll('.setCard'))if(c._laserSet?.setId===p.id)Object.assign(c._laserSet,p.patch);
      }
      addedSeals(r.added);
      cards(byId('libBody'),'current');cards(byId('libDone'),'done');
      // Let the wooden tool land, ink the new seal and lift before the completed set travels away.
      await Promise.all([...L.presses]);
      let landed = 0;
      if (done && r.setId) {
        if (r.setDone) landed=leave('set',r.setId,true,ids,r.setId);
        else { cards(byId('libBody'),'current'); partials(byId('libBody')); LaserReview.changed(); }
      } else landed=leave(kind,id,done,ids,r.setId);
      if (r.counts) {
        // what is on its way to the Completed tab is counted as it lands there, where the count pulses (Paul, 27 Sep)
        const was = L.counts ? { ...L.counts } : null;
        setCounts(r.counts);
        if (was && landed && done && L.tab !== 'done') for (const k of ['sheets', 'sets']) { const d = L.counts[k] - was[k]; if (d > 0) { L.counts[k] -= d; bump(k, d, false, landed); } }
      }
      for (const [k,st] of L.lists) if (st!==L.list || L.tab!=='done') dropList(k);
      if (!done && r.setId && r.setChanged && L.list && L.list.kind==='sets') removeRows(L.list,['set:'+r.setId]);
      if (!done) keepCurrent(r.sheetIds || ids);
      if (L.tab==='done' && done && L.list) { expectRows([key,r.setId && r.setDone?'set:'+r.setId:''],'current'); freshen(L.list,true); }
      if (done && r.setId && !r.setDone) {
        toast(`${name} completed · stays with its unfinished set`,'ok',5000);
      } else if (!o.undo) undoNote(kind,id,done,r,name,ids,Date.now()+landed);
      else said(done?`${name} is completed again`:`${name} is back in Laser cutting`,done?'done':'current',[key]);
      return r;
    } catch(e) { toast(`${done?'Not marked completed':'Not moved back'}: ${e.message}`,'bad',8000); throw e; }
    finally { L.busy.delete(key); }
  }
  /** "Sheet 2 marked completed · Undo", under the tab it went to, once it has landed there (Paul, 27 Sep 20:09-20:24:
      the tab says what arrived; one Undo, no toast beside it). With the Library out of sight, or under an open window,
      the toast says it as before. Undoing a set marks back only the sheets this mark changed. */
  function undoNote(kind, id, done, r, name, ids, landAt) {
    const setNote = kind === 'sheet' && r.setId && r.setChanged ? (r.setDone ? ' · its set is complete' : ' · its set is back in Laser cutting') : '';
    const text = done ? `${name} marked completed${setNote}` : `${name} returned to Laser cutting${setNote}`;
    const undo = () => {
      if (kind === 'set' && done) {
        // (the set's sheets as they were when it was marked: its card has left since)
        const all = ids && ids.length ? ids : setSheets(id), touched = r.sheetIds || [];
        if (!touched.length || (all.length && touched.length >= all.length)) return mark('set', id, false, { undo: true, name }).catch(() => {});
        return touched.reduce((p, x) => p.then(() => mark('sheet', x, false, { undo: true })), Promise.resolve()).catch(() => {});
      }
      return mark(kind, id, !done, { undo: true, name }).catch(() => {});   // (its card may have left: the name it had)
    };
    const tab = done ? 'done' : 'current', title = done ? 'Return it to Laser cutting' : 'Mark it completed again';
    const show = () => {
      const n = noteOn(tab, { text, actions: [{ label: 'Undo', fn: undo, title }], ms: 8000 });
      if (n) { n.dataset.ld = kind + ':' + id; return; }
      const el = toast(text, 'ok', 8000, 'ld-undo'); if (!el) return;
      let b = el.querySelector('.toastUndo');
      if (!b) { b = doc.createElement('button'); b.type = 'button'; b.className = 'toastUndo'; b.textContent = 'Undo'; el.querySelector('.c').before(b); }
      b.onclick = e => { e.stopPropagation(); if (el._go) el._go(); undo(); };
    };
    // (after the copy has come down on the tab and faded: the note answers what arrived)
    const wait = landAt - Date.now() + (landAt > Date.now() ? 260 : 0);
    if (wait > 0) setTimeout(show, wait); else show();
  }
  /** A note under a Library tab (the tab something went to, or its sign), or nothing when it cannot be seen there. */
  function noteOn(t, spec) {
    if (!window.Motion || S.mode !== 'library' || covered()) return null;
    const at = tabTarget(t); if (!onScreen(at)) return null;
    const n = Motion.note(at, spec); if (n && at.classList.contains('ldSign')) at._note = n;
    return n;
  }
  /** What an Undo did, said under the tab it went back to (a toast where the tab cannot be seen), but only when it was
      not seen come back: a card that flew into its place has said it already (asked once its flight is over). */
  function said(text, t, keys) {
    setTimeout(() => {
      if (keys.some(k => k && Date.now() - (L.flew.get(k) || 0) < 6000)) return;
      if (!noteOn(t, { text, ms: 3200 })) toast(text, 'ok', 3200, 'ld-undo');
    }, window.Motion ? Motion.T.fly + 150 : 0);
  }
  const flewIn = keys => { for (const k of keys) L.flew.set(k, Date.now()); while (L.flew.size > 200) L.flew.delete(L.flew.keys().next().value); };
  /** A sheet taken back that Current's list may not hold (it reads the newest 300): its record is kept for Current. */
  async function keepCurrent(ids) {
    if (!ids || !ids.length) return;
    try {
      const r = await api('charmNestLibrary', { op: 'laserStatus', sheetIds: ids.slice(0, 200) }, { quiet: true });
      for (const s of r.sheets || []) { LaserReview.record(s); if (!isFiled(s)) L.extra.set(s.id, { r: s, t: Date.now() }); }
      const body = byId('libBody'), shown = body && (r.sheets || []).every(s => isFiled(s) || body.querySelector(`.libCard[data-id="${CSS.escape(s.id)}"]`));
      // (drawn now: it comes in from Completed as it would have at once)
      if (L.tab !== 'done' && S.mode === 'library' && !shown) comeBack(body, (r.sheets || []).filter(s => !isFiled(s)).map(s => s.id), 'done');
    } catch (_) { /* Current reads it at its next refresh if it is among the newest */ }
  }

  /* ── cards leaving and coming back ──
     Paul, 27 Sep 20:09-20:24: nothing a click moves may just vanish or pop up. A card marked completed lifts and flies to
     the Completed tab, which counts it as it lands; a row moved back flies to the Current tab and its day folds after it;
     a card an Undo brings back flies in from the tab it was in. What stays glides into the room left (or out of the way),
     slowly enough to follow (charm-nest-motion.js). Under an open window (the sheet window marks too) nobody sees the
     list: it folds away there as it did. Nothing waits on a motion: the marks and the cloud go on at once. */
  const tabBtn = t => doc.querySelector(`#libTab button[data-t="${t === 'done' ? 'done' : 'current'}"]`);
  const covered = () => { try { return !!doc.querySelector('dialog:modal'); } catch (_) { return !!doc.querySelector('dialog[open]'); } };
  const moving = () => !!window.Motion && !Motion.reduced() && S.mode === 'library' && !covered();
  const onScreen = el => { if (!el || !el.isConnected) return false; const r = el.getBoundingClientRect(); return r.width > 0 && r.height > 0 && r.bottom > 0 && r.top < innerHeight && r.right > 0 && r.left < innerWidth; };
  /** In sight in the Library's scroller (a card scrolled away is not flown: its tab only answers). */
  const inView = el => { if (!onScreen(el)) return false; const r = el.getBoundingClientRect(), s = stage().getBoundingClientRect(); return r.bottom > s.top && r.top < s.bottom; };
  /** Where a tab is for what goes to it or comes from it: the tab, or, with the list scrolled past the bar that holds
      it, a small sign at the top of the list under where the tab is ("↑ Completed"). What goes there is seen go up to
      it, and its note stands under it; the sign leaves once that is over. */
  const signs = {};
  function tabTarget(t) {
    const tab = tabBtn(t); if (!tab) return null;
    if (onScreen(tab)) { const r = tab.getBoundingClientRect(), s = stage().getBoundingClientRect(); if (r.top + r.height / 2 > s.top && r.bottom < s.bottom) return tab; }
    let s = signs[t];
    if (!s || !s.isConnected) {
      s = signs[t] = doc.createElement('span'); s.className = 'ldSign'; s.setAttribute('aria-hidden', 'true'); s._until = 0;
      s.innerHTML = `<svg viewBox="0 0 16 16"><path d="M8 13V3.5M3.8 7.4L8 3.2l4.2 4.2" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round"/></svg>${t === 'done' ? 'Completed' : 'Current'}`;
      doc.body.appendChild(s); signOff(s);
    }
    const sr = stage().getBoundingClientRect(), r = tab.getBoundingClientRect();
    Object.assign(s.style, { left: Math.max(sr.left + 8, r.left) + 'px', top: sr.top + 10 + 'px' });
    s._until = Math.max(s._until, Date.now() + (window.Motion ? Motion.T.fly : 0) + 1800);
    return s;
  }
  function signOff(s) {
    const t = setInterval(() => {
      if (Date.now() < s._until || (s._note && s._note.isConnected)) return;
      clearInterval(t);
      const gone = () => { s.remove(); for (const k in signs) if (signs[k] === s) delete signs[k]; };
      if (reduced() || !s.animate) return gone();
      s.animate([{ opacity: 1 }, { opacity: 0, transform: 'translateY(-6px)' }], { duration: 320, easing: 'ease-in', fill: 'forwards' }).finished.then(gone, gone);
    }, 300);
  }
  /** A copy of `el` flies to the tab `to`; the time until it lands there (0: it could not be seen go, the tab only answers). */
  function flyOff(el, to, opts) {
    if (!inView(el) || !onScreen(to)) { Motion.arrive(to, opts || {}); return 0; }
    const g = Motion.ghost(el, el.getBoundingClientRect(), null, el), c = g._card || g;
    // (the copy as it looked at rest: no spinner on its button, no search glow)
    for (const n of [c, ...c.querySelectorAll('.busy, .ldHit, .ldIn')]) n.classList.remove('busy', 'ldHit', 'ldIn');
    Motion.fly(g, to, opts || {});
    return Math.round(Motion.T.fly * .86);
  }
  /** Folds a card or a row away where it stood. o.gone: its copy has already lifted off, so only its room closes, as
      slowly as the rest glides (o.ms); else it fades as it folds, quickly (a day heading after its last row, a card
      under an open window). */
  function collapse(el, then, o = {}) {
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
    const ms = o.ms || 230;
    const frames = o.gone ? [Object.assign({ opacity: 0 }, from), Object.assign({ opacity: 0 }, to)]
      : [Object.assign({ opacity: 1, transform: 'none' }, from), Object.assign({ opacity: 0, transform: 'scale(.96)', offset: .4 }, from), Object.assign({ opacity: 0, transform: 'scale(.96)' }, to)];
    const a = el.animate(frames, { duration: ms, easing: o.gone ? 'cubic-bezier(.3,.1,.2,1)' : 'ease-in-out', fill: 'forwards' });
    let over = false; const end = () => { if (over) return; over = true; el.remove(); if (then) then(); };
    a.onfinish = end; setTimeout(end, ms + 400);
  }
  /** The card or row of what was just marked leaves the tab it is in; one taken back comes into view again. The time
      until what left lands on its tab (0 when nothing was seen fly). */
  function leave(kind, id, done, ids, setOfSheet) {
    if (S.mode !== 'library') return 0;
    if (L.tab !== 'done') {
      const body = byId('libBody'); if (!body) return 0;
      if (!done) { comeBack(body, ids, 'done'); return 0; }
      const gone = [];
      if (kind === 'set') {
        for (const c of body.querySelectorAll('.setCard')) if (c._laserSet && c._laserSet.setId === id) gone.push(c);
        for (const c of body.querySelectorAll('.libCard[data-id]')) if (!c.closest('.setCard') && recordOf(c.dataset.id)?.setId===id) gone.push(c.closest('.librarySheet') || c);
      }
      else for (const c of body.querySelectorAll(`.libCard[data-id="${CSS.escape(id)}"]`)) {
        const item = c.closest('.librarySheet') || c, set = item.closest('.setCard');
        if (set && !isDone(set._laserSet)) { cards(body,'current'); partials(body); continue; }
        const left = set ? [...set.querySelectorAll('.librarySheet')].filter(x => x !== item && !gone.includes(x) && !x.dataset.leaving).length : 1;
        gone.push(left ? item : set);
      }
      return goCurrent(body, gone);
    }
    const st = L.list; if (!st) return 0;
    if (done) return 0;                                 // (an undo of a move back: freshen brings it in at the top)
    const keys = kind === 'set' ? ['set:' + id, ...ids.map(x => 'sheet:' + x)] : ['sheet:' + id];
    // a sheet moved back from an opened set takes its set out of Completed: the set is not complete any more
    if (kind === 'sheet') {
      const inSet = doc.querySelector(`#libDone .ldPanelIn .libCard[data-id="${CSS.escape(id)}"]`);
      const item = inSet && inSet.closest('.ldItem'); if (item && item.dataset.set) keys.push('set:' + item.dataset.set);
      if (setOfSheet) keys.push('set:' + setOfSheet);
    }
    return removeRows(st, keys, 'current');
  }
  /** Current: the cards marked completed fly to the Completed tab and the rest glide into their room. */
  function goCurrent(body, gone) {
    gone = [...new Set(gone)].filter(el => el && el.isConnected && !el.dataset.leaving);
    if (!gone.length) return 0;
    if (!moving()) { gone.forEach(el => collapse(el, () => { partials(body); LaserReview.changed(); emptyCurrent(body); })); partials(body); return 0; }
    const before = snapshot(body), to = tabTarget('done'); let landed = 0;
    for (const el of gone) { landed = Math.max(landed, flyOff(el, to)); el.dataset.leaving = '1'; }
    for (const el of gone) el.remove();
    partials(body); emptyCurrent(body); LaserReview.changed();
    // (after LaserReview's own frame has shown or hidden its sections: the glide starts from the page as it will be drawn)
    requestAnimationFrame(() => glideFrom(body, before));
    return landed;
  }
  /** Current is drawn again with these sheets back in it: they fly in from the tab `from`, the rest glide aside. */
  function comeBack(body, ids, from) {
    if (!body || !ids || !ids.length) return;
    L.flash = { ids: new Set(ids), from, before: moving() ? snapshot(body) : null, at: Date.now() };
    renderCurrent();
  }
  /** (decorate) The sheets an Undo brought back: from the tab they were in, into their place; the others glide. */
  function landBack(body, f) {
    const run = () => {
      if (f.before && Date.now() - f.at < 2500) glideFrom(body, f.before);
      const seen = new Set(); let from = null;
      for (const c of body.querySelectorAll('.libCard[data-id]')) {
        if (!f.ids.has(c.dataset.id)) continue;
        // a set card that was not here comes back whole; else the sheet into its set, or on its own
        const item = c.closest('.librarySheet') || c, set = item.closest('.setCard');
        const el = set && f.before && !f.before.has(gkey(set)) ? set : item;
        if (seen.has(el)) continue; seen.add(el);
        if (moving() && inView(el) && onScreen(from = from || tabTarget(f.from))) { Motion.flyIn(from, el); flewIn([...el.querySelectorAll('.libCard[data-id]')].map(x => 'sheet:' + x.dataset.id).concat(el._laserSet && el._laserSet.setId ? ['set:' + el._laserSet.setId] : [])); }
        else { c.classList.add('ldIn'); setTimeout(() => c.classList.remove('ldIn'), 400); }
      }
    };
    if (moving()) requestAnimationFrame(run); else run();
  }

  /* ── Current glides: what stays moves from where it stood to where it is now ──
     Current's two sections, its cards and the sheets in a set card are known by what they show (gkey), so a card drawn
     again under the same name glides rather than jumps. A card inside a set card that glides too moves only its own part
     of the way; a set card's height follows its sheets. */
  const GLIDE = '.laserSection, .laserAreaItems > .librarySheet, .laserAreaItems > .setCard, .sheetsRow > .librarySheet';
  function gkey(el) {
    if (el.classList.contains('laserSection')) return 'area:' + (el.dataset.laserArea || '');
    if (el.classList.contains('setCard')) { const s = el._laserSet; return s ? (s.setId ? 'set:' + s.setId : 'group:' + (s.key || s.name || '')) : ''; }
    const c = el.querySelector(':scope > .libCard[data-id]'); return c ? 'sheet:' + c.dataset.id : '';
  }
  function snapshot(body) {
    const m = new Map();
    for (const el of body.querySelectorAll(GLIDE)) {
      const k = gkey(el); if (!k || m.has(k) || el.dataset.leaving) continue;
      const r = el.getBoundingClientRect(); if (r.width || r.height) m.set(k, { x: r.left, y: r.top, h: r.height });
    }
    return m;
  }
  function glideFrom(body, before) {
    if (!before || !before.size || !moving() || !body.isConnected) return;
    const now = []; for (const el of body.querySelectorAll(GLIDE)) { const k = gkey(el); if (k && before.has(k)) now.push([k, el]); }
    // a glide still under way is let go first: each card is measured where it stands now
    for (const [, el] of now) if (el._glide) { el._glide.forEach(a => a.cancel()); el._glide = null; }
    const s = stage().getBoundingClientRect(), T = Motion.T.slide, ease = 'cubic-bezier(.3,.1,.2,1)', moved = new Map();
    for (const [k, el] of now) {
      const b = before.get(k), r = el.getBoundingClientRect(); if (!r.width && !r.height) continue;
      let p = el.parentElement; while (p && p !== body && !moved.has(p)) p = p.parentElement;
      const pd = p && p !== body ? moved.get(p) : { dx: 0, dy: 0 };
      const dx = b.x - r.left, dy = b.y - r.top, own = { dx: dx - pd.dx, dy: dy - pd.dy }, dh = b.h - r.height;
      if (Math.max(r.bottom, r.bottom + dy) < s.top || Math.min(r.top, r.top + dy) > s.bottom) continue;   // (out of sight)
      const anims = [];
      if (Math.abs(own.dx) >= 1 || Math.abs(own.dy) >= 1) { anims.push(el.animate([{ transform: `translate(${own.dx}px,${own.dy}px)` }, { transform: 'none' }], { duration: T, easing: ease, fill: 'backwards' })); moved.set(el, { dx, dy }); }
      if (el.classList.contains('setCard') && Math.abs(dh) >= 1) {
        const clip = dh < 0 ? 'hidden' : 'visible';     // (growing, what it makes room for is kept inside it)
        anims.push(el.animate([{ height: b.h + 'px', alignContent: 'start', overflow: clip }, { height: r.height + 'px', alignContent: 'start', overflow: clip }], { duration: T, easing: ease }));
      }
      if (anims.length) el._glide = anims;
    }
  }
  /** Current changed by `change` (it may wait for the cloud): what stays glides, once it is drawn. For a view drawn again
      in one piece (the Sets view after Undo set, charm-nest-bridge.js). */
  async function glide(body, change) {
    const before = moving() && body ? snapshot(body) : null, y = stage().scrollTop, t = Date.now();
    await change();
    // (the list scrolled meanwhile, or the answer was long in coming: it is shown as it is)
    if (!before || stage().scrollTop !== y || Date.now() - t > 8000) return;
    LaserReview.changed(); requestAnimationFrame(() => glideFrom(body, before));
  }
  /** Completed rows an Undo brings back: they fly in from the tab `from` when drawn (prepend). */
  function expectRows(keys, from) { for (const k of keys) if (k) L.coming.set(k, { from, until: Date.now() + 10000 }); }
  function emptyCurrent(body) {
    if (body.querySelector('.libCard')) return;
    const f = focus(); if (f) return decorate(body);
    if (!body.querySelector(':scope > .libEmpty')) body.insertAdjacentHTML('beforeend', '<div class="libEmpty ldIn">Nothing left here: every sheet is under Completed.</div>');
  }
  /** "2 of 5 sheets completed" on a set in Current whose laser work has begun. */
  function partials(root) {
    for (const card of root.querySelectorAll('.setCard')) {
      const head = card.querySelector(':scope > .sh'); if (!head) continue;
      const all = (card._sheets || []).filter(r => !r.archived), n = all.map(LaserReview.projected).filter(isDone).length;
      let tag = head.querySelector('.ldPartial');
      if (!n || n >= all.length) { if (tag) tag.remove(); continue; }
      if (!tag) { tag = doc.createElement('span'); tag.className = 'ldPartial'; (head.querySelector('.ldMarkSet') || head.querySelector('.nm')).after(tag); }
      tag.textContent = `${n} of ${all.length} sheets completed`;
    }
  }
  /** The check at each card's corner, and the set's own at its head: Mark completed in Current, Move back in Completed. */
  function addedSeals(events){for(const e of events || [])L.pendingSeals.add(e.kind+':'+e.id+':'+e.eventId);}
  /* ── The Library's seals (Paul, 7 Oct 2026 00:01 UTC: "there are too many seals visible here. There should only be one seal per each sheet and
     only visible when a given sheet has completed the laser cutting process, no interim seals no duplicates"). A DISPLAY rule only: every
     stamp (laserReady, laserDone, the legacy ones) stays in the sheet's and the set's record, nothing here writes or deletes one, and the
     order timeline keeps drawing them all. A sheet shows ONE seal, its LASER CUT (the latest, when Undo and a second completion left
     several), and only while it is completed. Never LASER READY (an approval), never a seal on a set (its header, its row), never one on a
     sheet that is not completed (a reopened one, a Rose Gold sheet with only a partial Cut Sheet press: roseCutAt writes no completion).
     One component draws it everywhere, Seal.sheetCut (charm-nest-motion.js: 72 px, zoom x1.8). Only data already on the page is read. */
  /** The stamp the sheet `r` shows: its latest laser completion while the sheet is completed (marked on this page, or by its record), else null.
   *  `completed`: the caller knows already that it is (a row of the Completed list). */
  function cutStamp(r,completed){
    if(!r || !(completed || isDone(r)))return null;
    let lead=null;
    for(const s of CharmNestReadiness.processStamps(r))if(s.how==='laserDone' && +s.at>1e9 && (!lead || +s.at>=+lead.at))lead=s;   // (a time before 2001 is the page's own "marked, time not read yet" placeholder, never a completion)
    return lead;
  }
  /** The one seal of the sheet `r` as HTML (a Completed row, the sheet window): "" when it shows none. */
  function processHtml(r,owner,completed){
    if(!window.Seal || !Seal.sheetCut || !String(owner || '').startsWith('sheet:'))return '';
    return Seal.sheetCut(cutStamp(r,completed),{owner});
  }
  /** Puts the sheet card's one seal in its footer, beside its counts (never over them), takes away what is no longer to be shown. */
  function processSeals(host,r,owner){
    if(!window.Seal || !host)return;
    const foot=host.querySelector(':scope > .m') || host,stamp=cutStamp(r);
    for(const old of host.querySelectorAll(':scope > .processSealRow, :scope > .m > .processSealRow'))old.remove();   // (the earlier drawing: every stamp in a row hung under the card)
    let row=foot.querySelector(':scope > .sheetCutRow'),shown=row && row.querySelector('.seal');
    if(!stamp){if(row)row.remove();return;}
    if(shown && shown.dataset.processSeal!==stamp.id){row.remove();row=shown=null;}   // (a newer completion: the older one is not kept beside it)
    const key=owner+':'+stamp.id,fresh=L.pendingSeals.has(key);
    if(!row){
      foot.insertAdjacentHTML('beforeend',Seal.sheetCut(stamp,{owner,pending:fresh}));
      row=foot.lastElementChild;shown=row.querySelector('.seal');
      const status=foot.querySelector(':scope > .sheetBackStatus');if(status)foot.insertBefore(row,status);   // (before the counter, so the counter keeps the card's right edge)
    }
    if(fresh){
      L.pendingSeals.delete(key);
      const p=Seal.press(shown).catch(()=>shown.classList.remove('pending'));L.presses.add(p);p.finally(()=>L.presses.delete(p));
    }
    Seal.fitGroups(row);
  }
  function cards(root, mode) {
    if (!root) return;
    const at = cardIndex(byId('libBody') || root);   // (canComplete looks a card up in the Library's list, whatever root is drawn)
    for (const c of root.querySelectorAll('.libCard[data-id]')) {
      const rec=recordOf(c.dataset.id) || {}, r=LaserReview.projected(rec), done=isDone(r);
      processSeals(c,rec,'sheet:'+c.dataset.id);   // (the sheet's own record: its stamps as stored, not the projection's placeholder time)
      let b=c.querySelector(':scope > .ldMark');
      const allowed=done || canComplete('sheet',c.dataset.id,at);
      if (!allowed) { if(b)b.remove(); continue; }
      if(!b){b=doc.createElement('button');b.type='button';b.className='ldMark';c.appendChild(b);}
      b.dataset.ld='sheet:'+c.dataset.id;b.dataset.done=done?'1':'0';b.innerHTML=done?ICON.undo:ICON.check;
      b.title=done?'Undo sheet completion':'Mark completed · the laser has cut this sheet';b.setAttribute('aria-label',done?'Undo sheet completion':'Mark this sheet completed');
    }
    for (const card of root.querySelectorAll('.setCard')) {
      const st=card._laserSet,head=card.querySelector(':scope > .sh');if(!head)continue;
      for(const old of card.querySelectorAll(':scope > .sh > .setProcessSeals, :scope > .sh > .processSealRow'))old.remove();   // (a set shows no seal: its sheets do)
      let b=head.querySelector(':scope > .ldMarkSet');
      const done=isDone(st),allowed=st?.setId && !st.standalone && !st.working && (done || canComplete('set',st.setId,at));
      if(!allowed){if(b)b.remove();continue;}
      if(!b){b=doc.createElement('button');b.type='button';b.className='ldMark ldMarkSet';(head.querySelector('.nm') || head.firstChild).after(b);}
      b.dataset.ld='set:'+st.setId;b.dataset.done=done?'1':'0';
      b.innerHTML=(done?ICON.undo:ICON.check)+`<span>${done?'Reopen set':'Mark set completed'}</span>`;
      b.title=done?'Return the set and its sheets to Laser cutting':'Confirm laser cutting is finished for every remaining sheet';
    }
    if (window.LibraryDnd) window.LibraryDnd.decorate(root);   // the grip (drag, or Move to…) on each card: charm-nest-library-dnd.js
    if (window.LibraryEngraving) window.LibraryEngraving.decorate(root);   // the small show/hide of each sheet's back-engraving shelf: charm-nest-library-engraving.js
  }
  function act(kind, id, done, btn) {
    if (btn) btn.classList.add('busy');
    return mark(kind, id, done).catch(e => console.warn('Library: not marked', e)).finally(() => { if (btn && btn.isConnected) btn.classList.remove('busy'); });
  }

  /* ── counts on the tab ── */
  function setCounts(c) { if (!c) return; L.counts = { sheets: +c.sheets || 0, sets: +c.sets || 0 }; L.countsAt = Date.now(); paintCount(); }
  function paintCount(pulse) {
    const n = byId('libDoneCount'); if (!n) return;
    // (what is still on its way to the tab is counted as it lands: bump)
    const kind = S.library.kind === 'sets' ? 'sets' : 'sheets', v = L.counts ? Math.max(0, L.counts[kind] - (L.flying[kind] || 0)) : null;
    n.textContent = v == null ? '' : v > 9999 ? Math.round(v / 1000) + 'k' : String(v);
    n.setAttribute('aria-label', v == null ? 'completed' : `${v} completed ${kind}`);
    if (pulse && !reduced()) { n.classList.remove('pulse'); void n.offsetWidth; n.classList.add('pulse'); }
  }
  /** The count moves by d: at once, or `after` ms later, when what was marked comes down on the Completed tab (it pulses
      and counts up then). Hands back what lets that wait go (a mark the cloud refused). */
  function bump(kind, d, quiet, after) {
    if (!L.counts) return () => {};
    L.counts[kind] = Math.max(0, (L.counts[kind] || 0) + d);
    if (!after) { paintCount(!quiet); return () => {}; }
    L.flying[kind] = (L.flying[kind] || 0) + d; paintCount();
    let over = false;
    const land = pulse => { if (over) return; over = true; clearTimeout(t); L.flying[kind] -= d; paintCount(pulse); };
    const t = setTimeout(() => land(!quiet), after);
    return () => land(false);
  }
  async function refreshCounts(force) {
    if (L.countsBusy || !S.cloud.ok || (!force && Date.now() - L.countsAt < FRESH)) return;
    L.countsBusy = true;
    try { setCounts((await api('charmNestLibrary', { op: 'laserDoneList', countOnly: true }, { quiet: true })).counts); }
    catch (_) { L.countsAt = Date.now(); }
    finally { L.countsBusy = false; }
  }

  /** A sheet or set was moved by drag and drop (charm-nest-library-dnd.js, LibraryFlow.commit wrote to the cloud): what this
      page had marked for `keys` ("sheet:<id>" | "set:<id>") is let go, Current is read again (it opens from a cache a
      minute old otherwise) and Completed takes its rows out and in again in place; the counts follow. */
  async function reload(keys) {
    for (const k of keys || []) L.marks.delete(k);
    if (S.library) S.library.loadedAt = 0;
    refreshCounts(true);
    if (S.mode !== 'library') { for (const k of [...L.lists.keys()]) dropList(k); return; }
    if (L.tab === 'done') {
      const st = L.list;
      for (const [k, x] of [...L.lists]) if (x !== st) dropList(k);
      if (st) { removeRows(st, (keys || []).filter(k => st.byKey.has(k))); await freshen(st, true); } else showDone(true);
      return;
    }
    for (const k of [...L.lists.keys()]) dropList(k);
    try { await loadLibrary(); } catch (_) { /* the list on screen stays */ }
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
        const cur = (res.sheets || []).filter(r => !isFiled(r) && metalOk(r, metal)), done = (res.rows || []).filter(r => isDone(r) && metalOk(r, metal));
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
    pass(() => { cards(body, 'current'); partials(body); });
    body.querySelectorAll(':scope > .ldFound').forEach(n => n.remove());
    const empty = body.querySelector(':scope > .libEmpty'); if (empty && !empty.textContent.trim()) empty.remove();
    const line = foundLine('current'); if (line) body.prepend(line);
    locate(body);
    if (L.flash) { const f = L.flash; L.flash = null; landBack(body, f); }
    paintCount(); refreshCounts(); syncSearching(); fitPlaceholder();
  }

  /* ── Completed: the list ── */
  const rowKey = r => r.kind === 'set' ? 'set:' + r.setId : 'sheet:' + r.id;
  function listKey() {
    const q = query(), n = lookup(), m = metalNow() || 'all';
    return `${S.library.kind === 'sets' ? 'sets' : 'sheets'}|${m}|${n ? '#' + n : typing() ? '' : foldText(q)}|${CNListActivity.key('library')}`;
  }
  function newList(key) {
    const [kind, metal, q] = key.split('|');
    const el = doc.createElement('div'); el.className = 'ldList';
    el.innerHTML = '<div class="ldRows"></div><div class="ldMore" aria-live="polite"></div>';
    return { key, kind, ...CNListActivity.state('library'), metal: metal === 'all' ? null : metal, q: q.startsWith('#') ? '' : q, find: q.startsWith('#') ? q.slice(1) : '', el, rowsEl: el.firstChild, moreEl: el.lastChild,
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
          st.pending = CNListActivity.select('library',[...sets, ...(res.rows || []).filter(r => st.kind !== 'sets' || !inSets.has(r.setId))].filter(r => metalOk(r, st.metal)));
        }
        if (st.dropped) return;
        append(st, st.pending.splice(0, CHUNK));
        st.end = !st.pending.length; st.at = st.at || Date.now();
      } else {
        const r = await api('charmNestLibrary', { op: 'laserDoneList', sort:'activity', direction:st.direction, range:st.range, kind: st.kind, limit: PAGE, cursor: st.next || undefined, metal: st.metal || undefined, q: st.q || undefined }, { quiet: true });
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
      const r = await api('charmNestLibrary', { op: 'laserDoneList', sort:'activity', direction:st.direction, range:st.range, kind: st.kind, limit: PAGE, metal: st.metal || undefined, q: st.q || undefined }, { quiet: true });
      if (st.dropped) return;
      if (r.counts) setCounts(r.counts);
      const fresh = (r.rows || []).filter(x => !st.byKey.has(rowKey(x)));
      const changed=(r.rows || []).some(x=>st.byKey.has(rowKey(x)) && CNListActivity.at(x)!==CNListActivity.at(st.byKey.get(rowKey(x))));
      if(changed || (st.direction==='asc' && fresh.length)){dropList(st.key);if(L.tab==='done')showDone();return;}
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
    } else if (st.loading) html = '<i class="spin" aria-hidden="true"></i> Loading more…';
    else if (st.error) html = `Could not load: ${esc(st.error.message)} <button type="button" class="btn ghost xs" data-ld-retry>Retry</button>`;
    else if (!st.rows.length && st.end) html = '';
    else if (st.rows.length >= MAX_ROWS) html = `The newest ${MAX_ROWS.toLocaleString()} are shown · search for an order, a listing or a date to find older ones`;
    else if (st.end) html = st.find ? '' : `${plural(st.rows.length, 'completed ' + what)}${st.q || st.metal ? ' shown' : ''} · that is all`;
    else if (st.empties >= 8) html = `Nothing more in the ${st.scanned.toLocaleString()} read <button type="button" class="btn ghost xs" data-ld-more>Keep looking</button>`;
    if (m.innerHTML !== html) m.innerHTML = html;
    const empty = st.rowsEl.querySelector(':scope > .libEmpty');
    if (!st.rows.length && st.end && !st.loading && !st.error) {
      const text = st.find || st.q ? '' : st.metal ? `No completed ${esc(metalOf({ metal: st.metal }).label || st.metal)} ${what}s yet.` : `Nothing is completed yet. <b>Mark a ${what} completed in Laser cutting with the check at its corner once the laser has cut it.</b>`;
      if (!empty && text) st.rowsEl.insertAdjacentHTML('beforeend', `<div class="libEmpty">${text}</div>`);
    } else if (empty) empty.remove();
  }
  /** Rows taken out of a Completed list. `t` (a Move back: 'current'): each seen flies to that tab while its room closes,
      as slowly as Current glides, and a day left empty folds after its last row. The time until they land (0: none). */
  function removeRows(st, keys, t) {
    const want = new Set(keys), fly = !!t && moving() && L.list === st, to = fly ? tabTarget(t) : null; let landed = 0;
    for (const k of want) st.byKey.delete(k);
    st.rows = st.rows.filter(r => !want.has(rowKey(r)));
    for (const it of st.rowsEl.querySelectorAll('.ldItem')) {
      const k = it.dataset.kind === 'set' ? 'set:' + it.dataset.set : 'sheet:' + it.dataset.id;
      if (!want.has(k) || it.dataset.leaving) continue;
      const day = it.closest('.ldDay');
      const after = () => { if (day && !day.querySelector('.ldItem')) collapse(day, () => more(st), { ms: fly ? 520 : 230 }); else if (day) countDay(day, st); more(st); paintFound(st); };
      const flew = fly ? flyOff(it, to) : 0; landed = Math.max(landed, flew);
      // (its copy on its way: only its room closes; one not seen go fades as its room closes, as slowly)
      collapse(it, after, flew ? { gone: true, ms: Motion.T.slide } : fly ? { ms: Motion.T.slide } : {});
      if (day && day.querySelector('.ldItem:not([data-leaving])')) countDay(day);   // (its day counts one less as the row lifts off; one left empty folds as it is)
    }
    return landed;
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
        + `<span class="ldNums"><span><b>${sheets.length}</b> ${sheets.length === 1 ? 'sheet' : 'sheets'}</span><span><b>${+r.orders || 0}</b> orders</span><span data-ld-pcs="${esc((Array.isArray(r.sheetIds) && r.sheetIds.length ? r.sheetIds : sheets.map(s => s.id || s.sheetId)).filter(Boolean).slice(0, 12).join(','))}"><b>${+r.pieces || 0}</b> pcs</span><span><b>${pct(r.fill)}</b> full</span></span>`
        + who(r) + `<button type="button" class="ldBack" data-ld-back="set:${esc(r.setId)}" title="Return the set and its sheets to Laser cutting">Reopen</button>${ICON.chev}`+'</div>'
        + '<div class="ldPanel"><div class="ldPanelIn"></div></div></div>';
    }
    const sn = !r.draft && r.setSeq ? r.setSeq : 0;
    return `<div class="ldItem" data-kind="sheet" data-id="${esc(r.id)}"><div class="ldLine" role="button" tabindex="0" data-m="${esc(r.metal || '')}" title="${esc(r.fileBase || r.id)}" aria-label="${esc(`${CODE[r.metal] || ''} Sheet ${r.sheetIndex || 1}${sn ? ', Set ' + sn : ''}: open`)}">`
      + thumb(r.preview)
      + `<span class="ldName">${swatch(metalOf(r), 0)}<b>Sheet ${esc(r.sheetIndex || 1)}</b>${sn ? `<span class="ldSet">Set ${esc(sn)}</span>` : ''}<span class="ldDate" title="Sheet day">${esc(dayShort(r.day))}</span></span>`
      + `<span class="ldNums"><span><b>${+r.orders || 0}</b> ${r.orders === 1 ? 'order' : 'orders'}</span><span data-ld-pcs="${esc(r.id)}"><b>${+r.pieces || 0}</b> pcs</span><span><b>${pct(r.fill)}</b> full</span></span>`
      + who(r) + `<button type="button" class="ldBack" data-ld-back="sheet:${esc(r.id)}" title="Return to Laser cutting">Reopen</button>${ICON.chev}`+processHtml(r,'sheet:'+r.id,true)+'</div></div>';
  }
  function dayHead(day, kind) {
    const t = realDay(Date.now()), y = realDay(Date.now() - 86400000), d = new Date(day + 'T12:00:00');
    const year = d.getFullYear() !== new Date().getFullYear() ? { year: 'numeric' } : {};
    const date = d.toLocaleDateString(undefined, Object.assign({ month: 'short', day: 'numeric' }, year));
    const title = day === t ? 'Today' : day === y ? 'Yesterday' : d.toLocaleDateString(undefined, { weekday: 'long' });
    return `<section class="ldDay" data-day="${esc(day)}"><h3 class="ldDayHead">${esc(title)}<span>${esc(date)}</span><i data-kind="${kind}"></i></h3><div class="ldDayRows"></div></section>`;
  }
  function countDay(sec) {
    const n = sec.querySelectorAll('.ldDayRows > .ldItem:not([data-leaving])').length, i = sec.querySelector('.ldDayHead i');
    if (i) i.textContent = plural(n, i.dataset.kind === 'set' ? 'set' : 'sheet');
  }
  /** Rows drawn in one piece per day: a page added below never draws the list again. */
  function append(st, list) {
    if (!list.length) return;
    st.rowsEl.querySelectorAll(':scope > .skel, :scope > .libEmpty').forEach(n => n.remove());
    const groups = []; for (const r of list) { st.rows.push(r); st.byKey.set(rowKey(r), r); const day = CNListActivity.day(CNListActivity.at(r)) || 'undated'; const g = groups[groups.length - 1]; if (g && g.day === day) g.rows.push(r); else groups.push({ day, rows: [r] }); }
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
    const added = [], fresh = new Set();
    for (const r of list.slice().reverse()) {
      st.rows.unshift(r); st.byKey.set(rowKey(r), r);
      const day = CNListActivity.day(CNListActivity.at(r)) || 'undated'; let sec = st.rowsEl.firstElementChild;
      if (!sec || sec.dataset.day !== day) { st.rowsEl.insertAdjacentHTML('afterbegin', dayHead(day, st.kind === 'sets' ? 'set' : 'sheet')); sec = st.rowsEl.firstElementChild; fresh.add(sec); }
      sec.lastElementChild.insertAdjacentHTML('afterbegin', rowHtml(r)); countDay(sec); added.push([r, sec.lastElementChild.firstElementChild, sec]);
    }
    // a row an Undo brought back flies in from the tab it was in while its room opens (its day's too, when new);
    // any other row new since the list was read comes in where it is
    const on = L.list === st && moving(), grown = new Set();
    for (const [r, it, sec] of added) {
      const k = rowKey(r), c = L.coming.get(k); if (c) L.coming.delete(k);
      const from = on && c && c.until > Date.now() && inView(it) ? tabTarget(c.from) : null;
      if (from && onScreen(from)) {
        Motion.flyIn(from, it); flewIn([k]);
        const room = fresh.has(sec) ? sec : it; if (!grown.has(room)) { grown.add(room); Motion.grow(room, { ms: Motion.T.slide }); }
      } else { it.classList.add('ldIn'); setTimeout(() => it.classList.remove('ldIn'), 400); }
    }
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
    return fillSet(item, r);
  }
  /** The set's card in its opened row: read, and put in (again, after its Undo set: in place, the list as it was). */
  async function fillSet(item, r) {
    const inner = item.querySelector('.ldPanelIn');
    try {
      const ids = (r.sheetIds && r.sheetIds.length ? r.sheetIds : (r.sheets || []).map(s => s.id)).slice(0, 300);
      const [st, got] = await Promise.all([api('charmNestLibrary', { op: 'laserStatus', sheetIds: ids, setIds: [r.setId] }, { quiet: true }), api('charmNestLibrary', { op: 'setGet', setId: r.setId }, { quiet: true })]);
      const sheets = (st.sheets || []).map(s => LaserReview.record(s)).sort((a, b) => String(a.metal || '').localeCompare(String(b.metal || '')) || (a.sheetIndex || 0) - (b.sheetIndex || 0));
      const set = Object.assign({ setId: r.setId, seq: r.seq, day: r.day, name: r.name, sheetIds: ids, materials: r.materials, status: r.status }, got.set || {}, { setId: r.setId });
      if (got.set && got.set.status) r.status = got.set.status;
      // Undo set (Paul, 27 Sep 20:09-20:24): the whole list was read again and drawn in one piece, the set closed under
      // the hand. Its row stays where it is and its card is read again in place; the lists not on screen are read anew.
      const card = Sets.libraryCard(set, sheets, sheets, { onUndo: () => { for (const [k, x] of [...L.lists]) if (x !== L.list) dropList(k); if (item.isConnected && item.classList.contains('open')) return fillSet(item, r); } });
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
    for (const [k, c] of L.coming) if (c.until < t) L.coming.delete(k);
    for (const [k, at] of L.flew) if (t - at > 60000) L.flew.delete(k);
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
    // pieces and pairs (Paul, 9 Oct 2026): hover the piece count of a sheet card or a Completed row for "24 pieces, 10 pairs, 3 half pairs"
    body.addEventListener('mouseover', onCountHover, { passive: true }); done.addEventListener('mouseover', onCountHover, { passive: true });
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

  /* A card's piece count says what it counts when a pair is on it: "24 pieces, 10 pairs, 3 half pairs" (a half pair: one ear here, its other piece on another sheet), read from OrderPieces by
     PiecePlacement.sheetWords the moment the count is hovered, so it is never older than the sheet. A card with no pair keeps its look and no title; a Completed sheet whose orders were never
     read on this page is read once (OrderPieces.loadSheet) and the words come as that lands. */
  const readOnce = new Set();
  function onCountHover(e) {
    const t = e.target && e.target.closest ? e.target.closest('[data-sheet-count], [data-ld-pcs]') : null; if (!t) return;
    const PP = window.PiecePlacement, OP = window.OrderPieces; if (!PP || typeof PP.sheetWords !== 'function') return;
    const ids = (t.dataset.sheetCount ? [t.dataset.sheetCount] : String(t.dataset.ldPcs || '').split(',')).filter(Boolean), nums = (String(t.textContent).match(/\d+/g) || []).map(Number);
    const words = () => { const w = PP.sheetWords(ids, nums); if (w) t.title = w; else t.removeAttribute('title'); return w; };
    if (words() || !OP || typeof OP.loadSheet !== 'function') return;
    const fresh = ids.filter(id => !readOnce.has(id)).slice(0, 6); if (!fresh.length) return;
    for (const id of fresh) readOnce.add(id);
    Promise.all(fresh.map(id => Promise.resolve().then(() => OP.loadSheet(id)).catch(() => {}))).then(words, () => {});
  }

  window.LibraryDone = { mark, isDone, isFiled, cutStamp, canComplete, addedSeals, recordOf, nameOf, setSheets, refreshCards: root => pass(() => { cards(root, L.tab); partials(root); }), tab: () => L.tab, setTab, show, focus, rows, decorate, input, fromHash, glide, snapshot, glideFrom, counts: () => L.counts && Object.assign({}, L.counts) };
  window.LibraryDone.reload = reload;
  if (doc.readyState === 'loading') doc.addEventListener('DOMContentLoaded', init); else init();
})();
