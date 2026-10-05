/* Charm Nest · the placement feed (window.PlacementFeed). Paul, 5 Oct 2026: "everything needs to be coordinated as information is added, changed,
   removed, altered. Everything needs to be seamlessly updated in the cloud and on all portions of the application so that there's no mismatch".

   Why it exists. Where a piece is (on a sheet, which one, held, cancelled, completed by hand) is ONE answer in the cloud (the sheet records, the pool
   rows, the cancel records, the custom records) and ONE answer on the page (OrderPieces -> PiecePlacement). What was missing is the cadence: a page
   read the sheet records of an order once (and again after 30 s, when something asked), so a Hold, a take-off, a sheet deleted or an order put on a
   sheet on ANOTHER computer reached the lists, the cards, the search and the windows only at the next reload or the next tab change. The Library has
   its own live read (about every 3 s while it is on screen), the Review tab has its own (every 2 s), the order window its timeline feed; every other
   surface had none, so each said whatever it had last been told, and they said different things.

   What it does. While the browser tab is visible and the cloud answers, every EVERY (2.5 s) it reads again, for the orders that are on screen
   (found by what the surfaces draw: a data-rid on a row, a card or a chip, the open order window, and any surface that says so with watch()), the
   sheet records and pool rows of those orders (OrderPieces.load, one getOrderPieces for up to 30 orders; the server answers `unchanged` from the
   documents' update times alone when it can, C3's ifRev) and the cancelled orders (Cancelled.load: only the records written since the last read).
   What came back is told to everything that listens (OrderPieces.subscribe: PiecePlacement and every surface that reads it) only when it differs from
   what the page held, so a quiet cloud redraws nothing.
   A hold or a release made on another computer is also taken into this page's own rows (adoptHolds below), so that this page's Hold button, its
   Release hold button and its On hold pile say what the cloud says.

   Never faster than GAP after a read; backs off after failures (x2 up to 30 s) and says nothing; nothing is read while the tab is hidden or nothing is
   on screen to say; nothing is written; no Etsy call, ever; a press that changes where a piece is nudges it (nudge(): a read now, and again 1.5 s later).

     PlacementFeed.watch(token, ids | () => ids)   a surface says which orders it shows (a list of receipt ids, or a function that gives them); returns stop()
     PlacementFeed.nudge()                         read now (a person's own change, a cloud change heard of elsewhere)
     PlacementFeed.watched()                       the receipt ids read at the next tick (the checks read it)
     PlacementFeed.state()                         { busy, fails, ticks, last, every, orders }
     PlacementFeed.adoptHolds(ids?)                take the cloud's holds into this page's rows now (the feed does it after every read)                   */
(function (root) {
  'use strict';
  const doc = root.document;
  if (!doc || root.PlacementFeed) return;
  const EVERY = 2500, GAP = 600, SECOND = 1500, FAIL_MAX = 30000, MAX_ORDERS = 48, NEAR = 700;
  const st = { timer: 0, second: 0, busy: false, again: false, fails: 0, start: 0, end: 0, ticks: 0, last: '', lastOk: 0, sig: '', cancelSig: '', dead: false };
  const watchers = new Map();   // token -> { ids: () => Iterable, at }
  const safe = (f, d) => { try { const v = f(); return v === undefined ? d : v; } catch (_) { return d; } };
  const ridOf = x => String(x == null ? '' : x).replace(/\D/g, '');
  const OK = /^\d{4,20}$/;
  const cloudOk = () => safe(() => !!(root.CN && root.CN.S && root.CN.S.cloud && root.CN.S.cloud.ok), false);
  const live = () => !st.dead && !doc.hidden && cloudOk();

  /** The orders on screen, nearest the top of the screen first. */
  function onScreen() {
    const out = new Map();
    const add = (rid, y) => { rid = ridOf(rid); if (OK.test(rid) && (!out.has(rid) || y < out.get(rid))) out.set(rid, y); };
    const vh = root.innerHeight || 900;
    for (const n of doc.querySelectorAll('[data-rid]')) {
      if (n.closest('[hidden]') || n.closest('dialog:not([open])')) continue;
      const r = n.getBoundingClientRect(); if (!(r.width > 0 && r.height > 0) || r.bottom < -NEAR || r.top > vh + NEAR) continue;
      add(n.dataset.rid, Math.abs(r.top));
    }
    // the open order window (it holds its order by the row's key: receipt_transaction)
    if (root.OrderWin && root.OrderWin.isOpen && safe(() => root.OrderWin.isOpen(), false)) { const k = safe(() => root.OrderWin.key(), ''); add(String(k).split('_')[0], -1); }
    for (const w of watchers.values()) for (const id of safe(() => [...(typeof w.ids === 'function' ? w.ids() : w.ids)], [])) add(id, -0.5);
    // the Library's sheet cards on screen: the orders on those sheets (its lists and chips say where their pieces are)
    if (root.LaserReview && root.LaserReview.shownOrders) for (const id of safe(() => root.LaserReview.shownOrders(), [])) add(id, 3000);
    return [...out].sort((a, b) => a[1] - b[1]).slice(0, MAX_ORDERS).map(x => x[0]);
  }
  const watched = () => safe(onScreen, []);
  function watch(token, ids) {
    const key = String(token || 'w' + watchers.size);
    watchers.set(key, { ids: typeof ids === 'function' ? ids : () => ids || [], at: Date.now() });
    schedule();
    return () => { watchers.delete(key); };
  }

  /* ── a hold or a release made on another computer, taken into this page's own rows ──
     A page that did not press Hold has rows that never heard of it (row.hold is a page's own flag): its Hold button was still offered, its pile did not
     list the order and its Release button was not there. When the cloud says a row's pieces are on hold and the row says nothing, the row takes the
     hold (the very fields the Hold engine sets: state, hold, reason, heldAt); when the cloud no longer says so and the hold was one the cloud had said
     (this page saw it there), the row lets it go. A hold this page has just set and the cloud has not confirmed yet is left alone, as is any row
     while a hold or release is under way here. Never writes anything. */
  // (a hold, a release or a cancel of this order is under way on this page: its own flow sets and clears the row's flags, and the cloud has not caught up yet)
  const flowBusy = rid => safe(() => !!((root.OrderHold && root.OrderHold.status && (root.OrderHold.status(rid) || {}).running)
    || (root.OrderHold && root.OrderHold.releaseStatus && (root.OrderHold.releaseStatus(rid) || {}).running)
    || (root.HoldUI && root.HoldUI.busy && root.HoldUI.busy(rid)) || (root.CancelUI && root.CancelUI.busy && root.CancelUI.busy(rid))), false);
  function adoptHolds(ids) {
    const PP = root.PiecePlacement, Orders = root.Orders;
    if (!PP || !Orders || !Orders.rows) return 0;
    const want = ids ? new Set(ids.map(ridOf)) : null;
    let changed = 0;
    for (const r of safe(() => Orders.rows(), [])) {
      if (!r || !r.order || r.state === 'gone') continue;
      const rid = ridOf(r.order.receiptId); if ((want && !want.has(rid)) || flowBusy(rid)) continue;
      // p: what this page says (the row's own hold counts until the cloud has confirmed it); c: what the cloud alone says (pool rows taken off for a hold, no row flag)
      const p = safe(() => PP.ofRow(r), null); if (!p || p.state === 'loading' || p.state === 'cancelled') continue;
      const c = safe(() => PP.ofRow(r, { cloudOnly: true }), null) || p;
      if (c.state === 'loading') continue;
      if (c.state === 'hold' && !r.hold) {
        r.state = 'held'; r.hold = r.reason = c.why ? c.why.replace(/^it is on hold:?\s*/i, '') || 'On hold' : 'On hold'; r.heldAt = c.since || Date.now(); r.holdSeen = true; r.holdFromCloud = true; changed++;
      } else if (c.state === 'hold') r.holdSeen = true;   // (the cloud now keeps the hold this page pressed: from here on it is the cloud's to release; the page's answer does not change)
      else if (r.holdSeen) {   // (the cloud had it on hold and no longer does: released, or taken off the pile elsewhere)
        const had = !!r.hold || r.state === 'held';
        r.hold = null; r.holdSeen = false; r.holdFromCloud = false; if (r.state === 'held') r.state = c.state === 'sheet' ? 'nested' : 'pooled'; r.reason = null; if (had) changed++;
      }
    }
    if (changed) {
      safe(() => Orders.render && Orders.render());
      safe(() => { if (root.OrderWin && root.OrderWin.isOpen && root.OrderWin.isOpen() && root.OrderWin.paint) root.OrderWin.paint(); });   // (the open order window's rows and chips read the row's hold too)
      safe(() => root.OrderPieces && root.OrderPieces.notify && root.OrderPieces.notify());   // (everything that listens reads the rows again: the Hold buttons, Review's cards, the search, the pile)
      safe(() => { if (root.LaserReview && root.LaserReview.changed && root.CN && root.CN.S && root.CN.S.mode === 'library') root.LaserReview.changed(); });   // (the Library's order check and lists read the rows' holds)
    }
    return changed;
  }

  /* ── the cadence ── */
  const arm = ms => { clearTimeout(st.timer); st.timer = setTimeout(tick, Math.max(0, ms)); };
  function schedule() {
    if (st.timer || st.busy || !live()) return;
    const every = Math.min(FAIL_MAX, EVERY * 2 ** Math.min(st.fails, 4));
    arm(Math.max(st.start + every, st.end + GAP) - Date.now());
  }
  const cancelSig = rids => safe(() => rids.map(r => (root.Cancelled && root.Cancelled.has && root.Cancelled.has(r) ? 1 : 0)).join(''), '');
  async function tick() {
    st.timer = 0;
    if (!live()) return;
    const rids = watched();
    st.start = Date.now();
    if (!rids.length) { st.end = st.start; return schedule(); }
    if (st.busy) { st.again = true; return; }
    st.busy = true; st.ticks++;
    const OP = root.OrderPieces, C = root.Cancelled;
    try {
      const jobs = [];
      if (OP && OP.load) jobs.push(OP.load(rids, { force: true }));
      if (C && C.load) jobs.push(Promise.resolve(C.load(true)).catch(() => null));
      await Promise.all(jobs);
      st.fails = 0; st.lastOk = Date.now(); st.last = rids.join(',');
      const cs = cancelSig(rids);
      if (cs !== st.cancelSig) { const first = st.cancelSig === ''; st.cancelSig = cs; if (!first && OP && OP.notify) OP.notify(); }
      adoptHolds(rids);
    } catch (e) { if (++st.fails === 1) safe(() => console.warn('Placement feed', e)); }
    finally {
      st.busy = false; st.end = Date.now();
      if (st.again) { st.again = false; arm(0); } else schedule();
    }
  }
  /** A person's own change (or news of one): read now, and again 1.5 s later; never twice within GAP. */
  function nudge() {
    if (!live()) return false;
    arm(Math.max(0, st.start + GAP - Date.now()));
    clearTimeout(st.second); st.second = setTimeout(() => { st.second = 0; if (!st.busy) arm(0); else st.again = true; }, SECOND);
    return true;
  }
  // a tab change shows other orders: read for them now (the feed finds them by what is drawn, a moment after the tab has drawn)
  doc.addEventListener('click', ev => { const t = ev.target; if (t && t.closest && t.closest('#modeSeg')) setTimeout(nudge, 350); }, true);
  doc.addEventListener('visibilitychange', () => { if (doc.hidden) { clearTimeout(st.timer); clearTimeout(st.second); st.timer = st.second = 0; } else { st.fails = 0; arm(0); } });
  root.addEventListener && root.addEventListener('online', () => { st.fails = 0; arm(0); });
  // anything the page learns of the sheets (a load made for a window, the Library's list) is adopted into the rows too
  const hook = () => { const OP = root.OrderPieces; if (!OP || !OP.subscribe || hook.done) return; hook.done = true; let queued = false; OP.subscribe(() => { if (queued) return; queued = true; Promise.resolve().then(() => { queued = false; adoptHolds(); safe(() => { if (root.CN && root.CN.S && root.CN.S.mode === 'library' && root.LaserReview && root.LaserReview.nudge) root.LaserReview.nudge(); }); }); }); };   // (a piece that moved on the cloud: the Library's own read of the sheets follows at once, not at its next beat; a quiet cloud bumps nothing)
  const start = () => { hook(); schedule(); };
  if (doc.readyState === 'loading') doc.addEventListener('DOMContentLoaded', () => setTimeout(start, 0)); else setTimeout(start, 0);
  // (the page's mode and the windows change what is on screen: a look every few seconds is enough to start the loop when there was nothing to read)
  setInterval(() => { hook(); if (!st.timer && !st.busy) schedule(); safe(() => root.ReviewLive && root.ReviewLive.ensure && root.ReviewLive.ensure()); }, EVERY);   // (the custom records feed, for the search box or the order window opened over a tab that does not follow them)
  root.PlacementFeed = { watch, nudge, watched, adoptHolds, state: () => ({ busy: st.busy, fails: st.fails, ticks: st.ticks, last: st.last, every: EVERY, orders: watched().length, lastOk: st.lastOk }), _tick: tick };
})(typeof self !== 'undefined' ? self : this);
