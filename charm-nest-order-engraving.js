/* The back engraving of ONE piece of an order, drawn wherever an order is shown (Paul, 5 Oct 2026: "incorporate the full back
 * engraving view ... into the detail order modal underneath the existing thumbnail image ... the full functionality, including the
 * Approved button/Seal and also the shortcut to the back engraving tab modal which would open the actual engraving details modal
 * for that specific order number and piece").
 *
 * It is not a second copy of the Sheet tab's card: it draws the very same card (CNEngravingSeals.panel + wirePanel, the same
 * classes, the same preview, the same words box, the same Approved button with the BACK ENGRAVING seal pressed on it through
 * CNEngravingSeals.press inside Engrave.approve, the same "View in Engrave" button), only for a piece found by its order, its
 * line and its pool id instead of by a sheet.
 *
 *   OrderEngraving.mount(host, { rid, key, poolId, piece, row?, events?, changed?, sheetEng? }) -> { update(ctx), destroy(), refresh(), el }
 *       rid     the order (receipt id)           key    the order line's key (what Engrave keeps its job under)
 *       poolId  the piece's pool id (optional: the line's first)       piece  the piece picked on the order's switcher (kept for EngraveLink)
 *       row     the order line itself, for an order that is not in the pull (the order window builds it from the records)
 *       events  the order's timeline events (an array, or a function giving them): an approval another computer makes shows
 *               here from them, within the timeline feed's own 2.5 s read; nothing is read for it
 *       changed called when something other than this card's own press changed the engraving (a person's approval elsewhere),
 *               and after this card's own approval: the host repaints what depends on it (the red "still to be settled" box, the
 *               Engraving cell, the Sheet tab)
 *       sheetEng a function giving the Sheet tab's own reading of this piece (used only when Engrave holds no job for it)
 *     update(ctx)   told again (the order window paints often): the card is drawn again only when what it shows changed; a
 *                   different order, line or piece swaps the card at once (nothing of the piece before stays on screen)
 *     destroy()     the card goes, its timers with it
 *   OrderEngraving.engOf(ctx) -> { row, job, eng, poolId, loading }   what the card would show, as data (no drawing)
 *   OrderEngraving.unmount(host)
 *
 * No back engraving ("none") draws nothing and the host is hidden. Every wait says what it waits for (a small spinner and words).
 * Live without a request of its own: Engrave's jobs are in this page, so a change made in the Engraving tab, the Sheet tab or the sheet
 * window is seen within a second by comparing them (the sheet window does the same every 1.5 s); the timeline feed the order window
 * already reads says what another computer did. Seals are never touched here: Engrave.approve adds the new one, the card shows all
 * of them, and the card is never drawn again while a seal is being pressed.
 */
(function (root) {
  'use strict';
  const doc = () => root.document;
  const tryDo = (f, d) => { try { return f(); } catch (e) { try { console.warn('order engraving:', e); } catch (_) {} return d; } };
  const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const DONE = ['approved', 'written'];
  const cors = u => (root.CharmNestAssets && root.CharmNestAssets.url ? tryDo(() => root.CharmNestAssets.url(u), u) : u);
  const say = (msg, kind, ms) => { try { if (typeof root.toast === 'function') root.toast(msg, kind || '', ms || 3600); } catch (_) {} };

  /* ── the card's own few rules (the card itself is styled by the Sheet tab's rules, charm-nest-activity.css) ── */
  function css() {
    const d = doc(); if (!d || d.getElementById('owEngCss')) return;
    const s = d.createElement('style'); s.id = 'owEngCss';
    s.textContent = '.owEng{display:grid;gap:6px;min-width:0;margin-top:6px}.owEng[hidden]{display:none}.owEng>.owEngCard{display:grid;gap:6px;min-width:0}.owEng .fLabel{margin:0}'
      + '.owEng .swEng .pv{position:relative}.owEngWait{display:flex;align-items:center;gap:8px;min-width:0;padding:9px 12px;border:1px solid var(--line);border-radius:12px;background:var(--card);font:11px var(--sans);color:var(--ink45)}'
      + '.owEngPvWait{position:absolute;inset:0;display:flex;align-items:center;justify-content:center;gap:7px;background:#fff;font:11px var(--sans);color:var(--ink45)}'
      + '.owEng .by.owEngBy{display:flex;align-items:center;gap:7px}.owEng .by.owEngBy .owSpin{width:11px;height:11px;flex-basis:11px}'
      // the Overview's column is narrow (200 px, the Sheet tab's panel is ~340): the seal still rests half over the button at its full size,
      // so the button is a little shorter (the rule that places the seal names the button's width) and the seals' own padding goes
      + '.owEng .swEng .btn.egApproveButton{min-width:136px}.owEng .swEng .egApproveWrap>.egButtonSeal{left:calc(136px - var(--seal-fit,var(--seal-base,50px)) / 2 + 8px);padding:0;gap:6px}';
    (d.head || d.documentElement).appendChild(s);
  }
  const waitNode = text => { const n = doc().createElement('div'); n.className = 'owEngWait'; n.setAttribute('role', 'status'); n.setAttribute('aria-live', 'polite'); n.innerHTML = `<span class="owSpin" aria-hidden="true"></span><span>${esc(text)}</span>`; return n; };

  /* ── what a piece's back engraving is, as the card shows it ── */
  const ordersRows = () => tryDo(() => (root.Orders && root.Orders.rows && root.Orders.rows()) || [], []);
  function rowFor(ctx) {
    if (ctx.row) return ctx.row;
    const rows = ordersRows();
    return rows.find(r => r.key === ctx.key) || (ctx.poolId && rows.find(r => (r.poolIds || []).includes(ctx.poolId))) || null;
  }
  function jobFor(row, poolId) {
    const E = root.Engrave; if (!E || !E.items) return null;
    const hit = row && E.jobOf ? tryDo(() => E.jobOf(row), null) : null; if (hit) return hit;
    if (!poolId) return null;
    for (const j of E.items().values()) if (!j.editingBack && (j.copies || []).includes(poolId)) return j;
    return null;
  }
  /** The back records Engrave holds for a job (the written ones carry the saved picture): this piece's, the newest. */
  function backOf(job, poolId) {
    const all = (job.backs || []).filter(Boolean), mine = poolId ? all.filter(b => b.poolId === poolId) : [];
    const list = mine.length ? mine : all;
    return list.reduce((a, b) => (!a || (+b.approvedAt || 0) >= (+a.approvedAt || 0) ? b : a), null);
  }
  /** One job read the way the Sheet tab and the sheet window read it (the same kinds, the same words). */
  function fromJob(job, saved, poolId) {
    const E = root.Engrave, s = job.state;
    const base = { job, saved: saved || undefined };
    if (s === 'none') return Object.assign(base, { kind: 'none' });
    if (DONE.includes(s)) return Object.assign(base, { kind: 'approved', back: backOf(job, poolId) || undefined, by: job.approvedBy, at: job.approvedAt, text: job.text });
    if (s === 'skipped') return Object.assign(base, { kind: 'skipped', by: job.approvedBy, text: job.text });
    if (s === 'review') return Object.assign(base, { kind: 'approve', text: job.text });
    if (s === 'words' || s === 'blocked') return Object.assign(base, { kind: 'words', text: job.text, reason: job.reason });
    const working = !!(E && E.isWorking && tryDo(() => E.isWorking(job), false));
    const note = working ? (s === 'classify' ? 'Reading the words…' : 'Fitting the words on the back…')
      : s === 'classify' ? 'Its words are not read yet: confirm them in Engrave'
      : E && E.canFit && !tryDo(() => E.canFit(job), true) ? 'Waits for its sheet to be written, then it is fitted' : 'Waits its turn in Engrave';
    return Object.assign(base, { kind: 'preparing', text: job.text, working, note });
  }
  function fromSaved(re) {
    if (re.needed) return { kind: re.approved ? 'approved' : 'words', text: re.text, by: re.approvedBy, at: re.approvedAt, saved: re };
    return { kind: re.state === 'skipped' ? 'skipped' : 'none', text: re.text, saved: re };
  }
  /** An approval of this line the timeline holds that is newer than anything this card could have seen: another computer's. */
  function foreignApproval(row, job, events, seenAt) {
    const S = root.CNEngravingSeals; if (!S || !row || !Array.isArray(events) || !events.length) return null;
    if (job && (DONE.includes(job.state) || job.state === 'skipped' || job.stamping || job.approvalPreparing || job.approvalIntent)) return null;
    const seals = tryDo(() => S.fromEvents(events, row), []).filter(x => x.how === 'engraveApproved' && x.by && +x.at > 0);
    const newest = seals.reduce((a, b) => (!a || b.at > a.at ? b : a), null);
    if (!newest || newest.at <= Math.max(+(job && job.approvedAt) || 0, (seenAt || 0) - 2000)) return null;
    const ev = events.find(e => e.type === 'engraveApproved' && +e.at === newest.at);
    return { by: newest.by, at: newest.at, text: ev && ev.data && ev.data.text ? String(ev.data.text) : '' };
  }
  function engOf(ctx, seenAt) {
    const row = rowFor(ctx), poolId = ctx.poolId || (row && (row.poolIds || [])[0]) || '';
    if (row && row.loading) return { row, poolId, loading: true, job: null, eng: null };
    if (!root.Engrave || !root.CNEngravingSeals) return { row, poolId, job: null, eng: { kind: 'none' } };
    const job = jobFor(row, poolId), saved = row && row.engrave || null;
    let eng;
    if (job) eng = fromJob(job, saved, poolId);
    else {
      const alt = typeof ctx.sheetEng === 'function' ? tryDo(() => ctx.sheetEng(), null) : null;
      eng = alt && alt.kind && alt.kind !== 'none' ? Object.assign({}, alt, { saved: saved || alt.saved }) : saved ? fromSaved(saved) : { kind: 'none' };
    }
    const events = typeof ctx.events === 'function' ? tryDo(() => ctx.events(), null) : ctx.events;
    const f = ['approve', 'words', 'preparing', 'none'].includes(eng.kind) ? foreignApproval(row, job, events, seenAt) : null;
    if (f) eng = Object.assign({}, eng, { kind: 'approved', by: f.by, at: f.at, text: eng.text || f.text, note: undefined, working: false, foreign: true });
    return { row, poolId, job, eng };
  }

  /* what the card shows, in a line: when it differs from the one drawn, the card is drawn again */
  const ids = new WeakMap(); let idN = 0;
  const idOf = o => { if (!o || typeof o !== 'object') return 0; let n = ids.get(o); if (!n) ids.set(o, n = ++idN); return n; };
  function sigOf(r) {
    if (r.loading) return 'loading';
    const e = r.eng, j = r.job, S = root.CNEngravingSeals;
    const seals = j && S ? tryDo(() => S.list(j).map(s => s.id).join(','), '') : e.saved && S ? tryDo(() => S.list({ seals: e.saved.seals || [], approvedAt: e.saved.approvedAt, approvedBy: e.saved.approvedBy, state: e.saved.state }).map(s => s.id).join(','), '') : '';
    return JSON.stringify([e.kind, e.text, e.note, !!e.working, e.by, +e.at || 0, !!e.foreign, j && j.state, j && +j.approvedAt || 0, seals, j && idOf(j.fit), j && idOf(j.view),
      e.back && (e.back.approvedAt || '') + (e.back.png || (e.back.outputs && e.back.outputs.png && e.back.outputs.png.url) || e.back.preview || '')]);
  }

  /* ── the card on a page ── */
  const live = new Set(); let timer = 0, nextId = 0;
  const hidden = () => { try { return doc().hidden; } catch (_) { return false; } };
  function tick() { if (hidden()) return; for (const m of [...live]) tryDo(() => m._tick()); }
  function arm() { if (!timer && live.size) timer = root.setInterval(tick, 1000); }
  function disarm() { if (timer && !live.size) { root.clearInterval(timer); timer = 0; } }
  if (root.document && root.document.addEventListener) root.document.addEventListener('visibilitychange', () => { if (!hidden()) tick(); });

  function mount(host, ctx0) {
    css();
    if (!host) return null;
    if (host._orderEngraving) tryDo(() => host._orderEngraving.destroy());
    const id = 'order-engraving:' + (++nextId);
    let ctx = Object.assign({}, ctx0), gone = false, sig = null, shown = null, busy = 0, seenAt = Date.now(), last = null, ro = null, attached = false;
    host.classList.add('owEng');
    // (the piece picked on the switcher is only told to EngraveLink: the same line on "All pieces" or on its own is the same card)
    const ident = c => [c.rid, c.key, c.poolId || ''].join('|');
    // (a card we hid ourselves has no box: it is seen through its parent, so a back that becomes real later is still drawn)
    const visible = () => { const n = host.hidden ? host.parentElement : host; return !!(n && host.isConnected && n.getClientRects().length); };
    const target = r => ({ rid: String(ctx.rid || (r.row && r.row.order && r.row.order.receiptId) || ''), key: ctx.key || (r.row && r.row.key) || '', poolId: r.poolId || '', piece: ctx.piece || ctx.key || (r.row && r.row.key) || '' });
    const poke = () => { tryDo(() => root.RunCtl && root.RunCtl.poke && root.RunCtl.poke()); tryDo(() => root.OrderWin && root.OrderWin.nudge && root.OrderWin.nudge()); };
    const told = r => { if (typeof ctx.changed === 'function') tryDo(() => ctx.changed(r)); };

    function render(r) {
      const S = root.CNEngravingSeals;
      if (r.loading) { host.hidden = false; host.replaceChildren(waitNode('Reading the back engraving…')); return; }
      const eng = r.eng;
      if (!eng || eng.kind === 'none') { host.hidden = true; host.replaceChildren(); return; }
      host.hidden = false;
      const card = doc().createElement('div'); card.className = 'owEngCard'; card.dataset.orderEngraving = eng.kind; card.setAttribute('role', 'group'); card.setAttribute('aria-label', 'Back engraving of this piece');
      card.innerHTML = S.panel(eng);
      host.replaceChildren(card);
      S.wirePanel(card, eng, { imageUrl: u => (/^https?:/.test(u) ? cors(u) : u), approve: b => approve(b), open: b => openEngrave(b) });
      // every wait is said: the picture still coming, the words still being read
      const pv = card.querySelector('.pv'), im = pv && pv.querySelector('img');
      if (im && !im.complete) {
        const w = doc().createElement('span'); w.className = 'owEngPvWait'; w.innerHTML = '<span class="owSpin" aria-hidden="true"></span><span>Loading the back…</span>'; pv.appendChild(w);
        im.addEventListener('load', () => w.remove(), { once: true });
        im.addEventListener('error', () => { pv.innerHTML = '<span class="by">Open in Engrave to see the back</span>'; }, { once: true });
      }
      const by = card.querySelector('.swEng > .by');
      if (by && eng.kind === 'preparing' && eng.working) { by.classList.add('owEngBy'); const sp = doc().createElement('span'); sp.className = 'owSpin'; sp.setAttribute('aria-hidden', 'true'); by.prepend(sp); by.setAttribute('role', 'status'); }
    }
    /** Look again; draw only when what the card shows has changed (or at once when asked). `quiet`: the host is not told. */
    function refresh(force, quiet) {
      if (gone || !host.isConnected) return;
      attached = true;
      if (busy) return;   // (this card's own approval is on its way: it draws the end of it)
      const r = engOf(ctx, seenAt), s = sigOf(r), has = !!(r.loading || r.eng && r.eng.kind !== 'none');
      last = r;
      if (!force && s === sig) return;
      // (something is here to show but cannot be drawn yet: the window is not on screen, or a seal is being pressed: it says so)
      const wait = () => { if (has && !host.firstChild) { host.hidden = false; host.replaceChildren(waitNode('Reading the back engraving…')); } if (!has) host.hidden = true; };
      if (!visible()) return wait();
      if (r.job && r.job.stamping) return wait();   // (drawn once the seal has landed)
      if (root.Seal && root.Seal.defer && root.Seal.defer(id, () => refresh(true, quiet))) return wait();
      const first = sig === null, before = shown; sig = s; shown = r.eng && r.eng.kind;
      render(r);
      if (!first && !quiet && before !== shown) told(r);
    }
    async function approve(btn) {
      const r = last || engOf(ctx, seenAt), job = r.job; if (!job || !btn) return;
      const who = tryDo(() => (root.CNEmployee && (root.CNEmployee.name() || root.CNEmployee.ask())) || '', '');
      if (!who) return;
      const token = busy = ++nextId; const key = ident(ctx);
      btn.disabled = true; btn.textContent = 'Approved'; btn.setAttribute('aria-busy', 'true');
      let ok = false;
      try {
        await root.Engrave.approve(job, who, btn);
        ok = DONE.includes(job.state);
        if (!ok && btn.isConnected) btn.disabled = false;
        // (Engrave.approve says nothing for a line that has left the pull, a cancelled order's: a press that did nothing says why)
        if (!ok && job.row && job.row.state === 'gone') say('This order has left the pull (cancelled or shipped), so its back engraving is not approved here.', '', 6000);
      } catch (e) { say('Not approved: ' + (e && e.message || e), 'bad', 6000); if (btn.isConnected) btn.disabled = false; }
      finally { if (btn.isConnected) { btn.removeAttribute('aria-busy'); btn.textContent = 'Approved'; } if (busy === token) busy = 0; }
      if (!ok || gone) return;
      poke();
      if (ident(ctx) === key) refresh(true, true);
      told(last);
    }
    async function openEngrave(btn) {
      const r = last || engOf(ctx, seenAt), t = target(r), link = root.EngraveLink;
      const html = btn && btn.innerHTML;
      if (btn) { btn.disabled = true; btn.innerHTML = '<span class="spin"></span>Opening Engrave…'; }
      try {
        if (link && typeof link.open === 'function') await link.open(t);
        else await fallbackOpen(r, t);
      } catch (e) { say('Could not open Engrave: ' + (e && e.message || e), 'bad', 5000); }
      finally { if (btn && btn.isConnected) { btn.disabled = false; btn.innerHTML = html; } }
    }
    function reset() {
      busy = 0; sig = null; shown = null; last = null; seenAt = Date.now();
      host.replaceChildren(); host.hidden = true;
    }
    const handle = {
      el: host,
      update(next) {
        if (gone) return;
        const was = ident(ctx); ctx = Object.assign({}, next);
        if (ident(ctx) !== was) { reset(); refresh(true, true); } else refresh(false);
      },
      refresh(force) { refresh(force); },
      /** What the card says about the piece now ("approve", "approved", "words", …; "" before it has looked): the host's red box follows it. */
      kind: () => (last && last.eng && last.eng.kind) || '',
      destroy() {
        if (gone) return; gone = true; live.delete(handle); disarm();
        if (ro) { tryDo(() => ro.disconnect()); ro = null; }
        host.replaceChildren(); host.hidden = true; host.classList.remove('owEng'); if (host._orderEngraving === handle) delete host._orderEngraving;
      },
      _tick() {
        // (a host taken out of the page takes the card with it; one not put in yet is waited for)
        if (!host.isConnected) return attached ? handle.destroy() : undefined;
        if (!visible()) return;
        refresh(sig === null);
      }
    };
    // (a card drawn while its window was still closed is drawn when the window shows: its seals are fitted to a box with a size)
    if (root.ResizeObserver) ro = tryDo(() => { const o = new root.ResizeObserver(() => { if (!gone && visible()) refresh(sig === null); }); o.observe(host); if (host.parentElement) o.observe(host.parentElement); return o; }, null);
    host._orderEngraving = handle; live.add(handle); arm();
    reset(); refresh(true, true);
    return handle;
  }
  /** The way to Engrave while EngraveLink is not here: the Sheet tab's own (close the window, open that order's engraving). */
  async function fallbackOpen(r, t) {
    const E = root.Engrave; if (!E || !E.restoreView) throw new Error('Engrave is not ready');
    try { if (root.OrderWin && root.OrderWin.close) await root.OrderWin.close(); } catch (_) {}
    const job = r.job, done = DONE.concat(['skipped']).includes(job && job.state) || (r.eng && r.eng.kind === 'approved');
    E.restoreView(Object.assign({}, E.view(), { tab: done ? 'done' : 'place', focus: done ? null : (job && job.key) || t.key, list: false, chosen: true, q: done ? t.rid : '' }));
    if (root.CN && root.CN.setMode) root.CN.setMode('engrave');
    E.render();
  }

  root.OrderEngraving = {
    mount,
    unmount: host => { if (host && host._orderEngraving) host._orderEngraving.destroy(); },
    engOf: ctx => engOf(ctx || {}, 0),
    version: 1
  };
})(typeof window !== 'undefined' ? window : globalThis);
