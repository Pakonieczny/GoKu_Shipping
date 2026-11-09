/* Charm Nest · the Completed pile of the Orders tab (window.CNCompleted).
   CMP1 (Paul, 9 Nov 2026: "Add a new tab called "Completed" where orders that have been marked as shipped automatically get placed. Completed orders must get removed from the open orders
   tab and placed in the "Completed" tab. Also make sure that once an order is shipped, there is a shipped stamp and seal and the seal and stamp gets automatically applied and be visible in
   the "Completed" orders tab. Each completed order must contain a shipped seal.")

   What makes an order shipped (the same facts the SHIPPED seal is drawn from, charm-nest-piece-seals.js: 'shipped' / 'etsyCompleted' → the Shipped step):
     · its permanent timeline holds a `shipped` or an `etsyCompleted` event (a shipping station's scan, or the server's "Marked shipped on Etsy"); this file learns it from the timeline reads
       the page ALREADY makes (OrderTimeline.get: the order window and its Timeline, the Engrave tab's history) and from the events this page records itself (OrderTimeline.onRecord). It never asks for a timeline of its own.
     · the order's own record says so: Etsy's status (shipped / completed, isShipped / is_shipped / wasShipped), or a line the orders check found gone because Etsy shipped it (reason "shipped").
   Nothing here polls, listens to Firestore or reads anything: the cost is nil. What it learned is kept on this computer (localStorage, one list for the Real workspace and one for the
   Sandbox), so an order that leaves the pull after it shipped stays under Completed.

   The SHIPPED seal of a completed order is the existing one: the timeline's own seal (OrderTimelineUI.faceModel through PieceSeals.render, the very Seal.face, a .seal element, so Seal.zoom
   grows it in place when rested on, clicked, tapped or reached with Tab). An order shipped before seals existed, or marked shipped on Etsy, has no recorded event: its seal is drawn from the
   shipped fact the page has (Etsy's shipped time, the label / tracking time, else the time the page learned it), with the person "Etsy" when Etsy said it and no name when nobody is known
   (the server's own derived "Marked shipped on Etsy" event is signed "Etsy" the same way). A person is never invented.

   API
     world(name)            'production' | 'sandbox': which list is read and kept (the bridge says so once)
     factOfRow(row)         pure: { type, at, by, src } when the row's own data says the order shipped, else null
     factOfEvents(events)   pure: { type, at, by, src: 'timeline', ev } from the first shipped / etsyCompleted event, else null
     learn(rid, fact, rows) keeps the fact (and a small copy of the order for the list); true when the order was not known as shipped before (then 'cn-order-shipped' { rid } is fired)
     note(rows)             learns from the rows' own data (cheap: nothing is read); the rids newly known as shipped
     has(rid) / of(rid) / list() / count()
     eventOf(entry)         the timeline event the seal is drawn from (the recorded one, else one built from the fact)
     seal(entry, o)         the SHIPPED seal as an element (PieceSeals.render), null when the seal scripts are not there; o: { size, caption, pending }
     renderInto(host, o)    draws the Completed list in host; o: { q, range, key, dir, open(rid, node), rows: rid → live rows, stamp: Set of rids whose seal is to be stamped now }
*/
(function (root) {
  'use strict';
  const doc = root.document;
  if (!doc || root.CNCompleted) return;

  const KEY = 'cn.completed.v1', MAX = 3000, ZONE = 'America/Toronto';
  const SHIP_TYPES = ['shipped', 'etsyCompleted'];
  const SHIP_WORDS = /\b(shipped|completed?)\b/i;
  let worldName = 'production', store = null;
  const listeners = new Set();
  const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const digits = v => { const s = String(v == null ? '' : v).trim(); return /^CUSTOM-/i.test(s) ? s : s.replace(/\D/g, ''); };
  const msOf = v => { const n = typeof v === 'string' && !/^\d+(\.\d+)?$/.test(v) ? Date.parse(v) : +v; return Number.isFinite(n) && n > 0 ? (n < 1e12 ? n * 1000 : n) : 0; };
  const warned = new Set();
  const warn = (what, e) => { if (warned.has(what)) return; warned.add(what); try { console.warn('[completed] ' + what, e && e.message || e || ''); } catch (_) {} };

  /* ═══ what this computer knows ═══ */
  const keyOf = () => KEY + ':' + worldName;
  function load() {
    if (store) return store;
    store = new Map();
    try { const j = JSON.parse(root.localStorage.getItem(keyOf()) || '[]'); if (Array.isArray(j)) for (const e of j) if (e && e.rid) store.set(String(e.rid), e); } catch (_) {}
    return store;
  }
  let saveT = 0;
  function save() {
    clearTimeout(saveT);
    saveT = setTimeout(() => {
      try {
        let all = [...load().values()];
        if (all.length > MAX) { all.sort((a, b) => (+b.at || 0) - (+a.at || 0)); all = all.slice(0, MAX); store = new Map(all.map(e => [String(e.rid), e])); }
        root.localStorage.setItem(keyOf(), JSON.stringify(all));
      } catch (_) { /* a full storage keeps the list in memory for this visit */ }
    }, 400);
  }
  function world(name) { const w = name === 'sandbox' ? 'sandbox' : 'production'; if (w === worldName && store) return; worldName = w; store = null; load(); }

  /* ═══ the facts ═══ */
  /** The first shipped / etsyCompleted event of an order's timeline (the one the SHIPPED step draws). */
  function factOfEvents(events) {
    if (!Array.isArray(events)) return null;
    let best = null;
    for (const e of events) if (e && SHIP_TYPES.includes(e.type) && (!best || (+e.at || 0) < (+best.at || 0))) best = e;
    if (!best) return null;
    const ev = { id: String(best.id || best.key || ''), type: best.type, at: +best.at || 0, by: String(best.by || ''), station: String(best.station || ''), source: String(best.source || '') };
    if (best.data && typeof best.data === 'object') ev.data = Object.assign({}, best.data);
    return { type: best.type, at: ev.at, by: ev.by, src: 'timeline', ev };
  }
  /** What the row's own data says: Etsy's status on the order, or a line the orders check found gone because Etsy shipped it. */
  function factOfRow(r) {
    try {
      if (!r || !r.order) return null;
      const o = r.order, l = r.line || {};
      const shipTime = () => msOf(o.shippedAt || o.shipped_timestamp || o.shipmentAt || o.shipment_notification_timestamp || l.shippedAt || l.shipped_timestamp
        || (Array.isArray(o.shipments) && o.shipments.length ? Math.min(...o.shipments.map(s => msOf(s && (s.shippedAt || s.shipment_notification_timestamp || s.at)) || Infinity)) : 0));
      const label = Array.isArray(o.shipments) && o.shipments.length && !(o.isShipped || o.is_shipped || o.wasShipped || o.was_shipped);
      const says = !!(o.isShipped || o.is_shipped || o.wasShipped || o.was_shipped || o.shipped === true || (typeof o.status === 'string' && SHIP_WORDS.test(o.status)) || (typeof o.etsyStatus === 'string' && SHIP_WORDS.test(o.etsyStatus)));
      if (says || label) {
        const t = shipTime() || msOf(o.updateTs) || 0;
        return { type: /complet/i.test(String(o.status || o.etsyStatus || '')) ? 'etsyCompleted' : 'shipped', at: t, by: label ? '' : 'Etsy', src: label ? 'label' : 'etsy' };
      }
      if (r.state === 'gone' && /^\s*(shipped|completed)\b/i.test(String(r.reason || ''))) {
        return { type: 'shipped', at: +r.shippedAt || msOf(o.updateTs) || +r.activityAt || 0, by: 'Etsy', src: 'etsy' };
      }
      return null;
    } catch (e) { warn('a row', e); return null; }
  }
  /** A small copy of the order for the list (what a row of the Completed list shows), from its lines in the pull. */
  function snapOf(rows) {
    const list = (rows || []).filter(r => r && r.order); if (!list.length) return null;
    const o = list[0].order;
    return { buyer: String((o.buyer && o.buyer.name) || ''), shipBy: +o.shipBy || 0, createTs: +o.createTs || 0,
      lines: list.slice(0, 12).map(r => ({ key: String(r.key || ''), sku: String((r.spec && r.spec.designSku) || (r.line && r.line.sku) || ''), title: String((r.line && r.line.title) || '').slice(0, 160), qty: +((r.spec && r.spec.quantity) || (r.line && r.line.quantity) || 1) || 1, material: String(r.material || '') })) };
  }
  const rank = f => f.src === 'timeline' ? 3 : f.src === 'label' ? 2 : 1;
  function learn(rid, fact, rows) {
    rid = digits(rid); if (!rid || !fact) return false;
    const m = load(), was = m.get(rid), snap = snapOf(rows);
    if (was) {
      // (a recorded event wins over a fact read from the order: the seal then shows who shipped it and where; the copy of the order is kept fresh while the pull holds it)
      let changed = false;
      if (rank(fact) > rank(was) || (rank(fact) === rank(was) && !was.at && fact.at)) { Object.assign(was, { type: fact.type, at: fact.at || was.at, by: fact.by, src: fact.src }); if (fact.ev) was.ev = fact.ev; else delete was.ev; changed = true; }
      if (snap && JSON.stringify(snap) !== JSON.stringify(was.snap)) { was.snap = snap; changed = true; }
      if (changed) save();
      return false;
    }
    const e = { rid, type: fact.type || 'shipped', at: +fact.at || Date.now(), by: String(fact.by || ''), src: fact.src || 'etsy', seenAt: Date.now() };
    if (fact.ev) e.ev = fact.ev;
    if (snap) e.snap = snap;
    m.set(rid, e); save();
    for (const f of listeners) { try { f(rid, e); } catch (_) {} }
    try { doc.dispatchEvent(new CustomEvent('cn-order-shipped', { detail: { rid, at: e.at } })); } catch (_) {}
    return true;
  }
  /** The rows' own facts, learned (grouped by order: one fact an order). → the rids newly known as shipped. */
  function note(rows) {
    const out = [], by = new Map();
    for (const r of rows || []) {
      if (!r || !r.order) continue;
      const rid = String(r.order.receiptId || ''), f = factOfRow(r); if (!rid) continue;
      if (f) { const g = by.get(rid) || { f: null, rows: [] }; if (!g.f || (+f.at && (!g.f.at || f.at < g.f.at))) g.f = f; by.set(rid, g); }
    }
    if (!by.size) return out;
    for (const r of rows) { const g = r && r.order && by.get(String(r.order.receiptId)); if (g) g.rows.push(r); }
    for (const [rid, g] of by) if (learn(rid, g.f, g.rows)) out.push(rid);
    return out;
  }
  const has = rid => load().has(digits(rid));
  const of = rid => load().get(digits(rid)) || null;
  const list = () => [...load().values()];
  const count = () => load().size;
  function forget(rid) { if (load().delete(digits(rid))) save(); }
  const onChange = f => { listeners.add(f); return () => listeners.delete(f); };

  /* the timeline reads the page already makes, and the events it records itself: a shipped event there is learned, nothing more is read */
  function hook() {
    const T = root.OrderTimeline;
    if (!T || T._cmpHooked) return !!T;
    T._cmpHooked = true;
    if (typeof T.get === 'function') {
      const get = T.get;
      T.get = function (orderId) {
        const p = get.apply(this, arguments);
        try { Promise.resolve(p).then(tl => { const f = tl && factOfEvents(tl.events); if (f) learn(orderId, f, rowsOfRid(orderId)); }, () => {}); } catch (_) {}
        return p;
      };
    }
    if (typeof T.onRecord === 'function') try { T.onRecord(ev => { if (ev && SHIP_TYPES.includes(ev.type) && ev.orderId) learn(ev.orderId, factOfEvents([ev]), rowsOfRid(ev.orderId)); }); } catch (_) {}
    return true;
  }
  const rowsOfRid = rid => { try { const id = digits(rid); return root.Orders && Orders.rows ? Orders.rows().filter(r => r && r.order && String(r.order.receiptId) === id) : []; } catch (_) { return []; } };

  /* ═══ the SHIPPED seal ═══ */
  /** The event the seal is drawn from: the recorded one, else one built from the fact (Etsy's derived event signs itself "Etsy"; a label or tracking time names nobody). */
  function eventOf(e) {
    if (!e) return null;
    if (e.ev && SHIP_TYPES.includes(e.ev.type)) return Object.assign({ orderId: e.rid }, e.ev);
    return { orderId: e.rid, id: 'cmp-' + (e.src || 'etsy') + '-shipped', type: e.type === 'etsyCompleted' ? 'etsyCompleted' : 'shipped', at: +e.at || 0, by: String(e.by || ''),
      source: e.src === 'label' ? 'label' : 'etsy', station: 'shipping', text: e.src === 'label' ? 'Shipping label bought' : 'Marked shipped on Etsy', data: { approx: true } };
  }
  /** The seal as an element: PieceSeals' own (the timeline's face, a .seal that zooms in place), with its quiet who · where · when line. */
  function seal(e, o) {
    o = o || {};
    try {
      const ev = eventOf(e); if (!ev || !root.PieceSeals || !root.Seal) return null;
      const el = root.PieceSeals.render('shipped', { events: [ev] }, { size: o.size || 44, caption: o.caption !== false, done: true });
      if (!el) return null;
      el.classList.add('cmpSeal'); el.dataset.testid = 'shipped-seal'; el.setAttribute('data-testid', 'shipped-seal');
      el.dataset.cmpRid = String(e.rid || ''); el.dataset.cmpSrc = String(e.src || '');
      const s = el.querySelector('.seal'); if (s && o.pending) s.classList.add('pending');
      return el;
    } catch (err) { warn('the seal', err); return null; }
  }
  /** The wooden stamp comes down on the seal (the Seal module's own press), once it is in sight. */
  function stamp(el) {
    const s = el && (el.matches && el.matches('.seal') ? el : el.querySelector && el.querySelector('.seal'));
    if (!s || !root.Seal || typeof root.Seal.press !== 'function') { if (s) s.classList.remove('pending'); return Promise.resolve(); }
    s.classList.add('pending');
    try { return Promise.resolve(root.Seal.press(s)).catch(() => s.classList.remove('pending')); } catch (_) { s.classList.remove('pending'); return Promise.resolve(); }
  }

  /* ═══ the Completed list ═══ */
  const fmt = o => { let f = null; return t => { try { return (f = f || new Intl.DateTimeFormat('en-US', o)).format(new Date(+t)); } catch (_) { return ''; } }; };
  const dayW = fmt({ timeZone: ZONE, weekday: 'short', month: 'short', day: 'numeric', year: 'numeric' });
  const findText = (e, live) => {
    const s = e.snap || {}, lines = live && live.length ? live.map(r => ({ sku: (r.spec && r.spec.designSku) || (r.line && r.line.sku) || '', title: (r.line && r.line.title) || '' })) : s.lines || [];
    const buyer = live && live[0] && live[0].order && live[0].order.buyer ? live[0].order.buyer.name : s.buyer;
    return [e.rid, buyer, e.by].concat(lines.map(l => l.sku + ' ' + l.title)).join(' \u0001 ').toLowerCase();
  };
  function select(o) {
    o = o || {};
    const A = root.CNListActivity, rows = o.rows || new Map(), words = String(o.q || '').trim().toLowerCase().split(/\s+/).filter(Boolean);
    const dir = o.dir === 'asc' ? 1 : -1, key = o.key || 'activity';
    const val = e => { const s = e.snap || {}; return key === 'arrival' ? +s.createTs || 0 : key === 'due' ? +s.shipBy || 0 : key === 'order' ? 0 : +e.at || 0; };
    return list().filter(e => {
      if (o.range && o.range !== 'all' && A && A.matches && !A.matches(+e.at || 0, o.range)) return false;
      if (!words.length) return true;
      const hay = findText(e, rows.get(e.rid)); return words.every(w => hay.includes(w));
    }).sort((a, b) => (key === 'order' ? String(a.rid).localeCompare(String(b.rid), 'en', { numeric: true }) * dir : (val(a) - val(b)) * dir) || String(b.rid).localeCompare(String(a.rid)));
  }
  const PAGE = 60;
  const W = { host: null, nodes: new Map(), limit: PAGE, drawn: '' };
  function rowHtml(e, live) {
    const s = e.snap || {}, lines = live && live.length ? live.map(r => ({ sku: (r.spec && r.spec.designSku) || (r.line && r.line.sku) || '', title: (r.line && r.line.title) || '', qty: +((r.spec && r.spec.quantity) || (r.line && r.line.quantity) || 1) || 1 })) : s.lines || [];
    const buyer = live && live[0] && live[0].order && live[0].order.buyer && live[0].order.buyer.name || s.buyer || '';
    const first = lines[0] || null, more = lines.length - 1, pieces = lines.reduce((n, l) => n + (+l.qty || 1), 0);
    return `<div class="cmpId"><span class="queueLabel">Order</span><b class="mono onum">${esc(e.rid)}</b>${buyer ? `<span class="cmpBuyer">${esc(buyer)}</span>` : ''}</div>`
      + `<div class="cmpWhat">${first ? `<span class="sku mono">${esc(first.sku || 'No SKU')}</span><span class="rowExcerpt" title="${esc(first.title)}">${esc(first.title || '')}</span>` : '<span class="rowExcerpt dim">Its pieces are in the order window</span>'}${more > 0 ? `<span class="cmpMore">+${more} more line${more === 1 ? '' : 's'}</span>` : ''}</div>`
      + `<div class="cmpFacts"><span class="ost ok">Shipped</span>${pieces ? `<span class="rowFacts">${pieces} piece${pieces === 1 ? '' : 's'}</span>` : ''}<span class="rowFacts cmpDay">${e.at ? esc(dayW(e.at)) : ''}</span></div>`
      + `<div class="cmpSealCell"></div>`;
  }
  function renderInto(host, o) {
    o = o || {};
    if (!host) return 0;
    const rows = o.rows || new Map(), stampSet = o.stamp || new Set();
    const all = select(o), key = JSON.stringify([o.q || '', o.range || 'all', o.key || 'activity', o.dir || 'desc']);
    if (W.host !== host || W.drawn !== key) { W.limit = PAGE; }
    let wrap = host.querySelector(':scope > .cmpWrap');
    if (!wrap) { host.innerHTML = ''; wrap = doc.createElement('div'); wrap.className = 'cmpWrap'; wrap.innerHTML = '<div class="cmpSum" role="status"></div><div class="cmpList"></div><div class="cmpFoot"></div>'; host.appendChild(wrap); }
    W.host = host; W.drawn = key;
    const sum = wrap.querySelector('.cmpSum'), listEl = wrap.querySelector('.cmpList'), foot = wrap.querySelector('.cmpFoot');
    const total = count(), dirWord = o.dir === 'asc' ? 'oldest' : 'newest', what = o.key === 'order' ? 'by order number' : o.key === 'arrival' ? `${dirWord} order first` : `${dirWord} shipped first`;
    sum.innerHTML = String(o.q || '').trim() ? `<b>${all.length}</b> of ${total} completed match “${esc(String(o.q).trim())}”` : `<b>${all.length}</b> completed order${all.length === 1 ? '' : 's'} · ${what}`;
    const shown = all.slice(0, W.limit), out = [], pressed = [];
    for (const e of shown) {
      const live = rows.get(e.rid) || null, sig = JSON.stringify([e, live ? live.map(r => [r.key, r.spec && r.spec.designSku, r.line && r.line.title]) : 0]);
      let n = W.nodes.get(e.rid);
      if (!n || n.sig !== sig || !n.node.querySelector('.seal')) {
        const node = doc.createElement('div'); node.className = 'cmpRow hoverItem'; node.dataset.rid = e.rid; node.dataset.mkey = 'cmp:' + e.rid;
        node.setAttribute('role', 'button'); node.tabIndex = 0; node.title = `Open order ${e.rid}`;
        node.innerHTML = rowHtml(e, live);
        const sl = seal(e, { size: 46, caption: true }); if (sl) node.querySelector('.cmpSealCell').appendChild(sl);
        const go = () => { if (o.open) o.open(e.rid, node); };
        node.onclick = ev => { if (ev.target.closest('button,a,.seal')) return; go(); };
        node.onkeydown = ev => { if (ev.target === node && (ev.key === 'Enter' || ev.key === ' ')) { ev.preventDefault(); go(); } };
        if (n && n.node.isConnected) n.node.replaceWith(node);
        n = { sig, node }; W.nodes.set(e.rid, n);
      }
      if (stampSet.has(e.rid)) { stampSet.delete(e.rid); const s = n.node.querySelector('.seal'); if (s) { s.classList.add('pending'); pressed.push(n.node); } }
      out.push(n.node);
    }
    if (!all.length) {
      const empty = doc.createElement('div'); empty.className = 'libEmpty cmpEmpty'; empty.dataset.mkey = 'cmpEmpty';
      empty.innerHTML = String(o.q || '').trim() || (o.range && o.range !== 'all')
        ? `<span>No completed order matches these filters.</span>`
        : `<span>No order has shipped yet.</span><small>An order marked shipped (at the shipping station, or on Etsy) is placed here automatically, with its SHIPPED seal.</small>`;
      out.push(empty);
    }
    if (root.Motion && root.Motion.reconcile) root.Motion.reconcile(listEl, out, { animate: false, clip: host }); else listEl.replaceChildren(...out);
    foot.innerHTML = '';
    if (shown.length < all.length) {
      const b = doc.createElement('button'); b.type = 'button'; b.className = 'btn ghost listMore'; b.textContent = `Show more · ${shown.length} of ${all.length}`;
      b.onclick = () => { W.limit += PAGE; renderInto(host, o); }; foot.appendChild(b);
    }
    // (an order that shipped while the list was in sight: its seal waits unseen, then the wooden stamp comes down on it)
    for (const n of pressed) { const t = () => { if (n.isConnected) stamp(n); }; if (root.requestAnimationFrame) root.requestAnimationFrame(() => setTimeout(t, 120)); else setTimeout(t, 120); }
    return all.length;
  }

  load(); hook();
  // (order-timeline.js is loaded before this file; should a page load it later, it is hooked on first use)
  if (!root.OrderTimeline) doc.addEventListener('DOMContentLoaded', hook, { once: true });
  root.CNCompleted = { world, factOfRow, factOfEvents, learn, note, has, of, list, count, forget, onChange, eventOf, seal, stamp, select, renderInto, hook, SHIP_TYPES };
})(typeof self !== 'undefined' ? self : this);
