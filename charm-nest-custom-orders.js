/* charm-nest-custom-orders.js — A CUSTOM PIECE IS AN ORDER IN THE ORDERS TAB (CUSTOMORDERS; Paul, 28 Oct 2026: "Custom made charm, even though it's not attached to an existing
   order must still have all the same abilities and features that a regular order has. This means that I need to be able to see it in the orders tab in the list view. It needs to be
   searchable and the detailed order view needs to work for it just like any other order. This must work for both matched custom charms to a custom order and extra charms that the user
   decide to add the do not have a matching order.")

   A design sent to a sheet from the Charm Editor (Pool.addCustomPiece, charm-nest-custom-pool.js) is pool rows only: no Etsy line, so no row of the pull (B.orders.rows), so the Orders
   list, the search and the order window had nothing to show for it. This file makes ONE order row per custom line (its pool rows grouped by line key, `<order>_<serial>`), in the shape
   the pull's rows have ({ key, order, line, spec, poolIds, state, problems, material, custom: true }), for the places a person LOOKS at orders only:
     · the Orders list (Orders.listRows: the pull's rows and these), its search box and its filters;
     · the order window (OrderWin: open, openOrder, its pieces), with the Sheet tab and the timeline as any order's;
     · the search (charm-nest-search.js reads the pool rows already; it is given the piece's name and "custom");
     · the sheet window's Open order.
   They are NEVER put in B.orders.rows: the pull, the Etsy check (an order line Etsy does not have is "gone"), the intake, Review and the run's saved lines read that list only, and a
   custom piece is already pooled, placed and recorded by its own path. An order-less piece is an order of its own, CUSTOM-<yyyymmdd>-<n> (its pool rows' orderId), named by the name
   it was given; an order's piece (attached to order N) is one more line of order N, beside N's own lines when N is in the pull.

   Where its rows come from, newest first: the pool rows this page holds (B.pool.rows: a piece sent here, kept with the workspace) and the cloud's list of custom pieces (op customPieceList,
   read once when first asked and again every few minutes while asked; one order read again by its id, op poolList, when it is opened and not known yet). Read only: nothing is written here.

   window.CustomOrders = { rows(), rowsOf(rid), isCustom(rid), load(force), fetch(rid), version() } */
(function (root) {
  'use strict';
  if (!root || root.CustomOrders) return;
  const RID = /^CUSTOM-\d{8}-\d{1,6}$/;
  const GONE = new Set(['abandoned', 'superseded']);
  const REFRESH_MS = 4 * 60000;
  const cloud = new Map();          // poolId → the cloud's pool row of a custom piece
  const made = new Map();           // line key → the row made for it (kept, so a row is the same object from one drawing to the next)
  let loadedAt = 0, loading = null, version = 0, memo = null;
  const B = () => root.B || null;
  const num = v => (Number.isFinite(+v) ? +v : 0);
  const isCustom = rid => RID.test(String(rid == null ? '' : rid).trim());
  const tryDo = (f, d) => { try { const v = f(); return v === undefined ? d : v; } catch (_) { return d; } };
  // (the page's own api() and allSheets(): globals of its inline script, a const that is no property of window)
  const api = (body, o) => (typeof root.api === 'function' ? root.api('charmNestLibrary', body, Object.assign({ quiet: true }, o || {})) : Promise.reject(new Error('no api')));
  const sheetsNow = () => { try { return typeof allSheets === 'function' ? allSheets() : []; } catch (_) { return []; } };   // eslint-disable-line no-undef

  /** Every custom pool row known here, newest of each id. */
  function pools() {
    const out = new Map();
    const take = p => { if (!p || p.custom !== true || !p.poolId) return; const had = out.get(p.poolId); if (!had || num(p.updatedAt) >= num(had.updatedAt)) out.set(p.poolId, p); };
    for (const p of cloud.values()) take(p);
    const b = B(); if (b && b.pool && b.pool.rows) for (const p of b.pool.rows.values()) take(p);
    return [...out.values()];
  }
  /** Where a piece is, from what this page holds: the live sheet that has it placed, else the sheet its pool row names. */
  function sheetOf(p) {
    const live = tryDo(() => sheetsNow().find(sh => sh.charms && sh.charms.some(c => c.poolId === p.poolId) && (sh.placements || []).some(pl => { const c = sh.charms.find(x => x.id === pl.id); return c && c.poolId === p.poolId; })), null);
    return live ? (live.sheetId || 'live') : p.sheetId || null;
  }
  const pullOrder = rid => tryDo(() => { const r = (root.Orders && root.Orders.rows ? root.Orders.rows() : []).find(x => x && x.order && String(x.order.receiptId) === rid); return r ? r.order : null; }, null);
  /** The row of one custom line (its pool rows), made once and kept up to date in place. */
  function rowOf(list) {
    const p0 = list.slice().sort((a, b) => num(a.copy) - num(b.copy))[0], rid = String(p0.orderId || '').trim(), orderless = !!p0.orderless || isCustom(rid);
    const cp = p0.customPiece && typeof p0.customPiece === 'object' ? p0.customPiece : {}, name = String(p0.customName || cp.name || '').trim();
    const t = num(p0.orderDate) || num(p0.arrivedAt) || num(p0.createdAt) || Date.now(), key = String(p0.lineKey || `${rid}_${p0.transactionId || ''}`);
    const live = list.filter(p => !GONE.has(p.state)), placed = live.filter(sheetOf), qty = Math.max(num(p0.quantity), list.length) || 1;
    const real = orderless ? null : pullOrder(rid);
    const r = made.get(key) || { key, custom: true, problems: [], reason: null, claimedBy: null, engrave: null };
    r.order = real || Object.assign(r.order && r.order.receiptId === rid && !r.order.fromPull ? r.order : {}, { receiptId: rid, createTs: Math.floor(t / 1000), shipBy: 0, buyer: { name: orderless ? name || 'Custom piece' : '' }, custom: true, orderless });
    r.line = { transactionId: String(p0.transactionId || ''), sku: 'CUSTOM', title: `Custom charm · ${name || 'design ' + (cp.designId || '')}`.trim(), quantity: qty, listingId: null, variations: [], custom: true };
    r.spec = Object.assign(r.spec || {}, { designSku: 'CUSTOM', quantity: qty, personalization: [], engraveCandidate: false, problems: [],
      custom: { designId: cp.designId || '', version: num(cp.version), name, by: p0.by || cp.by || '', customId: p0.customId || '', orderless, widthMm: cp.widthMm, heightMm: cp.heightMm, fixes: cp.fixes || null } });
    r.poolIds = list.map(p => p.poolId).sort();
    r.material = r.metal = p0.material || null;
    r.arrivedAt = t;
    r.state = !live.length ? 'gone' : placed.length === live.length ? 'nested' : 'pooled';
    r.reason = !live.length ? 'taken off its sheet' : null;
    made.set(key, r);
    return r;
  }
  /** Every custom line as an order row, newest first. Read a first time (and again every few minutes) from the cloud when asked. */
  function rows() {
    if (!loading && Date.now() - loadedAt > REFRESH_MS) load().catch(() => {});
    const all = pools(), sig = version + '|' + all.length + '|' + all.reduce((n, p) => n + num(p.updatedAt) + (p.sheetId ? 1 : 0), 0) + '|' + tryDo(() => root.Orders.rows().length, 0) + '|' + tryDo(() => sheetsNow().reduce((n, s) => n + (s.placements || []).length, 0), 0);
    if (memo && memo.sig === sig) return memo.rows;
    const by = new Map();
    for (const p of all) { const k = String(p.lineKey || `${p.orderId}_${p.transactionId || ''}`); if (!by.has(k)) by.set(k, []); by.get(k).push(p); }
    const out = [...by.values()].map(rowOf).sort((a, b) => num(b.arrivedAt) - num(a.arrivedAt));
    memo = { sig, rows: out };
    return out;
  }
  const rowsOf = rid => { rid = String(rid == null ? '' : rid).trim(); return rows().filter(r => String(r.order.receiptId) === rid); };
  /** The cloud's list of custom pieces, read again (force: now). Draws the Orders list again when it brought something new. */
  function load(force) {
    if (loading) return loading;
    if (!force && Date.now() - loadedAt < REFRESH_MS) return Promise.resolve(false);
    loadedAt = Date.now();
    loading = api({ op: 'customPieceList', limit: 1000 }).then(res => {
      const before = cloud.size; let moved = false;
      for (const p of (res && res.pools) || []) { const had = cloud.get(p.poolId); if (!had || num(p.updatedAt) !== num(had.updatedAt) || p.sheetId !== had.sheetId || p.state !== had.state) moved = true; cloud.set(p.poolId, p); }
      if (moved || cloud.size !== before) { version++; redraw(); }
      return moved;
    }, e => { console.warn('[custom orders] list not read', e && e.message); loadedAt = Date.now() - REFRESH_MS + 30000; return false; }).finally(() => { loading = null; });
    return loading;
  }
  /** One order's custom pieces, read from the cloud (an order opened that this page does not know yet). → its rows. */
  async function fetch(rid) {
    rid = String(rid == null ? '' : rid).trim(); if (!rid) return [];
    try { const res = await api({ op: 'poolList', orderId: rid }); for (const p of (res && res.pools) || []) if (p && p.custom === true) cloud.set(p.poolId, p); version++; }
    catch (e) { console.warn('[custom orders] order not read', rid, e && e.message); }
    return rowsOf(rid);
  }
  function redraw() {
    try { const v = document.getElementById('ordersView'); if (root.Orders && root.Orders.render && v && !v.classList.contains('hidden')) root.Orders.render(); } catch (_) {}
    try { if (root.OrderSearch && root.OrderSearch.rebuild && root.OrderSearch.isOpen && root.OrderSearch.isOpen()) root.OrderSearch.rebuild(); } catch (_) {}
  }
  root.CustomOrders = { rows, rowsOf, isCustom, load, fetch, version: () => version, RID };
})(typeof window !== 'undefined' ? window : globalThis);
