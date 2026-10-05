/* One truth for "which sheet holds which piece of which order" (Paul, 5 Oct 2026: an order with a charm on a Gold Filled
 * sheet and another on a Silver sheet said, from the Gold Filled side, that the Silver charm was "not on a sheet yet", and held
 * the Gold Filled sheet back as "still pooled").
 *
 * Why it broke. The page asked FOUR different things where one was needed:
 *   1. the sheet record's own list of pieces (poolIds / placements): what the sheet window and the laser read: always right;
 *   2. the pool row's sheetId (Charm_Pool): written once when a sheet is saved, and cleared to null by every re-nest, restart
 *      and "older run" path (Pool.update(... sheetId:null ...)), so a piece that IS on a saved sheet can carry no sheet;
 *   3. Pool.sheetOf(): only the sheets this sorter holds live on its own pages (a sheet built in another session is invisible);
 *   4. the order line's stored state ("pooled" until the set is written), which the readiness check read as "not nested".
 * "This sheet" came from (1) while "the other pieces" came from (2)-(4), so a piece seen from its own sheet was fine and the
 * same piece seen from the order's other sheet was lost.
 *
 * Now there is one rule: a piece is on a sheet when a sheet RECORD lists its pool id (or a live page places it); the pool row's
 * sheetId is only a hint, and only used while the records have not been read or are silent about it. Nothing is ever written.
 *
 *   CharmNestOrderPieces.resolve({ orderId, lines, pools, sheets, sheetsKnown, livePlaced(poolId), liveSheet(sheetId) })
 *       pure (also required by node tests): the pieces of one order
 *   window.OrderPieces.of(orderId)            -> [{ key, lineKey, index, label, sku, metal, qty, copy, poolId, transactionId,
 *                                                  state:'unnested'|'nested'|'sheeted'|'cut', nested, sheetId|null, sheetLabel|null,
 *                                                  setId|null, problem:null|'noSku'|'unmatched'|'noDesign'|'held', reason, why,
 *                                                  thumb, listingId, loading, unsure, hand }]
 *       (hand: the custom order's completion record when a person completed the piece by hand (Review → Complete Order, or its QR label printed):
 *        it needs no sheet, is never loading or "not on a sheet yet", and is numbered after the pieces that are cut, as a no-design one)
 *   window.OrderPieces.spread(orderId)        -> { multi, sheets:[{sheetId,sheetLabel,setId,metal,pieces:[key]}], unnested:[piece],
 *                                                  spreadAcross, loading, unsure }
 *   window.OrderPieces.ofLine(orderId, lineKey) · nestedOf(orderId, lineKey) · onSheet(sheetId) · ordersOf(setId) · known(orderId)
 *   window.OrderPieces.loadSheet(sheetId) -> Promise: reads that saved sheet whole and the records of every order on it (then onSheet is complete)
 *   window.OrderPieces.load(orderIds, { force }) -> Promise: one read of the sheet records that name the orders' pieces (op
 *       getOrderPieces); learn(orderId, { rows }) hands over the order lines the order window built from the records;
 *       subscribe(fn) says when anything changed (a read landed).
 * Synchronous and cheap: of()/spread() never read the network. While the records of an order are not read, a piece nothing else
 * places is nested:false with loading:true (so nobody says "not on a sheet" before the sheets were asked). */
(function (root, factory) { const api = factory(); if (typeof module === 'object' && module.exports) module.exports = api; else { root.CharmNestOrderPieces = api; root.OrderPieces = api.makePage(root); } })(typeof self !== 'undefined' ? self : this, function () {
  'use strict';
  const CODE = { gold: 'GF', silver: 'SS', rose: 'RG', gold10k: '10K', gold14k: '14K' };
  const GONE = new Set(['abandoned', 'superseded']);
  const POOL_ID = /^(\d{4,20})_([^_]*)_(\d{1,3})$/;
  const sidOf = s => (s && (s.id || s.sheetId)) || null;
  const stampOf = s => +(s && (s.updatedAt || s.createdAt)) || 0;
  const nOfName = name => +((/_Sheet-(\d+)/.exec(name || '') || [])[1]) || 0;
  /** A sheet's number as the Library cards and the sheet window say it. */
  const sheetNo = s => +s.sheetIndex || nOfName(s.folder) || nOfName(s.fileBase) || nOfName(s.sheetName) || +s.page || 1;
  const labelOf = s => `${CODE[s.metal] || ''} Sheet ${sheetNo(s)}`.trim();
  const tailNo = k => { const m = /_(\d+)$/.exec(String(k || '')); return m ? +m[1] : 0; };
  const cleanSku = x => String(x || '').replace(/_+/g, ' ').replace(/\s+/g, ' ').trim();
  const cut = s => !!(s && (+s.laserDoneAt > 0 || +s.roseCutAt > 0));

  /** A piece a person completed by hand (Review → Complete Order, or its QR label printed from Custom Orders): the custom order's own record, as the line
   *  carries it (spec.customDone) or the server read it (handDone). The same rule as CharmNestReadiness.isHand: resolved, it needs no sheet; Reopen
   *  (state 'open') takes it back; a custom order sent to the sheets with its own designs (how 'sheet') is cut. Only a line with no pool ids: a pooled one
   *  is waiting for the nester (and the card a person completes was never pooled). */
  const handOf = l => { const c = l && !(Array.isArray(l.poolIds) && l.poolIds.length) && (l.handDone || (l.spec && l.spec.customDone)); return c && typeof c === 'object' && c.state !== 'open' && c.how !== 'sheet' ? c : null; };
  function problemOf(l, sku) {
    if (!l) return sku ? null : 'noSku';
    const sp = l.spec || {}, kinds = (l.problems || []).map(p => String((p && p.kind) || p || ''));
    if (l.hold || l.state === 'held' || l.state === 'oversize' || kinds.includes('oversize')) return 'held';
    if (handOf(l)) return null;   // (resolved: it has no SKU or design question left, whatever its record still says)
    if (l.state === 'noDesign' || l.noDesign || sp.noDesign || kinds.includes('missingSize')) return 'noDesign';
    if (!sku) return 'noSku';
    if (l.state === 'unmatched' || kinds.some(k => /unmatched|needsMaterial|needsMapping/i.test(k))) return 'unmatched';
    return null;
  }
  const HAND_REASON = 'it was completed by hand and needs no sheet';
  const REASON = { noSku: 'it has no SKU', unmatched: 'its SKU is not in any master file', noDesign: 'it has no design yet', held: 'it is on hold' };

  /** The pieces of one order. See the header: records first, live pages next, the pool row's sheetId only as a hint. */
  function resolve(input) {
    const rid = String(input.orderId || '').replace(/\D/g, ''), prefix = rid + '_';
    const livePlaced = typeof input.livePlaced === 'function' ? input.livePlaced : () => null;
    const liveSheet = typeof input.liveSheet === 'function' ? input.liveSheet : () => null;
    const lines = (input.lines || []).filter(l => l && l.state !== 'gone');
    const goneKeys = new Set((input.lines || []).filter(l => l && l.state === 'gone').map(l => String(l.key)));
    const pools = new Map(); for (const p of input.pools || []) if (p && p.poolId && String(p.orderId == null ? String(p.poolId).split('_')[0] : p.orderId) === rid) { const k = String(p.poolId), cur = pools.get(k); if (!cur || (p.sheetId && !cur.sheetId) || stampOf(p) > stampOf(cur) && !(cur.sheetId && !p.sheetId)) pools.set(k, p); }
    const recById = new Map();
    for (const s of input.sheets || []) { const id = sidOf(s); if (!id || s.archived) continue; const cur = recById.get(id); if (!cur || stampOf(s) >= stampOf(cur)) recById.set(id, s); }
    const byPool = new Map();
    for (const s of recById.values()) for (const id of s.poolIds || []) { const k = String(id); if (!k.startsWith(prefix)) continue; (byPool.get(k) || byPool.set(k, []).get(k)).push(s); }
    const known = !!input.sheetsKnown, unsure = !!input.failed;

    // 1 · the pieces: every copy of every line, then pool rows and sheet entries no line explains (a line lost from the pull)
    const out = [], seen = new Set();
    const add = (id, line, extra) => {
      if (seen.has(id)) return; seen.add(id);
      const m = POOL_ID.exec(id), p = pools.get(id) || null;
      out.push(Object.assign({ id, line, pool: p, copy: m ? +m[3] : 1, tx: m ? m[2] : '', lineKey: line && line.key || (m ? `${m[1]}_${m[2]}` : id.replace(/_\d+$/, '')) }, extra || {}));
    };
    // (the order Issues-truth's CharmNestReadiness.pieces counts in, so that "piece 2" is the same piece in the Library's issues panel and here:
    //  lines by the number at the end of their key, a line's copies by their copy number (the number at the end of the pool id; the quantity's missing ones are "<line key>_<n>"))
    for (const l of lines.slice().sort((a, b) => tailNo(a.key) - tailNo(b.key))) {
      const key = String(l.key || `${rid}_${l.transactionId || ''}`), qty = Math.max(1, Math.floor(+((l.spec && l.spec.quantity) || l.quantity) || 1));
      const ids = [...new Set((Array.isArray(l.poolIds) ? l.poolIds : []).filter(Boolean).map(String))];
      for (let n = 1; ids.length < qty; n++) if (!ids.includes(`${key}_${n}`)) ids.push(`${key}_${n}`);
      for (const [id, p] of pools) if (p.lineKey === key && !ids.includes(id) && !GONE.has(p.state)) ids.push(id);
      ids.sort((a, b) => tailNo(a) - tailNo(b));   // (stable: by copy number, so a record that lists _3 before _1 does not change which piece is piece 2)
      for (const id of ids) add(id, l, { qty: Math.max(qty, ids.length) });
    }
    for (const [id, p] of pools) if (!seen.has(id) && !GONE.has(p.state)) add(id, null, { qty: +p.quantity || 1 });
    for (const id of byPool.keys()) if (!seen.has(id)) add(id, null, { qty: 1 });

    // 2 · where each piece is
    const pieces = out.map((o, i) => {
      const id = o.id, p = o.pool, page = livePlaced(id) || null;
      // a record names a sheet this sorter holds live, and that page no longer places the piece (taken off, moved): the page is the truth
      const claims = (byPool.get(id) || []).filter(s => { const lp = liveSheet(sidOf(s)); return !(lp && lp !== page && (+lp.placedCount > 0 || +(lp.placements && lp.placements.length) > 0)); });
      let holder = null, via = null;
      if (page) { const rec = page.sheetId ? recById.get(page.sheetId) : null; holder = Object.assign({}, rec || {}, { id: page.sheetId || null, metal: (rec && rec.metal) || page.metal, sheetIndex: page.sheetIndex || (rec && rec.sheetIndex) || null, page: page.page || (rec && rec.page), fileBase: page.fileBase || (rec && rec.fileBase), setId: page.setId || (rec && rec.setId) || null, laserDoneAt: page.laserDoneAt || (rec && rec.laserDoneAt) || 0, roseCutAt: page.roseCutAt || (rec && rec.roseCutAt) || 0 }); via = 'page'; }
      else if (claims.length) {
        const want = p && p.sheetId ? claims.find(s => sidOf(s) === p.sheetId) : null;
        holder = want || claims.slice().sort((a, b) => (!!a.draft - !!b.draft) || stampOf(b) - stampOf(a))[0]; via = 'record';
      } else if (p && p.sheetId && !GONE.has(p.state)) {
        const rec = recById.get(p.sheetId);
        // a record that is read and lists other pieces but not this one says the pool row is out of date; a record that lists
        // nothing (an old one) or one not read yet cannot contradict it
        const contradicted = rec ? (rec.poolIds || []).length > 0 : known;
        if (!contradicted) {
          const nm = /^([A-Za-z0-9]+)_.*_Sheet-(\d+)/.exec(p.sheetName || '');
          holder = rec ? Object.assign({}, rec) : { id: p.sheetId, metal: p.material || (nm && Object.keys(CODE).find(k => CODE[k] === nm[1])) || null, sheetIndex: nm ? +nm[2] : 0, fileBase: p.sheetName || '', setId: p.setId || null };
          via = 'hint';
        }
      }
      const l = o.line, sp = (l && l.spec) || {}, sku = (l && (l.sku || sp.designSku)) || (p && p.sku) || '', metal = (l && (l.material || sp.material)) || (p && p.material) || (holder && holder.metal) || null;
      const problem = problemOf(l, sku);
      const nested = !!holder, state = !holder ? 'unnested' : via === 'page' && !holder.id ? 'nested' : cut(holder) ? 'cut' : 'sheeted';
      // (completed by hand: a piece that needs no sheet is not waiting for the sheets to be read, and never "not on a sheet yet";
      //  a HELD piece is still a held piece: a person's stop holds wherever it sits, as CharmNestReadiness reads it)
      const hand = !nested && !(l && (l.hold || l.changePending)) ? handOf(l) : null;
      const loading = !nested && !known && !unsure && !hand, unsureHere = !nested && unsure && !hand;
      let reason = '';
      if (!nested) reason = hand ? HAND_REASON : loading ? '' : unsureHere ? 'its sheets could not be read just now' : problem ? REASON[problem] : l && l.state === 'pooled' ? 'it is waiting to be placed on a sheet' : 'it is not on a sheet yet';
      const thumb = (page && input.thumbOf && input.thumbOf(id)) || (l && l.thumb) || null;
      return { key: id, lineKey: o.lineKey, index: 0, hand, noDesign: !!(hand || l && !handOf(l) && (l.state === 'noDesign' || l.noDesign || sp.noDesign)), label: cleanSku(sku || (l && l.title)) || 'Piece', sku, metal, qty: o.qty, copy: o.copy, poolId: id, transactionId: o.tx, state, nested,
        sheetId: holder ? holder.id || null : null, sheetLabel: holder ? labelOf(Object.assign({}, holder, { metal: holder.metal || metal })) : null, sheetNo: holder ? sheetNo(holder) : null, setId: holder ? holder.setId || (p && p.setId) || null : null,
        problem, reason, why: (l && l.reason) || '', thumb, listingId: (l && l.listingId) || '', loading, unsure: unsureHere, via, gone: !l && goneKeys.has(o.lineKey) };
    });
    // a line cancelled or gone from the order: its pieces that are still on a sheet are reported (gone), those on none are not
    // numbered as CharmNestReadiness.pieces numbers them: the live pieces 1.. in order; a piece with nothing to cut (no design), or cancelled and still on a sheet, after them
    const shown = pieces.filter(p => !(p.gone && !p.nested)), live = shown.filter(p => !p.gone && !p.noDesign), rest = shown.filter(p => p.gone || p.noDesign);
    return live.concat(rest).map((p, i) => Object.assign(p, { index: i + 1 }));
  }

  /** How an order's pieces are spread over sheets. */
  function spread(all) {
    const pieces = all.filter(p => !p.gone), sheets = new Map(), unnested = [];
    for (const p of pieces) {
      if (!p.nested) { unnested.push(p); continue; }
      const k = p.sheetId || 'page:' + p.sheetLabel;
      let s = sheets.get(k); if (!s) sheets.set(k, s = { sheetId: p.sheetId || null, sheetLabel: p.sheetLabel, sheetNo: p.sheetNo, setId: p.setId || null, metal: p.metal || null, pieces: [] });
      s.pieces.push(p.key);
    }
    return { multi: pieces.length > 1, sheets: [...sheets.values()], unnested, spreadAcross: sheets.size > 1, loading: pieces.some(p => p.loading), unsure: pieces.some(p => p.unsure) };
  }

  /** The page's face of the module: reads what the page holds, learns what the order window reads, asks the server once. */
  function makePage(root) {
    const learned = new Map();      // orderId -> { at, pools, sheets, rows, ok, failed, task }
    const fullSheets = new Map();   // sheetId -> the sheet's record with ALL its poolIds (loadSheet)
    const subs = new Set();
    let version = 0, lib = null;
    const bump = () => { version++; for (const f of [...subs]) { try { f(version); } catch (e) { console.warn('OrderPieces subscriber', e); } } };
    const ridOf = x => String(x == null ? '' : x).replace(/\D/g, '');
    const entry = rid => { let e = learned.get(rid); if (!e) learned.set(rid, e = { at: 0, pools: [], sheets: [], rows: null, ok: false, failed: null, task: null }); return e; };

    // the Library's rows (Current tab, loaded), indexed once per list
    function libIndex() {
      const L = root.CN && root.CN.S && root.CN.S.library, rows = (L && L.rows) || [];
      if (lib && lib.rows === rows && lib.n === rows.length && lib.at === (L && L.loadedAt)) return lib;
      const byPool = new Map(), byOrder = new Map(), bySheet = new Map(), bySet = new Map();
      for (const r of rows) {
        const id = sidOf(r); if (!id || r.archived) continue;
        bySheet.set(id, r);
        if (r.setId) (bySet.get(r.setId) || bySet.set(r.setId, []).get(r.setId)).push(r);
        for (const o of r.orders || []) (byOrder.get(String(o)) || byOrder.set(String(o), []).get(String(o))).push(r);
        for (const p of r.poolIds || []) { const k = String(p); (byPool.get(k) || byPool.set(k, []).get(k)).push(r); const o = k.split('_')[0]; const l = byOrder.get(o); if (!l || !l.includes(r)) (l || byOrder.set(o, []).get(o)).push(r); }
      }
      return lib = { rows, n: rows.length, at: L && L.loadedAt, byPool, byOrder, bySheet, bySet };
    }
    const pages = () => { try { return typeof allSheets === 'function' ? allSheets() : []; } catch (_) { return []; } };
    const livePlaced = id => { try { return root.Pool && root.Pool.sheetOf ? root.Pool.sheetOf(id) : null; } catch (_) { return null; } };
    const liveSheet = sid => { if (!sid) return null; for (const p of pages()) if (p.sheetId === sid) return p; return null; };

    function lineOfRow(r) {
      const sp = r.spec || {}, ln = r.line || {};
      return { key: r.key, transactionId: ln.transactionId, sku: sp.designSku || ln.sku || '', title: ln.title || '', material: r.material || sp.material || r.metal || null,
        quantity: sp.quantity || ln.quantity || 1, state: r.state, poolIds: r.poolIds || [], hold: r.hold || null, changePending: !!r.changePending, problems: r.problems || [], spec: { noDesign: sp.noDesign, customDone: sp.customDone || null }, reason: r.reason || '', listingId: ln.listingId || '' };
    }
    function linesOf(rid) {
      const pulled = root.Orders && root.Orders.rows ? (root.Orders.rows() || []).filter(r => String(r.order && r.order.receiptId) === rid && r.state !== 'gone') : [];
      if (pulled.length) return pulled.map(lineOfRow);
      const e = learned.get(rid); return e && e.rows ? e.rows.filter(r => r.state !== 'gone').map(lineOfRow) : [];
    }
    function inputFor(rid, only) {
      const e = learned.get(rid), lines = only ? only.map(lineOfRow) : linesOf(rid);
      const pools = new Map();
      for (const p of (e && e.pools) || []) pools.set(p.poolId, p);
      const mem = root.B && root.B.pool && root.B.pool.rows;
      // (this page's own pool rows, for the pieces its lines name: a hint, merged with what was read, a sheet id kept from either)
      const merge = (id, p) => { const cur = pools.get(id); pools.set(id, cur ? Object.assign({}, cur, p, { sheetId: p.sheetId || cur.sheetId || null }) : p); };
      if (mem) for (const l of lines) {
        for (let c = 1; c <= Math.max(1, Math.round(+l.quantity || 1)); c++) { const id = `${l.key}_${c}`, p = mem.get(id); if (p) merge(id, p); }
        for (const id of l.poolIds || []) { const p = mem.get(String(id)); if (p) merge(String(id), p); }
      }
      // sheets: what the page holds (Library rows) and what was read for the order, newest copy of each
      const L = libIndex(), sheets = new Map();
      const take = s => { const id = sidOf(s); if (!id) return; const c = sheets.get(id); if (!c || stampOf(s) >= stampOf(c)) sheets.set(id, s); };
      for (const s of L.byOrder.get(rid) || []) take(s);
      for (const l of lines) for (let c = 1; c <= Math.max(1, Math.round(+l.quantity || 1)); c++) for (const s of L.byPool.get(`${l.key}_${c}`) || []) take(s);
      for (const s of (e && e.sheets) || []) take(s);
      return { orderId: rid, lines, pools: [...pools.values()], sheets: [...sheets.values()], sheetsKnown: !!(e && e.ok), failed: !!(e && !e.ok && e.failed), livePlaced, liveSheet,
        thumbOf: id => { try { const c = root.Pool && root.Pool.charmOf ? root.Pool.charmOf(id) : null; return c && c.thumbUrl || null; } catch (_) { return null; } } };
    }
    function enrich(pieces) {
      // a thumbnail already fetched for the listing (never fetched here)
      const LM = root.ListMedia; if (LM && LM.peek) for (const p of pieces) if (!p.thumb && p.listingId) { try { p.thumb = LM.peek(p.listingId) || null; } catch (_) {} }
      return pieces;
    }
    function of(orderId) { const rid = ridOf(orderId); if (!rid) return []; return enrich(resolve(inputFor(rid))); }
    const ofLine = (orderId, lineKey) => of(orderId).filter(p => p.lineKey === lineKey);
    /** The pieces of ONE order line (a row of the pull or of the order view), without reading the order's other lines: cheap enough for a list row. */
    function ofRow(row) { const rid = row && row.order ? ridOf(row.order.receiptId) : ''; return rid ? enrich(resolve(inputFor(rid, [row]))).filter(p => p.lineKey === row.key) : []; }
    const nestedOfRow = row => { const ps = ofRow(row); return ps.length > 0 && ps.every(p => p.nested); };
    function nestedOf(orderId, lineKey) { const ps = lineKey ? ofLine(orderId, lineKey) : of(orderId); return ps.length > 0 && ps.every(p => p.nested); }
    const spreadOf = orderId => spread(of(orderId));
    const known = orderId => { const e = learned.get(ridOf(orderId)); return !!(e && e.ok); };

    function recordOfSheet(sheetId) {
      const L = libIndex(); let rec = L.bySheet.get(sheetId) || fullSheets.get(sheetId) || null;
      if (!rec) for (const e of learned.values()) { const s = e.sheets.find(x => sidOf(x) === sheetId); if (s) { rec = s; break; } }
      const live = liveSheet(sheetId);
      return { rec, live };
    }
    /** Every order with a piece on this saved sheet: [{ orderId, pieces }] (pieces on THIS sheet). */
    function onSheet(sheetId) {
      if (!sheetId) return [];
      const { rec, live } = recordOfSheet(sheetId), ids = new Set();
      for (const id of (rec && rec.poolIds) || []) ids.add(String(id));
      if (live) for (const c of live.charms || []) if (c.poolId && livePlaced(c.poolId) === live) ids.add(String(c.poolId));
      const rids = [...new Set([...ids].map(id => id.split('_')[0]).filter(x => /^\d+$/.test(x)))];
      return rids.map(orderId => ({ orderId, pieces: of(orderId).filter(p => p.sheetId === sheetId) })).filter(x => x.pieces.length);
    }
    /** Every order with a piece on a sheet of this set: [{ orderId, pieces }] (the pieces that sit on the set's sheets). */
    function ordersOf(setId) {
      if (!setId) return [];
      const ids = new Set();
      for (const s of libIndex().bySet.get(setId) || []) ids.add(sidOf(s));
      for (const e of learned.values()) for (const s of e.sheets) if (s.setId === setId) ids.add(sidOf(s));
      for (const p of pages()) if (p.setId === setId && p.sheetId) ids.add(p.sheetId);
      const rows = new Map();
      for (const sid of ids) for (const x of onSheet(sid)) { const cur = rows.get(x.orderId); if (cur) cur.pieces.push(...x.pieces); else rows.set(x.orderId, { orderId: x.orderId, pieces: x.pieces.slice() }); }
      return [...rows.values()];
    }

    /** One read of the sheet records and pool rows of these orders (op getOrderPieces); a fresh answer is reused for 30 s. */
    function load(orderIds, opts = {}) {
      const rids = [...new Set((Array.isArray(orderIds) ? orderIds : [orderIds]).map(ridOf).filter(Boolean))];
      const api = root.CN && root.CN.api; if (!api || !rids.length) return Promise.resolve(false);
      const want = rids.filter(r => { const e = learned.get(r); return opts.force || !e || !(e.ok && Date.now() - e.at < 30000); });
      const waiting = rids.map(r => learned.get(r) && learned.get(r).task).filter(Boolean);
      const fresh = want.filter(r => !(learned.get(r) && learned.get(r).task));
      if (!fresh.length) return waiting.length ? Promise.all(waiting).then(() => true) : Promise.resolve(true);
      const task = (async () => {
        for (let i = 0; i < fresh.length; i += 30) {
          const part = fresh.slice(i, i + 30);
          try {
            const res = await api('charmNestLibrary', { op: 'getOrderPieces', orderIds: part }, { quiet: true, timeoutMs: 15000 });
            for (const r of part) { const x = (res.orders || {})[r] || {}, e = entry(r); e.pools = x.pools || []; e.sheets = x.sheets || []; e.at = Date.now(); e.ok = true; e.failed = null; }
          } catch (err) { for (const r of part) { const e = entry(r); e.failed = err; e.at = Date.now(); } console.warn('OrderPieces: sheet records not read', err && err.message); }
        }
        for (const r of fresh) { const e = learned.get(r); if (e) e.task = null; }
        bump();
        return fresh.every(r => learned.get(r) && learned.get(r).ok);
      })();
      for (const r of fresh) entry(r).task = task;
      return Promise.all([task, ...waiting]).then(([ok]) => ok);
    }
    /** Reads a saved sheet whole (every piece on it, so every order on it) and then the sheet records of those orders: when it resolves,
     *  onSheet(sheetId) and the spread of each order on it are complete. For a sheet the Library rows do not hold (a Completed one). */
    async function loadSheet(sheetId) {
      const api = root.CN && root.CN.api; if (!api || !sheetId) return false;
      try {
        const res = await api('charmNestLibrary', { op: 'getOrderPieces', sheetIds: [sheetId] }, { quiet: true, timeoutMs: 15000 });
        const rec = (res.sheets || {})[sheetId]; if (!rec) return false;
        fullSheets.set(sheetId, rec);
        const rids = [...new Set(rec.poolIds.map(id => id.split('_')[0]).filter(x => /^\d+$/.test(x)))];
        const ok = await load(rids); bump(); return ok;
      } catch (e) { console.warn('OrderPieces: sheet not read', e && e.message); return false; }
    }
    /** The order lines the order window built from the records (an order outside the pull). */
    function learn(orderId, data) { const rid = ridOf(orderId); if (!rid || !data) return; const e = entry(rid); if (data.rows) e.rows = data.rows; if (data.pools) { e.pools = data.pools; } if (data.sheets) { e.sheets = data.sheets; if (data.sheetsKnown) { e.ok = true; e.at = Date.now(); } } bump(); }
    const forget = orderId => { learned.delete(ridOf(orderId)); bump(); };

    return { of, spread: spreadOf, ofLine, ofRow, nestedOfRow, nestedOf, onSheet, ordersOf, known, load, loadSheet, learn, forget, subscribe: fn => { subs.add(fn); return () => subs.delete(fn); }, version: () => version, _resolve: resolve, _learned: learned };
  }
  return { CODE, resolve, spread, makePage, labelOf, sheetNo, problemOf, handOf, REASON };
});
