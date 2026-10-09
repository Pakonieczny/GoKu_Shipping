/* SharedOrders: the cardinal rule of a Set of Sheets (Paul, 5 Oct 2026, round 2 point 6).
 *
 *   "If any order in a given sheet has pieces that are also part of other sheets, that automatically means those sheets
 *    must become part of the same set of sheets. All Sheets that share multi-piece orders must always be in the same Set
 *    of Sheets. If a user tries to drag a sheet out of a given set of sheets and by doing so would violate the cardinal
 *    rules ... that sheet should not be allowed to be moved out of the set and the user should be clearly informed as to
 *    why, and which orders are preventing this. A user should be able to manually remove the offending orders that are
 *    mixed, and have those be immediately updated in real time everywhere in the application."
 *
 * One file, three parts:
 *   1. a pure core (CharmNestSharedOrders in node, SharedOrders.core in the page; the server requires it too, so the
 *      page and charmNestLibrary.js answer the rule with the same code) over normalised sheets:
 *        { id, label, metal, setId|null, fixed:''|'why it cannot change set', pieces:[{key, orderId}] }
 *      A piece is one charm copy (its pool id, `order_transaction_copy`), as everywhere in the app. An order is SHARED when
 *      more than one distinct piece of it sits on sheets and those sheets are two or more; an order whose pieces are all
 *      on one sheet, and an order with a single piece, never are.
 *   2. the page's side (window.SharedOrders): the answer from what the page holds (the Library's rows, the open run's
 *      pages, window.OrderPieces when it is there), synchronously and without any network call.
 *   3. removeFromSheet: the person's explicit choice to take an order off, through the sheet window's own "Take off the
 *      sheet" flows (SheetWin.takeOffOrder), then every surface is told at once.
 *
 *   SharedOrders.between(sheetIdOrSetId, targetSetIdOrNull, opts?)  -> [item]   the multi-piece orders that would be split
 *   SharedOrders.groupOf(sheetId)                                   -> { ids, labels, members:[{id,label,setId}], orders, sets }   the sheets that must stay together
 *   SharedOrders.removeFromSheet({orderId, sheetId, mode:'hold'|'cancel', by, note?, scope?}) -> Promise<{ok, error?, ...}>
 *   SharedOrders.subscribe(fn) -> off        fn() after a removal and whenever the Library's live read changed a card
 *   item = { orderId, label, customer, thumb, total, ids:[poolIds], here:'GF Sheet 1', there:['SS Sheet 1'], hereIds, thereIds,
 *            pieces:[{key,index,label,sku,sheetId,sheetLabel,setId,thumb}], locked:[{sheetId,sheetLabel,why}] }
 *   (here: the sheet or sheets being moved that hold the order; there: the sheets that would stay on the other side;
 *    locked: sheets among them that cannot give the order's pieces up at all: cut, or in a set sent to the station) */
(function (root, factory) { const api = factory(root); if (typeof module === 'object' && module.exports) module.exports = api; else { root.SharedOrders = api; root.CharmNestSharedOrders = api.core; } })(typeof self !== 'undefined' ? self : this, function (root) {
  'use strict';
  const CODE = { gold: 'GF', silver: 'SS', rose: 'RG', gold10k: '10K', gold14k: '14K' };
  const orderOfKey = k => (/^(\d{1,30})_/.exec(String(k || '')) || [])[1] || '';
  /** A pool id "receipt_transaction_copy" as its parts; groupKey (receipt:transaction) is shared by every piece of one order line. null for any other id. */
  const parseKey = k => { const m = /^(\d{1,30})_([^_]*)_(\d{1,4})$/.exec(String(k || '')); return m ? { orderId: m[1], transactionId: m[2], copy: +m[3], groupKey: m[1] + ':' + m[2] } : null; };
  const sideOk = s => (s === 'L' || s === 'R') ? s : null;
  const sideWord = s => s === 'L' ? 'left' : s === 'R' ? 'right' : '';
  const sideLabel = s => s === 'L' ? 'Left' : s === 'R' ? 'Right' : '';
  const sheetLabelOf = r => `${CODE[r.metal] || r.metalLabel || ''} Sheet ${r.sheetIndex || r.page || 1}`.trim();
  const setLabelOf = s => s && (s.seq || s.setSeq) ? `Set ${s.seq || s.setSeq}` : (s && s.name) || 'its set';
  // a sheet is in a set when it has one and is neither a draft nor left out of it (Readiness.sheet reads the same fields)
  const effectiveSet = r => (r && r.setId && !r.draft && r.solidIncluded !== false) ? String(r.setId) : null;
  const committedSet = d => !!(d && (+d.committedAt > 0 || /^complete/.test(String(d.status || ''))));

  /** Why a sheet cannot change set at all (cut, completed, or in a set sent to the station); '' when it can. */
  function fixedWhy(rec, set) {
    if (!rec) return '';
    if (rec.roseCutAt) return 'its Rose Gold cut is recorded';
    if (+rec.laserDoneAt > 0) return 'it was marked completed';
    if (set && committedSet(set)) return `${setLabelOf(set)} was committed to the Design Station`;
    return '';
  }
  /** A saved record (a Library row, a sheet document) as the core reads it. null for an archived or nameless one. */
  function sheetOf(rec, o = {}) {
    if (!rec || rec.archived) return null;
    const id = String(rec.id || rec.sheetId || ''); if (!id) return null;
    const keys = [...new Set((Array.isArray(rec.poolIds) ? rec.poolIds : []).map(String).filter(Boolean))];
    // a record may say which side a piece is (pieceSides: { poolId: 'L'|'R' }). A piece is still its pool id (an earring pair is two ids, a Left and a Right, matching or not);
    // only when sheets list ONE id with two different sides does index() tell the two bodies apart (a record without sides: the id alone, exactly as before)
    const sides = rec.pieceSides && typeof rec.pieceSides === 'object' && !Array.isArray(rec.pieceSides) ? rec.pieceSides : null;
    let pieces = keys.map(k => { const side = sides ? sideOk(sides[k]) : null, g = parseKey(k); return { key: k, orderId: orderOfKey(k), ...(g ? { groupKey: g.groupKey } : {}), ...(side ? { side } : {}) }; }).filter(p => p.orderId);
    // a record that lists orders and no pieces (an old one): one unknown piece per order and sheet
    if (!pieces.length) pieces = [...new Set((Array.isArray(rec.orders) ? rec.orders : []).map(String).filter(x => /^\d{1,30}$/.test(x)))].map(oid => ({ key: `${oid}~${id}`, orderId: oid, unknown: true }));
    return { id, label: o.label || sheetLabelOf(rec), metal: rec.metal || '', setId: o.setId !== undefined ? o.setId : effectiveSet(rec), runId: rec.runId || null, fixed: o.fixed || '', pieces };
  }

  /* ── the rule over sheets ───────────────────────────────────────────────────────────────────────────────────── */
  // orderId -> Map(sheetId -> Map(pieceKey -> piece))
  function index(sheets) {
    const by = new Map(), sidesOf = new Map();
    // the sides each pool id is listed with, across all sheets: an id listed with BOTH a Left and a Right is two bodies (never the case for an id listed with one side, or none: that is one piece, stale copies included)
    for (const s of sheets) for (const p of s.pieces) if (p.side) { let t = sidesOf.get(p.key); if (!t) sidesOf.set(p.key, t = new Set()); t.add(p.side); }
    for (const s of sheets) for (const p of s.pieces) {
      let m = by.get(p.orderId); if (!m) by.set(p.orderId, m = new Map());
      let k = m.get(s.id); if (!k) m.set(s.id, k = new Map());
      const two = p.side && sidesOf.get(p.key).size > 1, q = two ? Object.assign({}, p, { uid: p.key + '#' + p.side }) : p;
      k.set(q.uid || q.key, q);
    }
    return by;
  }
  const distinctPieces = m => { const all = new Set(); for (const k of m.values()) for (const key of k.keys()) all.add(key); return all; };
  /** The order is shared: more than one distinct piece of it, on two or more sheets. */
  const spans = m => m.size >= 2 && distinctPieces(m).size > 1;

  /** The sheets that share a multi-piece order, as connected groups (union-find): [{ ids, orders }], groups of two or more. */
  function groups(sheets) {
    const by = index(sheets), parent = new Map(sheets.map(s => [s.id, s.id]));
    const find = x => { let r = x; while (parent.get(r) !== r) r = parent.get(r); while (parent.get(x) !== r) { const n = parent.get(x); parent.set(x, r); x = n; } return r; };
    const shared = [];
    for (const [oid, m] of by) if (spans(m)) { shared.push(oid); const ids = [...m.keys()]; for (let i = 1; i < ids.length; i++) { const a = find(ids[0]), b = find(ids[i]); if (a !== b) parent.set(a, b); } }
    const comp = new Map();
    for (const s of sheets) { const r = find(s.id); let c = comp.get(r); if (!c) comp.set(r, c = { ids: [], orders: new Set() }); c.ids.push(s.id); }
    for (const oid of shared) comp.get(find([...by.get(oid).keys()][0])).orders.add(oid);
    return [...comp.values()].filter(c => c.ids.length > 1).map(c => ({ ids: c.ids, orders: [...c.orders].sort() }));
  }
  /** The group one sheet belongs to: { ids, orders } (just itself when it shares nothing). */
  function groupOf(sheets, id) { return groups(sheets).find(g => g.ids.includes(id)) || { ids: [id], orders: [] }; }

  /**
   * The multi-piece orders that would be split if the sheets `moving` (ids; they travel together) end up in set `dest`
   * (a set id, 'new' for a set not made yet, or null for no set) while every other sheet stays where it is. An order is
   * split when it has pieces on a moving sheet and on a sheet that stays in another set (or in none, when `dest` is a set).
   * → items, in order-number order; [] when nothing would be separated.
   */
  function between(sheets, moving, dest, o = {}) {
    const mv = new Set([].concat(moving || []).map(String)), byId = new Map(sheets.map(s => [s.id, s])), by = index(sheets), out = [];
    const target = dest === undefined || dest === '' ? null : dest;
    for (const [oid, m] of [...by].sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0))) {
      if (!spans(m)) continue;
      const here = [...m.keys()].filter(id => mv.has(id));
      if (!here.length) continue;
      const there = [...m.keys()].filter(id => !mv.has(id) && (byId.get(id).setId || null) !== target);
      if (!there.length) continue;
      const all = [...distinctPieces(m)].sort(), nth = new Map(all.map((k, i) => [k, i + 1]));
      const involved = here.concat(there), pieces = [];
      for (const sid of involved) { const s = byId.get(sid); for (const p of m.get(sid).values()) pieces.push({ key: p.key, index: nth.get(p.uid || p.key), label: `Piece ${nth.get(p.uid || p.key)}`, sheetId: sid, sheetLabel: s.label, setId: s.setId || null, ...(p.groupKey ? { groupKey: p.groupKey } : {}), ...(p.side ? { side: p.side } : {}) }); }
      pieces.sort((a, b) => a.index - b.index || (a.sheetLabel < b.sheetLabel ? -1 : 1));
      out.push({ orderId: oid, label: `Order ${oid}`, customer: '', thumb: null, total: all.length, ids: pieces.map(p => p.key),
        here: here.map(id => byId.get(id).label).join(' + '), there: there.map(id => byId.get(id).label), hereIds: here, thereIds: there,
        pieces, locked: involved.filter(id => byId.get(id).fixed).map(id => ({ sheetId: id, sheetLabel: byId.get(id).label, why: byId.get(id).fixed })) });
    }
    return describe(out, o.meta);
  }

  /* ── what the pieces ARE (Paul, 9 Oct: pairs, mismatched pairs and disc necklaces): the words a refusal or a plan says ──────────────────────────
   * The rule above counts pieces by ORDER (the number inside the pool id), never by design or SKU, so the two different charms of a mismatched pair,
   * the two of a matching pair and the n discs of a necklace are all counted. What follows only DESCRIBES an item (nothing here decides anything):
   *   item.kind   'mismatched' (a left and a right earring of two different designs) | 'pair' (a Left and a Right earring; two pieces of one line) | 'multi' (n discs, letters or other pieces) | 'single'/'' (not told)
   *   item.words  the verb phrase after "Order N": "has its left earring on GF Sheet 1 and its right earring on GF Sheet 2" (only when the item is told what its
   *               pieces are; without that it stays '' and every caller keeps the plain "has pieces on ..." words it always had)
   *   piece.side / piece.sideLabel / piece.groupKey, and item.groups [{ groupKey, kind, form, pieces:[key] }]
   * meta(poolKey) -> { side?, form?, kind?, discs? } | null is what the caller knows of a piece (the page: OrderPieces and the order rows; the server: the pool row).
   * A piece's own `side` (from the record or the page) always counts. Every earring pair is a Left and a Right piece (Paul, 9 Oct 18:47), matching or not, so a side alone never
   * says "mismatched": that comes from the caller's kind, or from the two pieces being two different bodies (bodyIndex 0 and 1). */
  const EAR_FORM = /earring|stud|hoop|huggie|pair/i;
  const listWords = l => l.length <= 1 ? (l[0] || '') : `${l.slice(0, -1).join(', ')} and ${l[l.length - 1]}`;
  function describe(items, meta) {
    for (const it of items || []) {
      const by = new Map();
      for (const p of it.pieces || []) {
        const m = typeof meta === 'function' ? (() => { try { return meta(p.key) || null; } catch (_) { return null; } })() : null, g = parseKey(p.key);
        if (!p.side && m && sideOk(m.side)) p.side = m.side;
        if (!p.groupKey && g) p.groupKey = g.groupKey;
        if (p.side) p.sideLabel = sideLabel(p.side);
        if (m && m.form && !p.form) p.form = String(m.form);
        if (m && m.kind && !p.kind) p.kind = String(m.kind);
        if (m && m.discs) p.discs = true;
        if (m && m.sku && !p.sku) p.sku = String(m.sku);
        if (m && m.bodyIndex != null && p.bodyIndex == null && Number.isFinite(+m.bodyIndex)) p.bodyIndex = +m.bodyIndex;
        const k = p.groupKey || `${it.orderId}:`;
        let grp = by.get(k); if (!grp) by.set(k, grp = { groupKey: k, kind: '', form: '', pieces: [], sides: 0, discs: false, bodies: new Set() });
        grp.pieces.push(p); if (p.side) grp.sides++; if (p.discs) grp.discs = true; if (!grp.form && p.form) grp.form = p.form; if (!grp.kind && p.kind) grp.kind = p.kind;
        if (p.bodyIndex != null) grp.bodies.add(p.bodyIndex);
      }
      const groups = [...by.values()];
      for (const g of groups) {
        if (g.kind === 'single') g.kind = '';       // (one piece says nothing about the others: an order of single pieces keeps the plain words)
        if (g.bodies.size > 1) g.kind = 'mismatched';                  // (two different bodies in one line: a left and a right of different designs)
        else if (g.sides) g.kind = g.kind === 'mismatched' ? 'mismatched' : 'pair';     // (a Left and a Right piece of an earring pair, matching or not)
        else if (g.kind === 'pair' && !(g.form && EAR_FORM.test(g.form))) g.kind = 'multi';     // (two pieces of a line that is not known to be earrings are "pieces", never "earrings")
        else if (!g.kind) g.kind = g.form && EAR_FORM.test(g.form) && g.pieces.length === 2 ? 'pair' : g.discs && g.pieces.length > 1 ? 'multi' : '';
      }
      it.groups = groups.map(g => ({ groupKey: g.groupKey, kind: g.kind, form: g.form, ...(g.discs ? { discs: true } : {}), pieces: g.pieces.map(p => p.key) }));
      const kinds = [...new Set(groups.map(g => g.kind))];
      it.kind = groups.length === 1 || kinds.length === 1 ? kinds[0] : groups.length ? 'multi' : '';
      it.words = wordsOf(it, groups);
    }
    return items;
  }
  const placeOf = p => p.sheetLabel || 'no sheet yet';
  const earWord = g => /huggie/i.test(g.form || '') ? 'huggie' : 'earring';
  /** The verb phrase for one item, '' when it is not told what its pieces are. One line (one group of pieces): where each piece is. An order of several lines (a mixed order:
   *  studs, huggies, discs, a single): what it holds and the sheets it is spread over, in one short line. */
  function wordsOf(it, groups) {
    if (!groups.some(g => g.kind)) return '';
    const sort = ps => ps.slice().sort((a, b) => (a.side === 'L' ? 0 : a.side === 'R' ? 1 : 2) - (b.side === 'L' ? 0 : b.side === 'R' ? 1 : 2) || a.index - b.index);
    if (groups.length === 1) {
      const g = groups[0], ps = sort(g.pieces), sheets = [...new Set(ps.map(placeOf))];
      if (g.kind === 'mismatched' || (g.kind === 'pair' && ps.some(p => p.side))) return `has ${listWords(ps.map(p => `its ${p.side ? sideWord(p.side) + ' ' : ''}${earWord(g)} on ${placeOf(p)}`))}`;
      if (g.kind === 'pair') return `has its two ${earWord(g)}s on ${listWords(sheets)}`;
      if (g.kind === 'multi') return `has its ${ps.length} ${g.discs ? 'discs' : 'pieces'} on ${listWords(sheets)}`;
      return `has ${ps.length === 1 ? 'a piece' : ps.length + ' pieces'} on ${listWords(sheets)}`;
    }
    const sheets = [...new Set(groups.flatMap(g => g.pieces.map(placeOf)))], slots = [], bySlot = new Map();
    const add = (key, one, more, n) => { let c = bySlot.get(key); if (!c) { c = { one, more, n: 0 }; bySlot.set(key, c); slots.push(c); } c.n += n; };
    for (const g of groups) {
      const n = g.pieces.length, e = earWord(g);
      if (g.kind === 'mismatched') add('m', 'mismatched pair', 'mismatched pairs', 1);
      else if (g.kind === 'pair') add('p', `pair of ${e}s`, `pairs of ${e}s`, 1);
      else if (g.kind === 'multi') slots.push({ text: `${n} ${g.discs ? 'discs' : 'pieces'}` });
      else add('s', 'piece', 'pieces', n);
    }
    const bits = slots.map(c => c.text || (c.n === 1 ? `a ${c.one}` : `${c.n} ${c.more}`));
    return `has ${listWords(bits)} across ${listWords(sheets)}`;
  }

  /**
   * The composition of the sets (what keeps every group in ONE set): for each group of sheets that share a multi-piece
   * order, which set it belongs to and which of its sheets still have to join it.
   *   pull:      [{ id, label, setId, because:[orderIds], with:[labels] }]  sheets that are in no set while a group mate is in `setId`
   *   conflicts: [{ ids, orders, sets:[setId...] }]                         a group already spread over two sets: nothing here may join them; the exact reason is told
   */
  function composition(sheets) {
    const byId = new Map(sheets.map(s => [s.id, s])), by = index(sheets), pull = [], conflicts = [];
    for (const g of groups(sheets)) {
      const sets = [...new Set(g.ids.map(id => byId.get(id).setId).filter(Boolean))];
      if (sets.length > 1) { conflicts.push({ ids: g.ids, orders: g.orders, sets }); continue; }
      if (sets.length === 1) for (const id of g.ids) {
        const s = byId.get(id); if (s.setId) continue;
        const mine = g.orders.filter(oid => by.get(oid).has(id)), mates = [...new Set(mine.flatMap(oid => [...by.get(oid).keys()]))].filter(x => x !== id);
        pull.push({ id, label: s.label, setId: sets[0], because: mine, with: mates.map(x => byId.get(x).label) });
      }
    }
    return { pull, conflicts };
  }
  /** A plain sentence for a list of items (a refusal, a log line): "Order 123 is also on SS Sheet 1." */
  function sentence(items, here) {
    if (!items.length) return '';
    const one = items.length === 1, there = [...new Set(items.flatMap(i => i.there))];
    if (one && items[0].words) return `Order ${items[0].orderId} ${items[0].words}: sheets that share a multi-piece order stay in the same set.`;
    return `${one ? 'Order ' + items[0].orderId + ' has pieces' : items.length + ' orders have pieces'} on ${there.join(', ')}${here ? ' and on ' + here : ''}: sheets that share a multi-piece order stay in the same set.`;
  }
  const core = { orderOfKey, parseKey, describe, sheetOf, sheetLabelOf, effectiveSet, fixedWhy, index, spans, groups, groupOf, between, composition, sentence, committedSet, CODE };

  /* ── the page's side ────────────────────────────────────────────────────────────────────────────────────────── */
  const hooks = {
    sheets: null,                 // () -> [normalised sheet] (tests, other pages)
    info: null,                   // (orderId) -> { customer, thumb }
    pieces: null,                 // (orderId) -> [{ key, label, sku, thumb }]
    employee: () => { try { return String((root.B && root.B.employee) || (root.localStorage && root.localStorage.getItem('cn.employee')) || '').trim(); } catch (_) { return ''; } },
    sets: null                    // (setId) -> set record (committedAt, status, seq) | null
  };
  const subs = new Set();
  let opHooked = false;
  function tell() { for (const f of [...subs]) { try { f(); } catch (e) { console.warn('SharedOrders listener', e); } } }
  const safe = (f, d) => { try { const v = f(); return v === undefined ? d : v; } catch (_) { return d; } };
  const libraryRows = () => safe(() => root.CN.S.library.rows, []) || [];
  const openPages = () => safe(() => root.CN.allSheets(), []) || [];
  function setDoc(id, runId) {
    if (hooks.sets) return safe(() => hooks.sets(id), null);
    return safe(() => (root.Sets && root.Sets.ofRun ? root.Sets.ofRun(runId) : []).find(s => s.setId === id) || null, null);
  }
  const pageId = sh => (sh && (sh.sheetId || `${sh.runId || 'run'}:${sh.metal}:${sh.page || 1}`)) || '';
  /** One page of the open run as the core reads it: only what is PLACED on it counts (a piece waiting in the queue is on no sheet).
   *  `saved` is the same sheet's Library row, when there is one: its label and (when nothing is placed yet) its pieces stand. */
  function fromPage(sh, saved) {
    if (!sh || sh.recalled) return null;
    const id = pageId(sh), byId = new Map((sh.charms || []).map(c => [c.id, c])), placed = new Set((sh.placements || []).map(p => p.id));
    const keys = [...placed].map(i => byId.get(i)).filter(Boolean).map(c => c.poolId).filter(Boolean);
    // the side of a placed piece of a mismatched pair (the page's charm says it: charm.side, or orderInfo.side), so the two bodies are two pieces whatever their ids
    const pieceSides = {}; for (const i of placed) { const c = byId.get(i), sd = c && sideOk(c.side || (c.orderInfo && c.orderInfo.side)); if (c && c.poolId && sd) pieceSides[c.poolId] = sd; }
    const solid = sh.metal === 'gold10k' || sh.metal === 'gold14k', picked = solid && root.Gate && root.Gate.solidSelected ? safe(() => root.Gate.solidSelected(sh.metal, sh), true) : true;
    const rec = { id, metal: sh.metal, page: sh.page, sheetIndex: sh.sheetIndex, setId: sh.setId, draft: !!sh.draft, solidIncluded: picked, poolIds: keys, runId: sh.runId, laserDoneAt: sh.laserDoneAt, roseCutAt: sh.roseCutAt, ...(Object.keys(pieceSides).length ? { pieceSides } : {}) };
    const s = sheetOf(rec, saved ? { label: saved.label } : {});
    if (!s) return null;
    const set = s.setId ? setDoc(s.setId, sh.runId) : null;
    s.fixed = fixedWhy(rec, set) || (saved && saved.fixed) || '';
    if (!keys.length && saved) s.pieces = saved.pieces;       // (a page that has not placed anything yet: the saved record's pieces stand)
    return s;
  }
  /** The sheets the page holds, as the core reads them: the Library's saved rows, then the open run's pages as they are now. */
  function pageSheets() {
    if (hooks.sheets) return hooks.sheets();
    const out = new Map();
    for (const r of libraryRows()) {
      const s = sheetOf(r, { fixed: undefined }); if (!s) continue;
      s.fixed = fixedWhy(r, s.setId ? setDoc(s.setId, r.runId) : null);
      out.set(s.id, s);
    }
    for (const sh of openPages()) {
      const s = fromPage(sh, out.get(pageId(sh)));
      if (s) out.set(s.id, s);
    }
    return [...out.values()];
  }
  const kindOf = (list, id) => list.some(s => s.id === id) ? 'sheet' : list.some(s => s.setId === id) ? 'set' : null;
  // person-facing words for an item: the order's customer and picture, each piece's name and picture (OrderPieces, else the order rows)
  function enrich(items) {
    if (!items.length) return items;
    const OP = root.OrderPieces, rows = safe(() => root.Orders.rows(), []) || [];
    const byOrder = new Map(); for (const r of rows) { const k = String(r && r.order && r.order.receiptId); let l = byOrder.get(k); if (!l) byOrder.set(k, l = []); l.push(r); }
    for (const it of items) {
      const rs = byOrder.get(it.orderId) || [], info = hooks.info ? safe(() => hooks.info(it.orderId), null) : null;
      it.customer = (info && info.customer) || safe(() => rs[0].order.buyer.name, '') || '';
      it.thumb = (info && info.thumb) || null;
      let known = hooks.pieces ? safe(() => hooks.pieces(it.orderId), null) : null;
      if (!known && OP && typeof OP.of === 'function') known = safe(() => OP.of(it.orderId), null);
      const byKey = new Map((Array.isArray(known) ? known : []).map(p => [String(p.key), p]));
      for (const p of it.pieces) {
        const k = byKey.get(p.key), row = rs.find(r => p.key.startsWith(String(r.key).replace(':', '_') + '_'));
        const sku = (k && (k.sku || k.label)) || safe(() => row.spec.designSku || row.line.sku, '') || '';
        if (k && k.label) p.label = k.label; else if (sku) p.label = sku;
        p.sku = sku || p.sku || '';
        p.thumb = (k && k.thumb) || safe(() => root.Orders.imageFor(row), null) || null;
        if (k && Number.isFinite(+k.index)) p.index = +k.index;
      }
      if (!it.thumb) it.thumb = (it.pieces.find(p => p.thumb) || {}).thumb || null;
      // what each piece IS (a left or right earring, one of two earrings, one of n discs): OrderPieces says it when it knows, else the order line's form
      describe([it], key => {
        const k = byKey.get(String(key)), row = rs.find(r => String(key).startsWith(String(r.key).replace(':', '_') + '_'));
        return { side: k && k.side, kind: k && k.kind, bodyIndex: k && k.bodyIndex, discs: !!(k && k.discs), form: (k && k.form) || safe(() => row.spec.form, '') || '', sku: (k && k.sku) || safe(() => row.spec.designSku, '') || '' };
      });
    }
    return items;
  }
  /** The sheets OrderPieces knows the moving sheets' orders sit on that this page does not list (a saved sheet of another run, one
   *  the Library has not drawn): added as sheets with just those pieces, so the page never answers "nothing" for want of a row. */
  function withKnown(list, moving) {
    const OP = root.OrderPieces; if (!OP || typeof OP.of !== 'function') return list;
    const by = new Map(list.map(s => [s.id, s])), orders = new Set();
    for (const id of moving) { const s = by.get(id); if (s) for (const p of s.pieces) orders.add(p.orderId); }
    const extra = new Map();
    for (const oid of orders) {
      const ps = safe(() => OP.of(oid), null); if (!Array.isArray(ps)) continue;
      for (const p of ps) {
        if (!p || !p.sheetId || by.has(p.sheetId)) continue;
        let s = extra.get(p.sheetId); if (!s) extra.set(p.sheetId, s = { id: p.sheetId, label: p.sheetLabel || p.sheetId, metal: p.metal || '', setId: p.setId || null, runId: null, fixed: '', pieces: [], cutAll: true });
        if (!s.pieces.some(x => x.key === p.key && (x.side || null) === (sideOk(p.side) || null))) s.pieces.push({ key: String(p.key), orderId: oid, ...(sideOk(p.side) ? { side: sideOk(p.side) } : {}) });
        if (p.state !== 'cut') s.cutAll = false;
      }
    }
    if (!extra.size) return list;
    for (const s of extra.values()) { s.fixed = fixedWhy(s.cutAll ? { laserDoneAt: 1 } : {}, s.setId ? setDoc(s.setId, null) : null); delete s.cutAll; }
    return list.concat([...extra.values()]);
  }
  /** between(idOrSetId, targetSetIdOrNull, {kind?, together?}) from what the page holds. Sync, pure, no network. */
  function pageBetween(id, targetSetId, opts = {}) {
    let list = pageSheets(); const id0 = String(id || ''), kind = opts.kind || kindOf(list, id0);
    if (!kind) return [];
    let moving = kind === 'set' ? list.filter(s => s.setId === id0).map(s => s.id) : [id0];
    list = withKnown(list, moving);
    if (opts.together) moving = [...new Set(moving.flatMap(x => groupOf(list, x).ids))];
    const dest = targetSetId ? String(targetSetId) : kind === 'set' ? id0 : null;
    return enrich(between(list, moving, dest === 'new' ? 'new' : dest));
  }
  function pageGroupOf(sheetId) {
    let list = pageSheets(); list = withKnown(list, [String(sheetId)]);
    const g = groupOf(list, String(sheetId)), by = new Map(list.map(s => [s.id, s]));
    return { ids: g.ids, labels: g.ids.map(i => (by.get(i) || {}).label || i), members: g.ids.map(i => ({ id: i, label: (by.get(i) || {}).label || i, setId: (by.get(i) || {}).setId || null })), orders: g.orders, sets: [...new Set(g.ids.map(i => (by.get(i) || {}).setId).filter(Boolean))] };
  }
  function pageComposition() { return composition(pageSheets()); }

  /* ── taking an order off (the person's explicit choice) ─────────────────────────────────────────────────────── */
  function changed() {
    try { root.LaserReview && root.LaserReview.changed && root.LaserReview.changed(); } catch (_) { /* drawn at the next read */ }
    try { root.LaserReview && root.LaserReview.nudge && root.LaserReview.nudge(); } catch (_) { /* the live read follows anyway */ }
    try { if (root.OrderWin && root.OrderWin.isOpen && root.OrderWin.isOpen() && root.OrderWin.nudge) root.OrderWin.nudge(); } catch (_) { /* its own poll follows */ }
    try { if (root.OrderPieces && root.OrderPieces.refresh) root.OrderPieces.refresh(); } catch (_) { /* read again at its next poll */ }
    try { if (root.CN && root.CN.loadLibrary && root.CN.S && root.CN.S.mode === 'library') Promise.resolve(root.CN.loadLibrary()).catch(() => {}); } catch (_) { /* the list is read at its next refresh */ }
    tell();
  }
  /**
   * removeFromSheet({orderId, sheetId, mode:'hold'|'cancel', by, note, scope}) or (orderId, sheetId, {mode, by, note, scope}).
   * Never runs by itself: the person chose Hold or Cancel. It is the sheet window's own "Take off the sheet" (SheetWin.takeOffOrder):
   *   scope 'order' (default)  the order's pieces come off every sheet that can give them up, and it waits On hold (or is cancelled)
   *   scope 'sheet'            only the pieces on `sheetId` come off, their lines wait On hold (a cancel is always the whole order). A line comes off WHOLE:
   *                            the other ear of a pair, or the other discs, on another sheet come off with it (SheetWin.offPlan, PairRemove); `removed` then also lists them, with `side` and `group`
   * A piece on a cut sheet, or in a set sent to the station, stays (answered in `stayed`, with the plain reason).
   */
  async function removeFromSheet(a, b, c) {
    const o = a && typeof a === 'object' ? { ...a } : { ...(c || {}), orderId: a, sheetId: b };
    const mode = o.mode === 'cancel' ? 'cancel' : o.mode === 'hold' ? 'hold' : '';
    if (!/^\d{4,}$/.test(String(o.orderId || ''))) return { ok: false, error: 'That is not an order number' };
    if (!mode) return { ok: false, error: 'Choose Hold or Cancel: an order is never taken off by itself' };
    const by = String(o.by || hooks.employee() || '').trim();
    if (!by) return { ok: false, error: 'Say who is making this change' };
    const SW = root.SheetWin;
    if (!SW || typeof SW.takeOffOrder !== 'function') return { ok: false, error: 'Taking an order off a sheet is not available on this page' };
    const scope = o.scope === 'sheet' ? 'sheet' : 'order';
    if (scope === 'sheet' && mode === 'cancel') return { ok: false, error: 'Only a whole order can be cancelled' };
    let r;
    try { r = await SW.takeOffOrder({ orderId: String(o.orderId), sheetId: o.sheetId || null, mode, by, note: String(o.note || '').slice(0, 200), scope }); }
    catch (e) { changed(); return { ok: false, error: (e && e.message) || String(e), scope }; }
    changed();
    return { ok: true, scope, ...(r || {}) };
  }

  const api = {
    between: pageBetween, groupOf: pageGroupOf, composition: pageComposition, removeFromSheet, changed,
    // fn() after a removal here, after the Library's live read applied a change (bridge.js calls refreshed), and when OrderPieces read something
    subscribe: fn => { subs.add(fn); if (!opHooked && root.OrderPieces && typeof root.OrderPieces.subscribe === 'function') { opHooked = true; try { root.OrderPieces.subscribe(tell); } catch (_) { opHooked = false; } } return () => subs.delete(fn); },
    refreshed: tell,
    sheets: pageSheets, fromPage, enrich, configure: o => { Object.assign(hooks, o || {}); return api; }, hooks, core
  };
  return api;
});
