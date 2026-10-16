/* The engraving of one PIECE of an order line (Paul, 9 Oct 2026: "whenever selling pairs of earrings, matching or mismatched ... even some
 * necklaces such as discs": every step tracks every piece).
 *
 * Until now the sorter kept ONE engraving job per order LINE: every copy of the line shared one text, one fit, one approval and one set of
 * seals. That is right for a matching pair (two copies of one charm, one engraving) and wrong for
 *   - a MISMATCHED pair: two different charm bodies, a left ear and a right ear. Each ear is fitted on its own body, can carry its own words
 *     ("Anna" on the left, "Ben" on the right), is approved on its own and has its own back file;
 *   - a necklace with several DISCS: each disc can carry its own letter.
 * Such a line is SPLIT: one job per SLOT. A slot is "L" or "R" (the ears) or "D1".."Dn" (the discs). Since 9 Oct 18:47 every piece of an earring pair
 * (matching or mismatched) carries side L or R and the Right is the Left mirrored, so a matching pair is split into a Left job and a Right job too (they
 * never share an approval). A single charm, a charm only, a line whose pieces carry no side: no slot, ONE job keyed by the line, byte for byte as before.
 * Old records (no side, no slot) read as they always did.
 * PIECES (Paul, 16 Oct 2026, order 4195059483 "CORGI 34728 · SS · ×2": "ALL multipiece orders must always show each individual piece with respect to all Engravings and Monograms"): a line of QUANTITY N >= 2 is cut
 * into its UNITS too. Unit 1 keeps the plain slot it always had (L, R, D1..Dn); unit u >= 2 is "<slot>.<u>" (L.2, R.2, D1.2); a unit with no ear or disc slot (a single charm, a bar) is "C<u>" (C1..CN, "Piece u of N").
 * A line of quantity 1 reads and writes exactly what it always did. /tmp/pc/CONTRACT-PIECES.md (plans/pieces-1016) is the full contract.
 *
 *   slotOfId(ctx, row, poolId)     -> "L" | "R" | "D<n>" | null   the piece's slot: its stored `side`, else derived (never written back)
 *   plan(ctx, row, existing)       -> { split, slots:[slot|null], of: Map(poolId -> slot|null) }   how a line's jobs are cut
 *   mirrorOfId(ctx, poolId, charm, hint) -> boolean  is the piece the mirror image of the drawing (the Right earring)?   pieceCharm(ctx, poolId, charm, hint) -> the charm as
 *       THIS piece is cut (CharmNestPair.pieceGeometry: outline, holes and art mirrored, text never reversed); the same object for a piece that is not mirrored
 *   jobKey(rowKey, slot, face) / parseKey(key) -> { rowKey, slot } (+ face: "front" for a front key only; a back key gives exactly what it always did)
 *   labelOf(slot) "Left" | "Right" | "Disc 2"  ·  earOf(slot) "Left ear" | "Right ear" | "Disc 2"  ·  tagOf(slot, of) "LEFT EAR" | "DISC 2 of 3"
 *   sideRow(ctx, parent, slot, job) -> a row for ONE slot: it inherits everything from the line's row (state, order, spec, line, hold: all
 *       live) and owns its key, its pool ids (only this slot's, live) and its engrave record (kept on the job, so a checkpoint keeps it)
 *   linkParent(ctx, parent, jobs)   -> the line's row `engrave` becomes the summary of its slots (read-only truth: worst state first,
 *       approved only when every slot is approved, `pieces[poolId]` = that piece's own decision); a write to it reaches every slot
 *   summary(ctx, legacy, jobs)      -> that summary, as plain data
 * FACES (plans/front-back-1011/CONTRACT.md, charm-nest-engrave-faces.js): a piece may have a FRONT job beside its back job (`job.face === 'front'`, key `<rowKey>[#slot]~front`).
 * Everything above is the BACK exactly as before; a front job is one more job of the line with its own side row, record and seals. A line with no front job reads and
 * writes the very same data as before this paragraph existed (the face fields below are only ever added, and the summary text/pieces are built by the old code).
 * ctx: { poolRow(poolId), charmOf(poolId), entryFor(sku), pair: CharmNestPair, mergeSeals(...records) }; every member optional.
 * Pure: no page, no network, no clock. Loads in the page (window.CharmNestEngraveSides) and in node. */
(function (root, factory) {
  const api = factory(root);
  if (typeof module === 'object' && module.exports) module.exports = api; else root.CharmNestEngraveSides = api;
})(typeof self !== 'undefined' ? self : this, function (root) {
  'use strict';
  const SLOT = /^(?:C\d{1,3}|(?:L|R|D\d{1,2})(?:\.\d{1,3})?)$/;
  const KEY = /^(.*)#(C\d{1,3}|(?:L|R|D\d{1,2})(?:\.\d{1,3})?)$/;
  const SLOT_PARTS = /^(?:C(\d{1,3})|(L|R|D(\d{1,2}))(?:\.(\d{1,3}))?)$/;
  const COPY = /_(\d{1,3})$/;
  const pairLib = ctx => (ctx && ctx.pair) || (root && root.CharmNestPair) || null;
  const dep = (name, file) => { const g = root && root[name]; if (g) return g; try { return typeof require === 'function' ? require(file) : null; } catch (_) { return null; } };
  const Faces = () => dep('CharmNestEngraveFaces', './charm-nest-engrave-faces.js');
  /** "front" | "back" of a job, a record or a key (absent = back). Without the face module every job is a back, as it was. */
  const faceOf = x => { const F = Faces(); return F ? F.faceOf(x) : 'back'; };
  const isFront = x => faceOf(x) === 'front';

  const jobKey = (rowKey, slot, face) => (slot ? `${rowKey}#${slot}` : String(rowKey)) + (face === 'front' ? '~front' : '');
  function parseKey(key) {
    let k = String(key == null ? '' : key), front = false;
    if (k.endsWith('~front')) { front = true; k = k.slice(0, -6); }
    const m = KEY.exec(k), out = m ? { rowKey: m[1], slot: m[2] } : { rowKey: k, slot: null };
    if (front) out.face = 'front';
    return out;
  }
  /** What a slot name says: { slot, kind: 'ear' | 'disc' | 'copy', base: 'L' | 'R' | 'D3' | 'C', side: 'L' | 'R' | null, n (the disc number, the copy number; 0 for an ear), unit (1-based) } or null. "D2.3" is disc 2 of the 3rd unit; "C2" copy 2. */
  function slotInfo(slot) {
    const s = String(slot == null ? '' : slot), m = SLOT_PARTS.exec(s); if (!m) return null;
    if (m[1]) return { slot: s, kind: 'copy', base: 'C', side: null, n: +m[1], unit: +m[1] };
    const unit = m[4] ? +m[4] : 1;
    return m[3] ? { slot: s, kind: 'disc', base: 'D' + (+m[3]), side: null, n: +m[3], unit } : { slot: s, kind: 'ear', base: m[2], side: m[2], n: 0, unit };
  }
  const isSlot = x => SLOT.test(String(x == null ? '' : x));
  const isCopy = slot => { const i = slotInfo(slot); return !!i && i.kind === 'copy'; };
  const baseSlot = slot => { const i = slotInfo(slot); return i ? i.base : ''; };
  const unitOf = slot => { const i = slotInfo(slot); return i ? i.unit : 1; };
  /** The side an ear slot names ("L" for "L" and "L.2"), else null. */
  const sideOfSlot = slot => { const i = slotInfo(slot); return i ? i.side : null; };
  /** The sort key of a slot, unit by unit (Left, Right, Disc 1..n, then the next unit); a copy slot is its unit; no slot or an unknown one last. */
  const slotRank = slot => { const i = slotInfo(slot); if (!i) return 9999; return (i.unit - 1) * 100 + (i.kind === 'ear' ? (i.side === 'L' ? 0 : 1) : i.kind === 'disc' ? 1 + i.n : 0); };
  /** The shape of a line from its slots: { units, per (pieces in a unit), total, kind: 'none' | 'ear' | 'disc' | 'copy', multi }. A line with no unit slot is one unit (multi false), exactly the old reading. */
  function shape(slots) {
    const list = [...new Set((slots || []).map(s => (s && typeof s === 'object' ? s.slot : s)).filter(isSlot))], infos = list.map(slotInfo);
    if (!infos.length) return { units: 1, per: 0, total: 0, kind: 'none', multi: false };
    const units = Math.max(...infos.map(i => i.unit)), copy = infos.some(i => i.kind === 'copy');
    const per = copy ? 1 : new Set(infos.map(i => i.base)).size;
    return { units, per, total: list.length, kind: copy ? 'copy' : infos[0].kind, multi: units > 1 };
  }
  const unitWord = (kind, unit) => (kind === 'ear' ? 'pair ' : 'set ') + unit;
  /** The short name of a slot: "Left" · "Disc 2" · "Piece 2"; a unit-qualified one says its unit: "Left · pair 2", "Disc 2 · set 2" (a slot of unit 1 only when the line's `shape` has more than one unit). */
  function labelOf(slot, shp) {
    const i = slotInfo(slot); if (!i) return '';
    if (i.kind === 'copy') return 'Piece ' + i.n;
    const base = i.kind === 'ear' ? (i.side === 'L' ? 'Left' : 'Right') : 'Disc ' + i.n, q = i.unit > 1 || (shp && shp.multi);
    return q ? base + ' · ' + unitWord(i.kind, i.unit) : base;
  }
  const earOf = (slot, shp) => { const i = slotInfo(slot); if (!i) return ''; if (i.kind !== 'ear') return labelOf(slot, shp); const base = i.side === 'L' ? 'Left ear' : 'Right ear'; return i.unit > 1 || (shp && shp.multi) ? base + ' · ' + unitWord('ear', i.unit) : base; };
  /** The chip of a piece: "LEFT EAR" · "DISC 2 of 3" · "PIECE 2 of 5" (`of`: the discs of a unit, the pieces of the line); unit-qualified: "LEFT EAR · PAIR 2", "DISC 2 of 3 · SET 2". */
  function tagOf(slot, of, shp) {
    const i = slotInfo(slot); if (!i) return '';
    if (i.kind === 'copy') return 'PIECE ' + i.n + (of > 1 ? ' of ' + of : '');
    const base = i.kind === 'ear' ? (i.side === 'L' ? 'LEFT EAR' : 'RIGHT EAR') : 'DISC ' + i.n + (of > 1 ? ' of ' + of : ''), q = i.unit > 1 || (shp && shp.multi);
    return q ? base + ' · ' + unitWord(i.kind, i.unit).toUpperCase() : base;
  }
  const copyOf = id => { const m = COPY.exec(String(id == null ? '' : id)); return m ? +m[1] : 0; };

  /** A pair of earrings whose design draws two different bodies: the master entry says so (`pair`), or the glued charm shows two bodies. */
  function mismatchedDesign(ctx, row, poolId) {
    const P = pairLib(ctx); if (!P) return false;
    // (not on a line that is no earring line: a necklace, pendant or charm of a two-body design is one piece per unit, never a Left and a Right ear: Paul, 10 Oct)
    try { if (row && row.spec && typeof P.plainLine === 'function' && P.plainLine({ spec: row.spec, form: row.spec.form })) return false; } catch (_) { /* decided below */ }
    const sku = row && row.spec && row.spec.designSku;
    // (nor on a line that makes ONE body of a design that draws the same outline twice, a single earring that names no ear: one charm, no ear, Paul 11 Oct)
    try { const e0 = ctx && ctx.entryFor && sku ? ctx.entryFor(sku) : null; if (e0 && typeof P.oneChoice === 'function' && typeof P.alikeDesign === 'function' && P.oneChoice({ spec: row.spec, form: row.spec.form }, e0) && P.alikeDesign(e0) === true) return false; } catch (_) { /* decided below */ }
    try { const dp = ctx && ctx.entryFor && sku ? P.designPair && P.designPair(ctx.entryFor(sku)) : null; if (dp) return !!dp.mismatched && dp.bodies === 2; } catch (_) { /* fall through to the charm */ }
    try { const ch = ctx && ctx.charmOf && poolId ? ctx.charmOf(poolId) : null; return !!(ch && ch.outline && P.isMismatched(ch)); } catch (_) { return false; }
  }
  /** How many pieces ONE unit of a counted-option line makes (discs, tags, charms of one necklace): the intake's own count first (spec.pieceCount over the quantity,
   *  never for an earring pair or a line it does not call a group: the one source of truth, charm-nest-orders.js countRead), else the words of the listing (discsOf).
   *  (DISCREAD: a "3 Tags" or "Set of 3 charms" necklace made 3 pieces but, found by the word "disc" alone, had ONE engraving job for all three.) */
  function perUnitOf(row) {
    const sp = row && row.spec, pr = sp && sp.pair, pc = Math.floor(+(sp && sp.pieceCount)), q = Math.max(1, Math.round(+(sp && sp.quantity) || +(row && row.line && row.line.quantity) || 1));
    if (!sp || !pr || pr.earring || pr.glued || pr.legacy || !(pc >= 2) || pr.kind !== 'multi' || pc % q) return 0;
    const per = pc / q; return per >= 2 && per <= 24 ? per : 0;
  }
  function discsOf(ctx, row) {
    const own = perUnitOf(row); if (own) return own;
    const P = pairLib(ctx); if (!P || !P.discsOf) return 0;
    try { return P.discsOf(Object.assign({}, row && row.line, { spec: row && row.spec })) || 0; } catch (_) { return 0; }
  }
  /** The slot of one piece. Its own `side` (PAIRPOOL's pool record, or the charm it was cut from) wins; else it is derived from the copy number
   *  (copy 1 = left, 2 = right, 3 = left ... for a mismatched design; copy n of d discs = disc ((n-1) mod d)+1). Never stored by this module. */
  function baseSlotOfId(ctx, row, poolId) {
    const rec = ctx && ctx.poolRow ? ctx.poolRow(poolId) : null;
    let ch = null; try { ch = ctx && ctx.charmOf ? ctx.charmOf(poolId) : null; } catch (_) { ch = null; }
    const side = (rec && rec.side) || (ch && ch.side);
    if (side === 'L' || side === 'R') return side;
    const n = copyOf(poolId); if (!n) return null;
    if (mismatchedDesign(ctx, row, poolId)) return (n - 1) % 2 === 0 ? 'L' : 'R';
    const d = discsOf(ctx, row);
    if (d > 1) return 'D' + (((n - 1) % d) + 1);
    return null;
  }
  /** How a line of quantity >= 2 is cut into units: { units, per (pieces in a unit), total } or null (quantity 1, or a piece count that is no whole number of units: the old, whole-line reading). */
  function unitShape(row) {
    const sp = row && row.spec, ln = row && row.line;
    const units = Math.max(1, Math.round(+(sp && sp.quantity) || +(ln && ln.quantity) || 1)); if (units < 2) return null;
    const pc = Math.floor(+(sp && sp.pieceCount)) || 0, ids = row && Array.isArray(row.poolIds) ? row.poolIds.length : 0, total = pc >= 1 ? pc : ids;
    if (!(total >= units) || total % units) return null;
    return { units, per: total / units, total };
  }
  const unitsOfRow = row => { const u = unitShape(row); return u ? u.units : 1; };
  /** The unit (1-based) a piece belongs to: copy n of a line of `per` pieces a unit. 1 for a line of quantity 1 or a piece with no copy number. */
  function unitOfId(row, poolId) {
    const u = unitShape(row), n = copyOf(poolId); if (!u || !n || n > u.total) return 1;
    return Math.floor((n - 1) / u.per) + 1;
  }
  /** The slot of one piece (see baseSlotOfId for the ears and discs). A line of quantity 1 gives exactly that. A line of quantity >= 2: unit 1 keeps the plain slot, unit u > 1 is "<slot>.<u>", and a unit with no slot of its own
   *  (a single charm, a bar) is "C<u>". A piece with no copy number keeps what it had. */
  function slotOfId(ctx, row, poolId) {
    const base = baseSlotOfId(ctx, row, poolId), u = unitShape(row);
    if (!u) {
      // (a line of quantity >= 2 pinned to the pieces it already has, a count the quantity does not divide: every piece it has is its own, as copies, unless the ears / discs already name it)
      const sp = row && row.spec, q = Math.round(+(sp && sp.quantity) || +(row && row.line && row.line.quantity) || 1), ids = row && Array.isArray(row.poolIds) ? row.poolIds.length : 0, k = copyOf(poolId);
      return !base && q >= 2 && ids >= 2 && k ? 'C' + k : base;
    }
    const n = copyOf(poolId); if (!n || n > u.total) return base;
    const unit = Math.floor((n - 1) / u.per) + 1;
    return base ? (unit === 1 ? base : base + '.' + unit) : 'C' + unit;
  }
  /** Is this piece the MIRROR IMAGE of the as-drawn master design? (Paul, 9 Oct 2026, 18:47: the Right earring is the Left turned 180 degrees about the
   *  vertical axis, so every stored earring side is L or R and one of them is the mirror.) The pool record says so (`mirror`); a piece that carries a
   *  side but no `mirror` asks the shared module which way the drawing faces (facingOf: null = symmetric or unknown, as-drawn is then the Left).
   *  Never guessed for a piece with no stored side, nor for the glued charm of a mismatched design (its two bodies face their own ways): an old line
   *  stays exactly as it was. `hint` is a saved back record or a job ({ side, mirror }). */
  function mirrorOfId(ctx, poolId, charm, hint) {
    const rec = ctx && ctx.poolRow ? ctx.poolRow(poolId) : null;
    if (rec && typeof rec.mirror === 'boolean') return rec.mirror;
    if (charm && typeof charm.mirror === 'boolean') return charm.mirror;
    if (hint && typeof hint.mirror === 'boolean') return hint.mirror;
    const side = (rec && rec.side) || (charm && charm.side) || (hint && hint.side);
    if (side !== 'L' && side !== 'R') return false;
    const P = pairLib(ctx);
    try { if (P && typeof P.facingOf === 'function' && charm && charm.outline && !(P.isMismatched && P.isMismatched(charm))) return side !== (P.facingOf(charm) || 'L'); } catch (_) { /* as drawn */ }
    return false;
  }
  /** The charm as THIS piece is cut: CharmNestPair.pieceGeometry gives the body this ear is (a mismatched design's left or right body alone, not the two
   *  glued) and, for the Right earring, its mirror image (outline, holes, hoop and engraving art mirrored). The fit, the back view and the back file are
   *  made on that. The engraved TEXT is never mirrored: it is fitted to this geometry and drawn as letters that read normally (the back-side flip that
   *  makes a back read from behind is a different operation and stays where it is). A single charm, a disc, a line with no side: the same object back.
   *  A charm that is already the mirrored one (`charm.mirrored`) is never mirrored twice. The pair module caches the result per charm, body and mirror,
   *  so a fit kept against it still matches after a wait. `hint`: { slot, side, mirror, bodyIndex } of a job or a saved back. */
  function pieceCharm(ctx, poolId, charm, hint) {
    if (!charm || charm.mirrored) return charm;
    const P = pairLib(ctx); if (!P || typeof P.pieceGeometry !== 'function') return charm;
    const rec = ctx && ctx.poolRow ? ctx.poolRow(poolId) : null;
    const side = (rec && rec.side) || charm.side || (hint && hint.side) || (hint && sideOfSlot(hint.slot)) || null;
    if (side !== 'L' && side !== 'R') return charm;
    const mirror = mirrorOfId(ctx, poolId, charm, hint);
    const bodyIndex = rec && rec.bodyIndex != null ? +rec.bodyIndex : hint && hint.bodyIndex != null ? +hint.bodyIndex : side === 'R' ? 1 : 0;
    let g = null;
    try { g = P.pieceGeometry(charm, { poolId, side, bodyIndex, mirror }); } catch (_) { g = null; }
    return g && g.outline ? g : charm;
  }
  const order = s => (slotInfo(s) ? slotRank(s) : 9999);   // (the old ranks for the old slots: L 0, R 1, D n 1+n; a unit-qualified slot after the unit before it; no slot last)
  /** How the line's engraving jobs are cut. `existing`: the slots jobs of this line already exist under (permanent: a job that exists keeps its slot).
   *  A line is split when its pieces fall into two or more slots, or when slotted jobs already exist. A legacy job already decided under the plain
   *  line key (approved, written, cut plain) keeps the line whole: an approval is never re-cut. */
  function plan(ctx, row, existing) {
    const ids = (row && Array.isArray(row.poolIds) ? row.poolIds : []).slice();
    const of = new Map(ids.map(id => [id, slotOfId(ctx, row, id)]));
    const have = (existing || []).filter(e => e && typeof e === 'object' && !isFront(e));   // (the slots are the back jobs' alone: a front job is a face of a piece, never a piece)
    const slotted = have.filter(e => e.slot), plain = have.find(e => !e.slot);
    // (PIECES: a job that exists keeps its slot. A copy job "C<n>" of a line that has since come down to one unit still holds its piece, so the piece stays "C<n>" and its approval stays on its job)
    if (slotted.some(e => isCopy(e.slot))) for (const id of ids) if (!of.get(id)) { const c = 'C' + copyOf(id); if (copyOf(id) && slotted.some(e => e.slot === c)) of.set(id, c); }
    const distinct = [...new Set([...of.values()].filter(Boolean))];
    // (PIECES: a line cut into UNITS, quantity >= 2: its slots are "C<u>" or "<slot>.<u>". A job decided under the plain line key keeps a line of quantity 1 whole, as it always did; it does NOT keep a line of several units whole:
    //  each unit is its own piece and the bridge hands every piece the decision (approval and seals) the whole line had, never asking twice)
    const byUnit = [...distinct, ...slotted.map(e => e.slot)].some(sl => { const i = slotInfo(sl); return !!i && (i.kind === 'copy' || i.unit > 1); });
    const legacyDecided = !!plain && ['approved', 'written', 'skipped'].includes(plain.state) && !slotted.length && !(byUnit && distinct.length >= 2);
    const split = !legacyDecided && (distinct.length >= 2 || slotted.length > 0);
    if (!split) return { split: false, slots: [null], of: new Map(ids.map(id => [id, null])) };
    const slots = [...new Set([...distinct, ...slotted.map(e => e.slot)])].sort((a, b) => order(a) - order(b));
    if ([...of.values()].some(s => !s)) slots.push(null);
    return byUnit ? { split: true, slots, of, byUnit: true } : { split: true, slots, of };
  }
  /** The pool ids of one slot, in the line's own order (live: read again each time). */
  const idsOfSlot = (ctx, row, slot) => (row && Array.isArray(row.poolIds) ? row.poolIds : []).filter(id => (slotOfId(ctx, row, id) || null) === (slot || null));

  /** A row for one slot. Everything not owned here is the line's own (prototype), so a cancelled or held line reads as gone/held for every slot. */
  function sideRow(ctx, parent, slot, job) {
    const row = Object.create(parent), face = job ? faceOf(job) : 'back';
    Object.defineProperties(row, {
      key: { value: jobKey(parent.key, slot, face), enumerable: true, writable: true, configurable: true },
      slot: { value: slot || null, enumerable: true, configurable: true },
      side: { value: sideOfSlot(slot), enumerable: true, configurable: true },   // ("L.2" is a Left ear too)
      parentRow: { value: parent, enumerable: false, configurable: true },
      poolIds: { get() { return slot ? idsOfSlot(ctx, parent, slot) : (parent && Array.isArray(parent.poolIds) ? parent.poolIds.slice() : []); }, enumerable: true, configurable: true },   // (no slot: the whole line's pieces, as the job under the line key always had; only a front job or the back of a line with a front gets such a row)
      engrave: { get() { const r = job.engraveRec; if (face === 'front' && r && typeof r === 'object' && r.face !== 'front') r.face = 'front'; return r; }, set(v) { job.engraveRec = v; }, enumerable: true, configurable: true }   // (a front's record says so: its approvedAt is a FRONT approval wherever it is merged into a seal list)
    });
    if (face === 'front') Object.defineProperty(row, 'face', { value: 'front', enumerable: true, configurable: true });   // (a back row has no `face`: the row is what it always was)
    return row;
  }

  const STATE_ORDER = ['reclassify', 'classify', 'words', 'blocked', 'ready', 'fitting', 'review', 'approved', 'written'];
  const settled = s => s === 'none' || s === 'skipped';
  /** "Front: W · Back: 1. 27. 24" for a line with a front job; per piece when the pieces differ: "Left (Front: W · Back: 1) · Right (Front: W · Back: 2)". */
  function textWithFaces(recs) {
    const by = new Map();
    for (const x of recs) {
      const t = String(x.rec.text || '').replace(/\s*\n\s*/g, ' / ').trim(); if (!t) continue;
      const slot = x.job.slot || ''; if (!by.has(slot)) by.set(slot, {});
      by.get(slot)[faceOf(x.job)] = t;
    }
    const slots = [...by.keys()].sort((a, b) => order(a || null) - order(b || null));
    if (!slots.length) return undefined;
    const one = e => ['front', 'back'].filter(f => e[f]).map(f => (f === 'front' ? 'Front' : 'Back') + ': ' + e[f]).join(' · ');
    const strs = slots.map(sl => one(by.get(sl)));
    return new Set(strs).size === 1 ? strs[0] : slots.map((sl, i) => (labelOf(sl) ? labelOf(sl) + ' (' + strs[i] + ')' : strs[i])).join(' · ');
  }
  /** The line's engraving as one record, from its slots' records. Approved only when every slot is; the state is the least advanced of the slots still
   *  needing a back; `pieces[poolId]` carries each piece's own decision (the readiness check reads it). Returns undefined when no slot has a record yet. */
  function summary(ctx, legacy, jobs) {
    const list = (jobs || []).filter(j => j && !j.archived), recs = list.map(j => ({ job: j, rec: j.engraveRec })).filter(x => x.rec);   // (an archived job is a piece the line no longer has: kept with its approvals, out of the line's summary)
    if (!recs.length) return legacy && Object.keys(legacy).length ? legacy : undefined;
    const shp = shape(list.map(j => j.slot));
    const needed = recs.filter(x => !settled(x.rec.state) && x.rec.needed !== false);
    const out = {};
    out.needed = recs.some(x => !!x.rec.needed);
    if (needed.length) out.state = needed.map(x => x.rec.state).sort((a, b) => (STATE_ORDER.indexOf(a) < 0 ? 99 : STATE_ORDER.indexOf(a)) - (STATE_ORDER.indexOf(b) < 0 ? 99 : STATE_ORDER.indexOf(b)))[0];
    else out.state = recs.every(x => x.rec.state === 'none') ? 'none' : 'skipped';
    out.approved = recs.length === list.length && recs.every(x => !!x.rec.approved);
    const anyFront = list.some(isFront);
    const texts = recs.map(x => ({ slot: x.job.slot, text: String(x.rec.text || '').replace(/\s*\n\s*/g, ' / ').trim() })).filter(x => x.text);
    out.text = anyFront ? textWithFaces(recs) : texts.length ? (new Set(texts.map(x => x.text)).size === 1 ? texts[0].text : texts.map(x => (labelOf(x.slot, shp) || 'All') + ': ' + x.text).join(' · ')) : undefined;
    const by = recs.map(x => x.rec.approvedBy).filter(Boolean); if (by.length) out.approvedBy = by[0];
    out.approvedAt = Math.max(0, ...recs.map(x => +x.rec.approvedAt || 0)) || undefined;
    out.decidedAt = Math.max(0, ...recs.map(x => +x.rec.decidedAt || 0)) || undefined;
    const reason = recs.map(x => x.rec.reason).filter(Boolean); if (reason.length && !out.approved) out.reason = reason[0];
    const merge = ctx && ctx.mergeSeals;
    const asFace = x => (isFront(x.job) && x.rec && typeof x.rec === 'object' && x.rec.face !== 'front' ? Object.assign({}, x.rec, { face: 'front' }) : x.rec);   // (each record says the face of ITS job: a front's approval is never a back seal in the line's list)
    out.seals = merge ? merge(legacy, ...recs.map(asFace)) : [].concat(...recs.map(x => x.rec.seals || []));
    const pieces = {};
    if (!anyFront) for (const x of recs) for (const id of x.job.copies || []) pieces[id] = { needed: !!x.rec.needed, state: x.rec.state, approved: !!x.rec.approved };
    else {
      // a piece with a front job: ONE combined decision (the old three fields, so readiness and the order gate stay right untouched), approved only when every face is, and the faces apart (additive)
      const by = new Map();
      for (const x of recs) for (const id of x.job.copies || []) { if (!by.has(id)) by.set(id, []); by.get(id).push({ face: faceOf(x.job), needed: !!x.rec.needed, state: x.rec.state, approved: !!x.rec.approved }); }
      const F = Faces();
      for (const [id, rs] of by) pieces[id] = F ? F.combine(rs) : { needed: !!rs[0].needed, state: rs[0].state, approved: !!rs[0].approved };
    }
    if (Object.keys(pieces).length) out.pieces = pieces;
    if (shp.multi) {   // (PIECES: a line of several units says how many of its pieces are approved, and which are not, worst first: additive, a line of one unit has none of these. A piece with no record yet counts: it is not approved)
      const by = new Map(); for (const j of list) { const k = j.slot || ''; if (!by.has(k)) by.set(k, []); by.get(k).push({ job: j, rec: j.engraveRec || { state: j.state || 'classify', approved: false } }); }
      const rank = st => (STATE_ORDER.indexOf(st) < 0 ? 99 : STATE_ORDER.indexOf(st));
      const open = [];
      for (const [k, xs] of by) { if (xs.every(x => !!x.rec.approved)) continue; const st = xs.map(x => x.rec.state).sort((a, b) => rank(a) - rank(b))[0]; open.push({ slot: k || null, poolId: (xs[0].job.copies || [])[0] || '', state: st }); }
      open.sort((a, b) => rank(a.state) - rank(b.state) || slotRank(a.slot) - slotRank(b.slot));
      out.n = by.size; out.done = by.size - open.length; out.open = open;
    }
    for (const k of Object.keys(out)) if (out[k] === undefined) delete out[k];
    return out;
  }
  /** The line's row reads and writes its `engrave` through its slots. A write (a whole new record) reaches every slot. */
  function linkParent(ctx, parent, jobs) {
    const had = Object.getOwnPropertyDescriptor(parent, 'engrave');
    if (had && had.get && parent._engraveLinked) { parent._engraveLinked.jobs = jobs; return parent; }
    const legacy = had && 'value' in had ? had.value : undefined;
    const link = { jobs, legacy };
    Object.defineProperty(parent, '_engraveLinked', { value: link, enumerable: false, configurable: true, writable: true });
    Object.defineProperty(parent, 'engrave', {
      enumerable: true, configurable: true,
      get() { return summary(ctx, link.legacy, link.jobs); },
      set(v) { link.legacy = v; for (const j of link.jobs) j.engraveRec = v && typeof v === 'object' ? Object.assign({}, v) : v; }
    });
    return parent;
  }
  /** Takes the line's row back to a plain record (the last slot is gone): its summary is written as data. */
  function unlinkParent(ctx, parent) {
    const link = parent._engraveLinked; if (!link) return parent;
    const value = summary(ctx, link.legacy, link.jobs);
    delete parent._engraveLinked;
    Object.defineProperty(parent, 'engrave', { value, writable: true, enumerable: true, configurable: true });
    return parent;
  }

  /* ── the buyer's words, one disc at a time (DISCREAD, Paul 10 Oct: "Tag 1: J, Tag 2: Q" is disc 1 = J, disc 2 = Q) ─────────────────────────────────────
     splitWords(texts, n) -> { ok, words: [n strings], how, why }. Deterministic, no AI, nothing guessed: it answers only when the note says plainly which words
     go on which disc, and otherwise says why not (the paid reader or a person decides, as before). A line of n discs gives exactly n words, disc 1 first.
       numbered     "Tag 1: J, Tag 2: Q" · "Disc 1: A; Disc 2: B" · "Initial 1 - J, Initial 2 - Q" · "1: J, 2: Q" · "1) J 2) Q" · "First disc: J, second: Q" · "Charm #2 = Q" (any order, each number once, 1..n)
       lines        n entries of the personalisation field, or n lines ("J" / "Q"), or n comma / semicolon / slash / ampersand separated words ("J, Q")
       letters      n single characters separated by spaces ("J Q")
       same         the buyer says it is the same on every disc ("All discs: J", "Same on all: J", "J on each disc"): the one word on every disc
     NOT answered (ok false): a count that differs from n, a number used twice or missing, one bare word with no sign it is for all discs, any other text around the markers. */
  const NOWORDS = /^(?:n\/?a|none|nil|no|nothing|not applicable|blank|-+|—+|\.+)$/i;   // (what a buyer writes when there is nothing to write)
  const NUMW = { one: 1, two: 2, three: 3, four: 4, five: 5, six: 6, seven: 7, eight: 8, nine: 9, ten: 10, eleven: 11, twelve: 12, first: 1, second: 2, third: 3, fourth: 4, fifth: 5, sixth: 6, seventh: 7, eighth: 8, ninth: 9, tenth: 10 };
  const NUMRX = '(\\d{1,2}(?:st|nd|rd|th)?|' + Object.keys(NUMW).join('|') + ')', UNITRX = '(?:tags?|discs?|disks?|charms?|pendants?|circles?|initials?|letters?|names?|words?|pieces?)';
  const numOfWord = w => { w = String(w || '').toLowerCase(); const d = /^(\d{1,2})(?:st|nd|rd|th)?$/.exec(w); return d ? +d[1] : NUMW[w] || 0; };
  const clean = x => String(x == null ? '' : x).replace(/[​-‍﻿]/g, '').replace(/\s+/g, ' ').trim();
  const trimWord = x => clean(x).replace(/^[\s,;|/&:\-–—]+/, '').replace(/(?:[\s,;|/&]|\band\b)+$/i, '').replace(/^(["'“‘])(.*)(["'”’])$/, '$2').trim();
  const LEAD = /^(?:please\s+)?(?:engrav\w*|initials?|letters?|names?|text|words?|personali[sz]ation|discs?|tags?)?\s*[:\-]?\s*$/i;
  function splitWords(texts, n) {
    n = Math.floor(+n);
    const fail = why => ({ ok: false, words: [], how: '', why });
    if (!(n >= 2)) return fail('fewer than two discs');
    const parts = (Array.isArray(texts) ? texts : [texts]).map(x => String(x == null ? '' : x).replace(/\r/g, '').split('\n').map(clean).filter(Boolean).join('\n')).filter(Boolean);
    if (!parts.length) return fail('no words written');
    const text = parts.join('\n');
    // same on every disc, said so
    const all = /^(?:(?:all|each|every|both)(?:\s+(?:of\s+(?:the\s+)?)?(?:discs?|tags?|charms?|pendants?))?|same(?:\s+(?:on|for)\s+(?:all|each|every|both)(?:\s+(?:discs?|tags?|charms?|pendants?))?)?)\s*[:=\-–]\s*(.+)$/i.exec(text) || /^(.+?)\s+(?:on|for)\s+(?:all|each|every|both)(?:\s+(?:of\s+(?:the\s+)?)?(?:discs?|tags?|charms?|pendants?))?\s*$/i.exec(text);
    if (all) { const w = trimWord(all[1]); return w ? { ok: true, words: Array.from({ length: n }, () => w), how: 'same', why: '' } : fail('the same words on every disc, but no words'); }
    // numbered markers
    const re = new RegExp('(^|[,;|/&(\\n]|\\s)\\s*(?:' + UNITRX + '\\s*#?\\s*' + NUMRX + '|' + NUMRX + '\\s*' + UNITRX + '|#?(\\d{1,2}|' + Object.keys(NUMW).join('|') + '))\\s*(?:[:=)\\-\\u2013\\u2014.]+)\\s*', 'gi');
    const marks = [];
    for (let m; (m = re.exec(text));) { const k = numOfWord(m[2] || m[3] || m[4]); marks.push({ k, at: m.index + m[1].length, end: m.index + m[0].length }); if (m[0].length === 0) re.lastIndex++; }
    if (marks.length) {
      const nums = marks.map(x => x.k);
      if (marks.length !== n || new Set(nums).size !== n || nums.some(k => k < 1 || k > n)) return fail(`the note numbers ${[...new Set(nums)].sort((a, b) => a - b).join(', ')}, the line has ${n} discs`);
      const lead = text.slice(0, marks[0].at); if (clean(lead) && !LEAD.test(clean(lead))) return fail('other words come before the numbered ones');
      const words = new Array(n).fill('');
      marks.forEach((x, i) => { words[x.k - 1] = trimWord(text.slice(x.end, i + 1 < marks.length ? marks[i + 1].at : text.length)); });
      return words.every(Boolean) ? { ok: true, words, how: 'numbered', why: '' } : fail('a numbered disc has no words');
    }
    // n entries of the field, or n lines
    const nonEmpty = list => list.map(trimWord).filter(Boolean);
    if (parts.length === n) { const w = nonEmpty(parts); if (w.length === n) return { ok: true, words: w, how: 'lines', why: '' }; }
    const lines = nonEmpty(text.split(/\n/));
    if (lines.length === n) return { ok: true, words: lines, how: 'lines', why: '' };
    const items = nonEmpty(text.replace(/\n/g, ' ').split(/\s*[,;|/&]\s*|\s+and\s+/i));
    if (items.length === n) return { ok: true, words: items, how: 'list', why: '' };
    const chars = clean(text).split(' ');
    if (chars.length === n && chars.every(c => Array.from(c).length === 1)) return { ok: true, words: chars, how: 'letters', why: '' };
    return fail(items.length === 1 && !/\s/.test(clean(text)) ? 'one word and no sign it goes on every disc' : `the note does not say which words go on which of the ${n} discs`);
  }

  /** The words of each disc of a counted line from its reading (a line spec: personalization, buyerMessage, staffNote, messages) and its slots ("D1".."Dn", in order), or null:
   *  the personalisation field must say it plainly (splitWords) and nobody else may have spoken (a buyer message, a staff note, an engraving-like message: the reader weighs them all).
   *  o.engravingNote(text) says whether a staff message is about the engraving. Returns { ok, words, how }. */
  function wordsForDiscs(spec, slots, o) {
    const sp = spec || {}, list = (slots || []).filter(Boolean);
    if (list.length < 2 || !list.every(x => /^D\d{1,2}$/.test(x))) return null;
    const note = o && typeof o.engravingNote === 'function' ? o.engravingNote : () => true;
    if (String(sp.staffNote || '').trim() || String(sp.buyerMessage || '').trim() || (sp.messages || []).some(m => note(m && m.text))) return null;
    const r = splitWords(sp.personalization || [], list.length);
    // SETBAR (13 Oct 2026): the two bars of a set a RULE names (spec.pair.set.source "rule", charm-nest-orders.js SET_RULES) are two necklaces, not discs of one: "coordinates / Ginger" in one box is NOT "bar 1 gets the coordinates, bar 2
    // gets Ginger". Only the words that name the bar by number ("Disc 1: ..") or say the same on both are read here; a list, lines or letters are left to the reader and a person, as the line was before.
    if (r.ok && sp.pair && sp.pair.set && sp.pair.set.source === 'rule' && r.how !== 'numbered' && r.how !== 'same') return null;
    return r.ok ? r : null;
  }

  /** The words of each piece of a line cut into UNITS (quantity >= 2: "C1".."Cn", or discs "D1.2"..), from the buyer's personalisation field alone, or null (PIECES, Paul 16 Oct 2026: "sometimes customers will request different
   *  engraving for each piece, even though the two pieces are the same"). Deterministic, no paid call: it answers only when the note says plainly which words go on which piece (splitWords: numbered, n lines, n words separated
   *  by , ; | / & and, n letters, "same on all"); anything else (a count that differs, one bare word, another voice on the order: a buyer message, a staff note) is null, and then EVERY piece gets the one reading of the whole line.
   *  Discs with units: the note is read as n discs (or as every piece, in order) and the words of a disc are repeated for every unit. Returns { ok, words: { [slot]: text }, how, sure }; sure: the note numbered the pieces, had one entry per
   *  line, or said it is the same on all (the job goes on to its fit); a plain list ("Mom & Dad") or letters is only a proposal for a person to confirm. */
  function wordsForPieces(spec, slots, o) {
    const sp = spec || {}, list = [...new Set((slots || []).filter(isSlot))];
    if (list.length < 2) return null;
    const note = o && typeof o.engravingNote === 'function' ? o.engravingNote : () => true;
    if (String(sp.staffNote || '').trim() || String(sp.buyerMessage || '').trim() || (sp.messages || []).some(m => note(m && m.text))) return null;
    const shp = shape(list), infos = list.map(slotInfo), texts = sp.personalization || [], words = {};
    // (the copies of one thing: the words-per-piece reader owns the parsing when it is on the page: charm-nest-engrave-piece-words.js forSlots; deferred / nothing to engrave / the whole text on every piece -> null: the line is read once and every piece gets the reading)
    const PW = shp.kind === 'copy' ? dep('CharmNestEngravePieceWords', './charm-nest-engrave-piece-words.js') : null;
    if (PW && typeof PW.forSlots === 'function') {
      const ordered = list.slice().sort((a, b) => slotRank(a) - slotRank(b)); let r = null; try { r = PW.forSlots(sp, ordered, o); } catch (_) { r = null; }
      if (!r || r.deferred || r.none || !r.bySlot) return null;
      if (!(r.split || (r.said && r.same))) return null;
      if (!ordered.every(sl => String(r.bySlot[sl] || '').trim())) return null;
      return { ok: true, words: Object.fromEntries(ordered.map(sl => [sl, String(r.bySlot[sl])])), how: r.said && r.same && !r.split ? 'same' : String(r.how || 'list'), sure: !!r.sure };
    }
    // (no words at all: "N/A", "none", "no": there is nothing to give any piece)
    if (NOWORDS.test(clean([].concat(texts).join(' ')))) return null;
    // (a plain list is a proposal only when it clearly is one: "Anna and Ben" is two names, "Mom and Dad forever" is one phrase; the items of a list joined by "and" are single words, and none runs past three words)
    const listOk = r0 => { if (!r0.ok || r0.how !== 'list') return r0.ok; const t = clean([].concat(texts).join(' ')), viaAnd = !/[,;|/&]/.test(t); return r0.words.every(w => w.split(/\s+/).length <= (viaAnd ? 1 : 3)); };
    let r = null;
    if (shp.kind === 'copy') {
      r = splitWords(texts, list.length); if (!r.ok || !listOk(r)) return null;
      for (const i of infos) words[i.slot] = r.words[i.n - 1];
    } else if (shp.kind === 'disc') {
      r = shp.multi ? splitWords(texts, shp.per) : { ok: false };
      if (r.ok && !listOk(r)) return null;
      if (r.ok) for (const i of infos) words[i.slot] = r.words[i.n - 1];
      else {
        r = splitWords(texts, list.length); if (!r.ok || !listOk(r)) return null;
        const ordered = infos.slice().sort((a, b) => slotRank(a.slot) - slotRank(b.slot));
        ordered.forEach((i, k) => { words[i.slot] = r.words[k]; });
      }
      // (SETBAR: the two bars of a set a RULE names are two necklaces: only words that name the bar by number or say the same on both are read, as wordsForDiscs)
      if (sp.pair && sp.pair.set && sp.pair.set.source === 'rule' && r.how !== 'numbered' && r.how !== 'same') return null;
    } else return null;
    if (!list.every(sl => words[sl])) return null;
    return { ok: true, words, how: r.how, sure: r.how === 'numbered' || r.how === 'lines' || r.how === 'same' };
  }

  /** EVERY piece of a line, whether anything is engraved on it or not (the Order window's "Its pieces", the Pieces list, a count): [{ poolId, n (copy number), unit, within (place inside its unit), slot, index (1-based over the line),
   *  of (pieces of the line), label, tag }] in piece order. The ids are the line's (row.poolIds, else ctx.previewIds(row), else the ids the line WILL get: <receipt>_<transaction>_<n>). A line of one piece: one entry, slot null, no label. */
  function pieceList(ctx, row) {
    if (!row) return [];
    let ids = Array.isArray(row.poolIds) ? row.poolIds.slice() : [];
    if (!ids.length && ctx && typeof ctx.previewIds === 'function') { try { ids = (ctx.previewIds(row) || []).slice(); } catch (_) { ids = []; } }
    if (!ids.length) {
      const sp = row.spec || {}, u = unitShape(row), total = u ? u.total : Math.max(1, Math.floor(+sp.pieceCount) || Math.round(+sp.quantity || +(row.line && row.line.quantity) || 1)), rid = row.order && row.order.receiptId, tx = row.line && row.line.transactionId;
      ids = Array.from({ length: Math.min(total, 400) }, (_, k) => (rid != null && tx != null ? `${rid}_${tx}_${k + 1}` : `piece_${k + 1}`));
    }
    const pre = ids.map((id, k) => ({ poolId: id, n: copyOf(id) || k + 1, slot: slotOfId(ctx, row, id) || null }));
    const shp = shape(pre.map(x => x.slot)), of = pre.length, tagOf2 = shp.kind === 'disc' ? shp.per || of : of, multi = shp.multi || shp.kind === 'copy';
    return pre.map((x, k) => {
      const i = slotInfo(x.slot), within = i ? (i.kind === 'ear' ? (i.side === 'L' ? 1 : 2) : i.kind === 'disc' ? i.n : 1) : 1;
      return { poolId: x.poolId, n: x.n, unit: i ? i.unit : 1, within, slot: x.slot, index: k + 1, of, label: x.slot ? labelOf(x.slot, multi ? shp : null) : '', tag: x.slot ? tagOf(x.slot, tagOf2, multi ? shp : null) : '' };
    });
  }

  /* ── a SET of different designs (SIBLINGS / SCHROLINK / SETBAR; Paul, 13 Oct 2026: "see both bar charms side-by-side ... manipulate each one for the back and front engraving") ──────────────
     A necklace set of two exact designs (spec.pair.set: the large bar BAR_1548 and the small bar SKINNY_0191) is one order line with two pieces, slots D1 and D2, each its OWN design, job, words, Approve and seal.
     The UI told them apart by "Disc 1" / "Disc 2" and the line's joined name, and drew each picture to fill its own box. These are the pure answers every surface asks, so none of them re-reads the spec its own way:
       setDesigns(x)        ["BAR_1548", "SKINNY_0191"] | null    x: a job, a line row, a spec (a line that is not a set of two or more different designs, an earring pair, a glued or an old line: null, and then every
                                                                   surface draws exactly what it always drew)
       setPieces(x)         { designs, source, layoutRequested, pieces: [{ slot "D1", index 0, sku, name "BAR 1548", role "large" | "small" | null, label "Large bar" | "Small bar" | null,
                            length '16"' | null, lengths [...] }] } | null     the line's pieces in slot order; role / label / length come from CharmNestOrders.setPiecesOf (SETBAR's rule row: which chain each bar hangs on
                            is decided THERE, never here); a set a person saved has the design names only. layoutRequested: the buyer asked for another layout, so a person decides which bar is on which chain.
       setPieceOf(x, slot)  the entry of one piece (x a job: its own slot)   ·   pieceName(piece)  "Small bar" else "SKINNY 0191"   ·   skuName("SKINNY_0191") "SKINNY 0191"
       slotWord(job, short) "Left ear" | "Disc 2" | "Small bar"   ·   pieceTag(job, of) "DISC 2 of 3" | "SMALL BAR · 16″"   (the one wording every list, card and message uses for a piece)
     Derived, never stored, no arrays inside arrays. */
  const skuName = s => String(s == null ? '' : s).replace(/_+/g, ' ').replace(/\s+/g, ' ').trim();
  const lineOfAny = x => (x && x.row && typeof x.row === 'object' && !x.spec ? x.row : x) || null;
  const specOfAny = x => { const l = lineOfAny(x); return l && l.spec && typeof l.spec === 'object' ? l.spec : l && l.pair && typeof l.pair === 'object' ? l : null; };
  function setDesigns(x) {
    const sp = specOfAny(x), pr = sp && sp.pair, d = pr && !pr.glued && !pr.legacy && !pr.earring && pr.set && Array.isArray(pr.set.designs) ? pr.set.designs.map(v => String(v || '')) : null;
    return d && d.length >= 2 && d.every(Boolean) && new Set(d).size === d.length ? d : null;
  }
  const lengthOfWord = v => (v == null || v === '' ? null : String(v));
  function setPieces(x) {
    const designs = setDesigns(x); if (!designs) return null;
    const l = lineOfAny(x), sp = specOfAny(x), meta = (sp.pair && sp.pair.set) || {};
    // (the page's one piece model of a set, when it is there: roles, labels and the chain each bar hangs on are its rule row's, not guessed here)
    let model = null; try { const O = dep('CharmNestOrders', './charm-nest-orders.js'); model = O && typeof O.setPiecesOf === 'function' ? O.setPiecesOf(l && l.spec ? l : { spec: sp }) : null; } catch (_) { model = null; }
    const first = Array.isArray(model) ? model.slice(0, designs.length) : [];
    const roles = Array.isArray(meta.roles) ? meta.roles : [], lengths = (Array.isArray(meta.lengths) ? meta.lengths : []).map(String);
    const pieces = designs.map((sku, i) => {
      const m = first.find(q => q && q.slotKey === 'D' + (i + 1)) || null;
      return { slot: 'D' + (i + 1), index: i, sku: String((m && m.sku) || sku), name: skuName((m && m.sku) || sku), role: (m && m.role) || roles[i] || null, label: (m && m.label) || null, length: lengthOfWord(m && m.length), lengths: ((m && m.lengths) || lengths).map(String) };
    });
    // the buyer asked for another layout and nobody has looked yet (SETBAR-A: a piece's `layoutOpen`, which is spec.pair.set.layout { asked, answered }); a person's answer ("the default stands") ends the note, and the chain a piece hangs on is null until then
    const lay = meta.layout && typeof meta.layout === 'object' ? meta.layout : null;
    const layoutRequested = first.some(q => q && q.layoutOpen === true) || !!(lay && lay.asked && !lay.answered);
    return { designs, source: String(meta.source || ''), layoutRequested, pieces };
  }
  /** The slot of x when it names one (a job, a side row, a key "<line>#D2", a slot word). */
  const slotOfAny = (x, slot) => { if (slot) return String(slot); if (x && typeof x === 'object') { if (x.slot) return String(x.slot); const k = x.key != null ? x.key : x.row && x.row.key; const m = /#(D\d{1,2}(?:\.\d{1,3})?)(?:~front)?$/.exec(String(k == null ? '' : k)); return m ? m[1] : ''; } return ''; };
  function setPieceOf(x, slot) {
    const s = setPieces(x); if (!s) return null;
    const sl = baseSlot(slotOfAny(x, slot)); return s.pieces.find(p => p.slot === sl) || null;   // (the piece of a unit-qualified slot "D2.2" is the set's piece D2)
  }
  /** What a piece of a set is called: the rule's word ("Small bar"), else its design's name ("SKINNY 0191"). */
  const pieceName = p => (p ? p.label || p.name : '');
  /** The chain length as said on a label: 16" → 16″. */
  const lengthText = p => (p && p.length ? String(p.length).replace(/"|''|”/g, '″') : '');
  /** The words a piece goes by in a sentence: "Left ear" / "Disc 2" (earOf), `short` "Left" (labelOf); a piece of a set of different designs: "Small bar". Every message that names a piece asks this. */
  const slotWord = (job, short, shp) => { const sp = setPieceOf(job), u = unitOf(job && job.slot); return sp ? pieceName(sp) + (u > 1 || (shp && shp.multi) ? ' · set ' + u : '') : short ? labelOf(job && job.slot, shp) : earOf(job && job.slot, shp); };
  /** The tag of a piece as every list and card says it: "LEFT EAR", "DISC 2 of 3"; a piece of a set: "SMALL BAR · 16″" (the rule's name and the chain). '' for a job with no slot. */
  const pieceTag = (job, of, shp) => { const sp = setPieceOf(job), u = unitOf(job && job.slot), i = slotInfo(job && job.slot); return sp ? [pieceName(sp).toUpperCase(), lengthText(sp), u > 1 || (shp && shp.multi) ? 'SET ' + u : ''].filter(Boolean).join(' · ') : job && job.slot ? tagOf(job.slot, shp && shp.multi && i && i.kind === 'disc' ? shp.per : of, shp) : ''; };

  /* ── the per-piece record (DISCCYCLE, DISCMODALS, the engraving cards): the existing job read as the disc it is. Derived, never stored, no nested arrays. ──
     pieceRecord(job, o) -> { index (1-based), of, slot, key, lineKey, groupKey, tag "DISC 2 of 3", label "Disc 2", poolIds, words, lines, font, fontAsked, state, approved,
                              approvedBy, approvedAt, sealed, wordsSource, face "back"|"front", faceLabel "Back"|"Front", faceTag "BACK"|"FRONT", pieceKey "<line>[#slot]" }
     o: { of, faces } (the number of PIECES of the line; else the line's own count · how many faces this piece has: 2 puts " · FRONT" on the front's tag) */
  const fontOf = job => {
    const f = (job && job.font) || (job && job.row && job.row.spec && job.row.spec.font) || (job && job.row && job.row.parentRow && job.row.parentRow.spec && job.row.parentRow.spec.font) || null;
    return f && typeof f === 'object' && (f.asked || f.id || f.name) ? { asked: String(f.asked || ''), id: String(f.id || ''), name: String(f.name || ''), source: String(f.source || '') } : null;
  };
  function pieceRecord(job, o) {
    if (!job) return null;
    const slot = job.slot || (job.row && job.row.slot) || null, info = slotInfo(slot), shp = o && o.shape && typeof o.shape === 'object' ? o.shape : null;
    // (the place of the piece among the pieces of the line: Left 1, Right 2, Disc n, Piece n; a unit after the first counts on from the unit before it: Right of the 2nd pair is 4)
    const within = info ? (info.kind === 'ear' ? (info.side === 'L' ? 1 : 2) : info.n) : 1, perU = shp && shp.per ? shp.per : info && info.kind === 'ear' ? 2 : info && info.kind === 'disc' ? info.n : 1;
    const idx = !info ? 1 : info.kind === 'copy' ? info.n : info.unit > 1 ? (info.unit - 1) * perU + within : within;
    const of = Math.max(1, Math.floor(+(o && o.of) || 0) || (job.row && job.row.parentRow && perUnitOf(job.row.parentRow)) || idx);
    const lines = (Array.isArray(job.lines) ? job.lines : []).map(x => String(x).trim()).filter(Boolean), font = fontOf(job);
    const seals = [].concat(job.engravingSeals || [], (job.engraveRec && job.engraveRec.seals) || [], job.seals || []);
    const face = faceOf(job), lineKey = job.rowKey || parseKey(job.key).rowKey, withBack = !!(o && o.faces > 1);
    const rec = { index: idx, of, slot, key: job.key, lineKey, groupKey: job.groupKey || '', tag: tagOf(slot, shp && shp.multi && info && info.kind === 'disc' ? shp.per : info && info.kind === 'copy' && !(o && Math.floor(+o.of)) ? 0 : of, shp), label: labelOf(slot, shp),   // (a copy piece asked without the line's count says "PIECE 2", never a made-up "of 2")
      poolIds: (Array.isArray(job.copies) ? job.copies : []).slice(), words: lines.join(' / '), lines, font, fontAsked: font ? font.asked : '', state: job.state || '',
      approved: ['approved', 'written'].includes(job.state), approvedBy: job.approvedBy || (job.engraveRec && job.engraveRec.approvedBy) || '', approvedAt: +job.approvedAt || +(job.engraveRec && job.engraveRec.approvedAt) || 0, sealed: seals.length > 0, wordsSource: job.wordsSource || job.source || '' };
    // the face of the job (additive: derived, never stored). A front's tag says so only when the piece has a back besides ("DISC 2 of 3 · FRONT"); the back's tag is what it always was.
    rec.face = face; rec.faceLabel = face === 'front' ? 'Front' : 'Back'; rec.faceTag = face === 'front' ? 'FRONT' : 'BACK'; rec.pieceKey = lineKey + (slot ? '#' + slot : '');
    // a piece of a SET of different designs (the large bar, the small bar): its own design, and the chain it hangs on when the set's rule says (additive; a line that is no set has no `design`)
    if (info && (info.kind === 'copy' || info.unit > 1 || (shp && shp.multi))) { rec.kind = info.kind; rec.unit = info.unit; }   // (PIECES: additive, only on a line of several units)
    const sp = setPieceOf(job); if (sp) rec.design = { sku: sp.sku, name: sp.name, role: sp.role, label: sp.label, length: sp.length };
    if (face === 'front' && withBack) rec.tag = [rec.tag, 'FRONT'].filter(Boolean).join(' · ');
    return rec;
  }
  const faceRank = j => (isFront(j) ? 0 : 1);
  const bySlot = (a, b) => order(a.slot) - order(b.slot) || faceRank(a) - faceRank(b);
  /** The pieces of one line (D1..Dn, Left and Right, ...), each record saying the same `of` (the number of PIECES: a piece with a front and a back counts once) and, when a piece has
   *  both faces, `faces` 2; one record per (piece, face), Front before Back. A line with no front job: exactly the records it always gave. */
  function pieceRecords(jobs, o) {
    const list = (jobs || []).filter(Boolean).slice().sort(bySlot), front = list.some(isFront);
    const of = Math.floor(+(o && o.of) || 0) || (front ? new Set(list.map(j => j.slot || (j.row && j.row.slot) || null)).size : list.length);
    const per = new Map(); for (const j of list) { const k = j.slot || (j.row && j.row.slot) || ''; per.set(k, (per.get(k) || 0) + 1); }
    const shp = shape(list.map(j => j.slot || (j.row && j.row.slot) || null)), shpArg = shp.multi || shp.kind === 'copy' ? shp : null;   // (a line of one unit: no shape, the records it always gave)
    return list.map(j => pieceRecord(j, Object.assign({ of, faces: front ? per.get(j.slot || (j.row && j.row.slot) || '') : 1 }, shpArg ? { shape: shpArg } : {})));
  }
  return { faceOf, isFront, jobKey, parseKey, labelOf, earOf, tagOf, copyOf, slotOfId, slotInfo, isSlot, isCopy, baseSlot, unitOf, sideOfSlot, slotRank, shape, unitsOfRow, unitOfId, unitShape, wordsForPieces, pieceList, mirrorOfId, pieceCharm, plan, idsOfSlot, sideRow, summary, linkParent, unlinkParent, perUnitOf, splitWords, wordsForDiscs, pieceRecord, pieceRecords, setDesigns, setPieces, setPieceOf, pieceName, lengthText, slotWord, pieceTag, skuName, STATE_ORDER, SLOT: SLOT };
});
