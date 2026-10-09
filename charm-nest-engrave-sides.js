/* The engraving of one PIECE of an order line (Paul, 9 Oct 2026: "whenever selling pairs of earrings, matching or mismatched ... even some
 * necklaces such as discs": every step tracks every piece).
 *
 * Until now the sorter kept ONE engraving job per order LINE: every copy of the line shared one text, one fit, one approval and one set of
 * seals. That is right for a matching pair (two copies of one charm, one engraving) and wrong for
 *   - a MISMATCHED pair: two different charm bodies, a left ear and a right ear. Each ear is fitted on its own body, can carry its own words
 *     ("Anna" on the left, "Ben" on the right), is approved on its own and has its own back file;
 *   - a necklace with several DISCS: each disc can carry its own letter.
 * Such a line is SPLIT: one job per SLOT. A slot is "L" or "R" (the ears) or "D1".."Dn" (the discs). A single charm, a matching pair, a charm
 * only: no slot, ONE job keyed by the line, byte for byte as before. Old records (no side, no slot) read as they always did.
 *
 *   slotOfId(ctx, row, poolId)     -> "L" | "R" | "D<n>" | null   the piece's slot: its stored `side`, else derived (never written back)
 *   plan(ctx, row, existing)       -> { split, slots:[slot|null], of: Map(poolId -> slot|null) }   how a line's jobs are cut
 *   jobKey(rowKey, slot) / parseKey(key) -> { rowKey, slot }
 *   labelOf(slot) "Left" | "Right" | "Disc 2"  ·  earOf(slot) "Left ear" | "Right ear" | "Disc 2"  ·  tagOf(slot, of) "LEFT EAR" | "DISC 2 of 3"
 *   sideRow(ctx, parent, slot, job) -> a row for ONE slot: it inherits everything from the line's row (state, order, spec, line, hold: all
 *       live) and owns its key, its pool ids (only this slot's, live) and its engrave record (kept on the job, so a checkpoint keeps it)
 *   linkParent(ctx, parent, jobs)   -> the line's row `engrave` becomes the summary of its slots (read-only truth: worst state first,
 *       approved only when every slot is approved, `pieces[poolId]` = that piece's own decision); a write to it reaches every slot
 *   summary(ctx, legacy, jobs)      -> that summary, as plain data
 * ctx: { poolRow(poolId), charmOf(poolId), entryFor(sku), pair: CharmNestPair, mergeSeals(...records) }; every member optional.
 * Pure: no page, no network, no clock. Loads in the page (window.CharmNestEngraveSides) and in node. */
(function (root, factory) {
  const api = factory(root);
  if (typeof module === 'object' && module.exports) module.exports = api; else root.CharmNestEngraveSides = api;
})(typeof self !== 'undefined' ? self : this, function (root) {
  'use strict';
  const SLOT = /^(L|R|D\d{1,2})$/;
  const KEY = /^(.*)#(L|R|D\d{1,2})$/;
  const COPY = /_(\d{1,3})$/;
  const pairLib = ctx => (ctx && ctx.pair) || (root && root.CharmNestPair) || null;

  const jobKey = (rowKey, slot) => (slot ? `${rowKey}#${slot}` : String(rowKey));
  function parseKey(key) { const m = KEY.exec(String(key == null ? '' : key)); return m ? { rowKey: m[1], slot: m[2] } : { rowKey: String(key == null ? '' : key), slot: null }; }
  const labelOf = slot => (slot === 'L' ? 'Left' : slot === 'R' ? 'Right' : /^D\d+$/.test(slot || '') ? 'Disc ' + slot.slice(1) : '');
  const earOf = slot => (slot === 'L' ? 'Left ear' : slot === 'R' ? 'Right ear' : labelOf(slot));
  const tagOf = (slot, of) => (slot === 'L' ? 'LEFT EAR' : slot === 'R' ? 'RIGHT EAR' : /^D\d+$/.test(slot || '') ? 'DISC ' + slot.slice(1) + (of > 1 ? ' of ' + of : '') : '');
  const copyOf = id => { const m = COPY.exec(String(id == null ? '' : id)); return m ? +m[1] : 0; };

  /** A pair of earrings whose design draws two different bodies: the master entry says so (`pair`), or the glued charm shows two bodies. */
  function mismatchedDesign(ctx, row, poolId) {
    const P = pairLib(ctx); if (!P) return false;
    const sku = row && row.spec && row.spec.designSku;
    try { const dp = ctx && ctx.entryFor && sku ? P.designPair && P.designPair(ctx.entryFor(sku)) : null; if (dp) return !!dp.mismatched && dp.bodies === 2; } catch (_) { /* fall through to the charm */ }
    try { const ch = ctx && ctx.charmOf && poolId ? ctx.charmOf(poolId) : null; return !!(ch && ch.outline && P.isMismatched(ch)); } catch (_) { return false; }
  }
  function discsOf(ctx, row) {
    const P = pairLib(ctx); if (!P || !P.discsOf) return 0;
    try { return P.discsOf(Object.assign({}, row && row.line, { spec: row && row.spec })) || 0; } catch (_) { return 0; }
  }
  /** The slot of one piece. Its own `side` (PAIRPOOL's pool record, or the charm it was cut from) wins; else it is derived from the copy number
   *  (copy 1 = left, 2 = right, 3 = left ... for a mismatched design; copy n of d discs = disc ((n-1) mod d)+1). Never stored by this module. */
  function slotOfId(ctx, row, poolId) {
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
  const order = s => (s === 'L' ? 0 : s === 'R' ? 1 : /^D\d+$/.test(s || '') ? 1 + (+s.slice(1)) : 99);
  /** How the line's engraving jobs are cut. `existing`: the slots jobs of this line already exist under (permanent: a job that exists keeps its slot).
   *  A line is split when its pieces fall into two or more slots, or when slotted jobs already exist. A legacy job already decided under the plain
   *  line key (approved, written, cut plain) keeps the line whole: an approval is never re-cut. */
  function plan(ctx, row, existing) {
    const ids = (row && Array.isArray(row.poolIds) ? row.poolIds : []).slice();
    const of = new Map(ids.map(id => [id, slotOfId(ctx, row, id)]));
    const have = (existing || []).filter(e => e && typeof e === 'object');
    const slotted = have.filter(e => e.slot), plain = have.find(e => !e.slot);
    const distinct = [...new Set([...of.values()].filter(Boolean))];
    const legacyDecided = !!plain && ['approved', 'written', 'skipped'].includes(plain.state) && !slotted.length;
    const split = !legacyDecided && (distinct.length >= 2 || slotted.length > 0);
    if (!split) return { split: false, slots: [null], of: new Map(ids.map(id => [id, null])) };
    const slots = [...new Set([...distinct, ...slotted.map(e => e.slot)])].sort((a, b) => order(a) - order(b));
    if ([...of.values()].some(s => !s)) slots.push(null);
    return { split: true, slots, of };
  }
  /** The pool ids of one slot, in the line's own order (live: read again each time). */
  const idsOfSlot = (ctx, row, slot) => (row && Array.isArray(row.poolIds) ? row.poolIds : []).filter(id => (slotOfId(ctx, row, id) || null) === (slot || null));

  /** A row for one slot. Everything not owned here is the line's own (prototype), so a cancelled or held line reads as gone/held for every slot. */
  function sideRow(ctx, parent, slot, job) {
    const row = Object.create(parent);
    Object.defineProperties(row, {
      key: { value: jobKey(parent.key, slot), enumerable: true, writable: true, configurable: true },
      slot: { value: slot || null, enumerable: true, configurable: true },
      side: { value: slot === 'L' || slot === 'R' ? slot : null, enumerable: true, configurable: true },
      parentRow: { value: parent, enumerable: false, configurable: true },
      poolIds: { get() { return idsOfSlot(ctx, parent, slot); }, enumerable: true, configurable: true },
      engrave: { get() { return job.engraveRec; }, set(v) { job.engraveRec = v; }, enumerable: true, configurable: true }
    });
    return row;
  }

  const STATE_ORDER = ['reclassify', 'classify', 'words', 'blocked', 'ready', 'fitting', 'review', 'approved', 'written'];
  const settled = s => s === 'none' || s === 'skipped';
  /** The line's engraving as one record, from its slots' records. Approved only when every slot is; the state is the least advanced of the slots still
   *  needing a back; `pieces[poolId]` carries each piece's own decision (the readiness check reads it). Returns undefined when no slot has a record yet. */
  function summary(ctx, legacy, jobs) {
    const list = (jobs || []).filter(Boolean), recs = list.map(j => ({ job: j, rec: j.engraveRec })).filter(x => x.rec);
    if (!recs.length) return legacy && Object.keys(legacy).length ? legacy : undefined;
    const needed = recs.filter(x => !settled(x.rec.state) && x.rec.needed !== false);
    const out = {};
    out.needed = recs.some(x => !!x.rec.needed);
    if (needed.length) out.state = needed.map(x => x.rec.state).sort((a, b) => (STATE_ORDER.indexOf(a) < 0 ? 99 : STATE_ORDER.indexOf(a)) - (STATE_ORDER.indexOf(b) < 0 ? 99 : STATE_ORDER.indexOf(b)))[0];
    else out.state = recs.every(x => x.rec.state === 'none') ? 'none' : 'skipped';
    out.approved = recs.length === list.length && recs.every(x => !!x.rec.approved);
    const texts = recs.map(x => ({ slot: x.job.slot, text: String(x.rec.text || '').replace(/\s*\n\s*/g, ' / ').trim() })).filter(x => x.text);
    out.text = texts.length ? (new Set(texts.map(x => x.text)).size === 1 ? texts[0].text : texts.map(x => (labelOf(x.slot) || 'All') + ': ' + x.text).join(' · ')) : undefined;
    const by = recs.map(x => x.rec.approvedBy).filter(Boolean); if (by.length) out.approvedBy = by[0];
    out.approvedAt = Math.max(0, ...recs.map(x => +x.rec.approvedAt || 0)) || undefined;
    out.decidedAt = Math.max(0, ...recs.map(x => +x.rec.decidedAt || 0)) || undefined;
    const reason = recs.map(x => x.rec.reason).filter(Boolean); if (reason.length && !out.approved) out.reason = reason[0];
    const merge = ctx && ctx.mergeSeals;
    out.seals = merge ? merge(legacy, ...recs.map(x => x.rec)) : [].concat(...recs.map(x => x.rec.seals || []));
    const pieces = {};
    for (const x of recs) for (const id of x.job.copies || []) pieces[id] = { needed: !!x.rec.needed, state: x.rec.state, approved: !!x.rec.approved };
    if (Object.keys(pieces).length) out.pieces = pieces;
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
  return { jobKey, parseKey, labelOf, earOf, tagOf, copyOf, slotOfId, plan, idsOfSlot, sideRow, summary, linkParent, unlinkParent, STATE_ORDER, SLOT: SLOT };
});
