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
 *
 *   slotOfId(ctx, row, poolId)     -> "L" | "R" | "D<n>" | null   the piece's slot: its stored `side`, else derived (never written back)
 *   plan(ctx, row, existing)       -> { split, slots:[slot|null], of: Map(poolId -> slot|null) }   how a line's jobs are cut
 *   mirrorOfId(ctx, poolId, charm, hint) -> boolean  is the piece the mirror image of the drawing (the Right earring)?   pieceCharm(ctx, poolId, charm, hint) -> the charm as
 *       THIS piece is cut (CharmNestPair.pieceGeometry: outline, holes and art mirrored, text never reversed); the same object for a piece that is not mirrored
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
    // (not on a line that is no earring line: a necklace, pendant or charm of a two-body design is one piece per unit, never a Left and a Right ear: Paul, 10 Oct)
    try { if (row && row.spec && typeof P.plainLine === 'function' && P.plainLine({ spec: row.spec, form: row.spec.form })) return false; } catch (_) { /* decided below */ }
    const sku = row && row.spec && row.spec.designSku;
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
    const side = (rec && rec.side) || charm.side || (hint && hint.side) || (hint && (hint.slot === 'L' || hint.slot === 'R') ? hint.slot : null);
    if (side !== 'L' && side !== 'R') return charm;
    const mirror = mirrorOfId(ctx, poolId, charm, hint);
    const bodyIndex = rec && rec.bodyIndex != null ? +rec.bodyIndex : hint && hint.bodyIndex != null ? +hint.bodyIndex : side === 'R' ? 1 : 0;
    let g = null;
    try { g = P.pieceGeometry(charm, { poolId, side, bodyIndex, mirror }); } catch (_) { g = null; }
    return g && g.outline ? g : charm;
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

  /* ── the buyer's words, one disc at a time (DISCREAD, Paul 10 Oct: "Tag 1: J, Tag 2: Q" is disc 1 = J, disc 2 = Q) ─────────────────────────────────────
     splitWords(texts, n) -> { ok, words: [n strings], how, why }. Deterministic, no AI, nothing guessed: it answers only when the note says plainly which words
     go on which disc, and otherwise says why not (the paid reader or a person decides, as before). A line of n discs gives exactly n words, disc 1 first.
       numbered     "Tag 1: J, Tag 2: Q" · "Disc 1: A; Disc 2: B" · "Initial 1 - J, Initial 2 - Q" · "1: J, 2: Q" · "1) J 2) Q" · "First disc: J, second: Q" · "Charm #2 = Q" (any order, each number once, 1..n)
       lines        n entries of the personalisation field, or n lines ("J" / "Q"), or n comma / semicolon / slash / ampersand separated words ("J, Q")
       letters      n single characters separated by spaces ("J Q")
       same         the buyer says it is the same on every disc ("All discs: J", "Same on all: J", "J on each disc"): the one word on every disc
     NOT answered (ok false): a count that differs from n, a number used twice or missing, one bare word with no sign it is for all discs, any other text around the markers. */
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
    return r.ok ? r : null;
  }

  /* ── the per-piece record (DISCCYCLE, DISCMODALS, the engraving cards): the existing job read as the disc it is. Derived, never stored, no nested arrays. ──
     pieceRecord(job, o) -> { index (1-based), of, slot, key, lineKey, groupKey, tag "DISC 2 of 3", label "Disc 2", poolIds, words, lines, font, fontAsked, state, approved,
                              approvedBy, approvedAt, sealed, wordsSource }
     o: { of } (the number of discs of the line; else the line's own count) */
  const fontOf = job => {
    const f = (job && job.font) || (job && job.row && job.row.spec && job.row.spec.font) || (job && job.row && job.row.parentRow && job.row.parentRow.spec && job.row.parentRow.spec.font) || null;
    return f && typeof f === 'object' && (f.asked || f.id || f.name) ? { asked: String(f.asked || ''), id: String(f.id || ''), name: String(f.name || ''), source: String(f.source || '') } : null;
  };
  function pieceRecord(job, o) {
    if (!job) return null;
    const slot = job.slot || (job.row && job.row.slot) || null, m = /^D(\d{1,2})$/.exec(slot || ''), idx = m ? +m[1] : slot === 'L' ? 1 : slot === 'R' ? 2 : 1;
    const of = Math.max(1, Math.floor(+(o && o.of) || 0) || (job.row && job.row.parentRow && perUnitOf(job.row.parentRow)) || idx);
    const lines = (Array.isArray(job.lines) ? job.lines : []).map(x => String(x).trim()).filter(Boolean), font = fontOf(job);
    const seals = [].concat(job.engravingSeals || [], (job.engraveRec && job.engraveRec.seals) || [], job.seals || []);
    return { index: idx, of, slot, key: job.key, lineKey: job.rowKey || parseKey(job.key).rowKey, groupKey: job.groupKey || '', tag: tagOf(slot, of), label: labelOf(slot),
      poolIds: (Array.isArray(job.copies) ? job.copies : []).slice(), words: lines.join(' / '), lines, font, fontAsked: font ? font.asked : '', state: job.state || '',
      approved: ['approved', 'written'].includes(job.state), approvedBy: job.approvedBy || (job.engraveRec && job.engraveRec.approvedBy) || '', approvedAt: +job.approvedAt || +(job.engraveRec && job.engraveRec.approvedAt) || 0, sealed: seals.length > 0, wordsSource: job.wordsSource || job.source || '' };
  }
  const bySlot = (a, b) => order(a.slot) - order(b.slot);
  /** The discs of one line, D1..Dn, each record saying the same `of`. */
  function pieceRecords(jobs, o) {
    const list = (jobs || []).filter(Boolean).slice().sort(bySlot), of = Math.floor(+(o && o.of) || 0) || list.length;
    return list.map(j => pieceRecord(j, { of }));
  }
  return { jobKey, parseKey, labelOf, earOf, tagOf, copyOf, slotOfId, mirrorOfId, pieceCharm, plan, idsOfSlot, sideRow, summary, linkParent, unlinkParent, perUnitOf, splitWords, wordsForDiscs, pieceRecord, pieceRecords, STATE_ORDER, SLOT: SLOT };
});
