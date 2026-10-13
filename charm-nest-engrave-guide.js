/* The GUIDE of an order's pieces (Paul, 13 Oct 2026: "every engraved piece needs a dedicated Approve button so that the user only approves one charm at a time and doesn't mistakenly press approve without
 * seeing the rest of the pieces in the multi-piece order ... The user must be never allowed to skip pieces ... the UI must guide the user very clearly and simply to every piece ... I wanna keep it fairly
 * minimalist"). The rule, kept as small as it can be:
 *
 *   1. Approve approves ONE piece (a charm, a disc; both of its faces only through that piece's own "Approve both faces"). There is no press that approves several pieces.
 *   2. An order is done only when EVERY engraved piece of it is approved (CharmNestEngraveOrder.countOf(rid).left === 0), counted across all its lines whatever their metal.
 *   3. After a piece is approved the screen goes on to what is left of the SAME order, never to some other order: first the piece's other face, then the next piece that is not approved.
 *   4. A press that would leave an order with a piece not approved (Back / Next past its ends, close, Escape) is not silent: it draws ONE calm line in the detail, "2 pieces of this order are not approved yet",
 *      with one tap to the next piece and one to leave anyway. A press that would leave an order of one piece, or an order with nothing left, is not asked anything.
 *   5. In the order window the Engraving section shows only the piece picked; once that piece is approved, one quiet line under it says how many pieces of the order are still to approve and shows the next.
 *
 * This file only DECIDES and DRAWS that one line. Nothing is stored, nothing is read from the network, no timer or listener of its own (a click on its own two buttons is all). The approval itself, its seal and its
 * records are Engrave.approve's and are not touched here. Every call fails open: if the model is not on the page, or throws, the press goes through as it always did.
 *
 *   afterApprove(job)                 -> { rid, pieceKey, face, key } | null    where the detail goes after `job` was approved: the same piece's other face still to settle, else the next piece of the order
 *                                        that is not approved (CharmNestEngraveOrder.nextUnapproved); null when the order has nothing left (the queue goes on as it always did)
 *   leaving(rid, pieceKey, go)        -> true when it took over the press (the line is drawn, `go` is NOT called); false when the press may go ahead (the caller then does its normal move)
 *                                        `go` is what "Leave" does: the caller's normal move
 *   mount(card, rid, pieceKey)        puts the line back into a card drawn again while it was showing (the card draws an empty `.egGuide` host; a card of another piece or order clears it)
 *   clear()                           the line is gone (the focus moved by another way)
 *   pointer(rid, { only, shown })     -> { k, key, lineKey, label, index } | null   the order window's line: the pick is approved, k more pieces of the order are not; `only` { line, side } (a line, or one ear / disc), `shown` the pick's card-model pieces
 *   leftOf(rid)                       -> { n, left: [P], next }                  what is not approved yet, for a caller that wants to say it
 * Loads in the page (window.CharmNestEngraveGuide) and in node. */
(function (root, factory) {
  const api = factory(root);
  if (typeof module === 'object' && module.exports) module.exports = api; else root.CharmNestEngraveGuide = api;
})(typeof self !== 'undefined' ? self : this, function (root) {
  'use strict';
  const dep = (name, file) => { const g = root && root[name]; if (g) return g; try { return typeof require === 'function' ? require(file) : null; } catch (_) { return null; } };
  const Ord = () => dep('CharmNestEngraveOrder', './charm-nest-engrave-order.js');
  const Faces = () => dep('CharmNestEngraveFaces', './charm-nest-engrave-faces.js');
  const tryDo = (f, d) => { try { return f(); } catch (e) { try { console.warn('engrave guide:', e); } catch (_) {} return d; } };
  const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const OPEN = ['review', 'words', 'blocked', 'classify', 'ready', 'fitting', 'reclassify'];
  const faceOf = j => { const F = Faces(); return F && F.faceOf ? F.faceOf(j) : j && (j.face === 'front' || /~front$/.test(String(j.key || ''))) ? 'front' : 'back'; };
  const pieceKeyOf = key => { const F = Faces(); return F && F.pieceKeyOf ? F.pieceKeyOf(key) : String(key || '').replace(/~front$/, ''); };
  const ridOfJob = job => String((job && job.row && ((job.row.parentRow || job.row).order || {}).receiptId) || '');

  /* ── what is not approved yet ── */
  function leftOf(rid) {
    const O = Ord(); if (!O || !O.piecesOf) return { n: 0, left: [], next: null, all: [] };
    const all = tryDo(() => O.piecesOf(String(rid)), []) || [], left = all.filter(p => p.state !== 'approved');
    return { n: all.length, left, next: null, all };
  }
  /** After `job` was approved: where the detail goes on. */
  function afterApprove(job) {
    const O = Ord(); if (!O || !job) return null;
    const rid = ridOfJob(job), pk = pieceKeyOf(job.key); if (!rid || !pk) return null;
    const all = tryDo(() => O.piecesOf(rid), []) || [], me = all.find(p => p.pieceKey === pk);
    if (all.length < 2 && !(me && me.faces && me.faces.length > 1)) return null;   // (one piece with one face: nothing of this order is left to guide to)
    if (me) {
      // (the same piece's other face first: a piece is not done while one of its faces waits)
      const other = ['front', 'back'].map(f => tryDo(() => O.jobOf(me, f), null)).filter(j => j && j !== job && OPEN.includes(j.state));
      if (other.length) return { rid, pieceKey: pk, face: faceOf(other[0]), key: other[0].key };
    }
    const nx = tryDo(() => O.nextUnapproved(rid, pk), null); if (!nx) return null;
    const j = tryDo(() => O.jobOf(nx), null);
    return { rid, pieceKey: nx.pieceKey, face: j ? faceOf(j) : undefined, key: j ? j.key : '' };
  }

  /* ── the line in the Engrave detail ── */
  let S = null;   // { rid, pieceKey, go }: the press that was held back, while its line is showing
  const hostIn = card => (card && card.querySelector ? card.querySelector('.egGuide') : null);
  const liveHost = () => { const d = root.document; return d ? hostIn(d.querySelector('#egQueue .rvItem[data-kind=placement]')) : null; };
  function css() {
    const d = root.document; if (!d || d.getElementById('egGuideCss')) return;
    const s = d.createElement('style'); s.id = 'egGuideCss';
    s.textContent = '.egGuide{display:flex;align-items:center;gap:8px 10px;flex-wrap:wrap;min-width:0;padding:6px 11px;border-radius:9px;background:var(--goldSoft,#f6efdc);border:1px solid var(--line,#e4ddd0);font:12.5px/1.35 var(--sans,system-ui);color:var(--ink,#1f1a14)}'
      + '.egGuide[hidden],.engLeft[hidden]{display:none}.egGuide .egGuideTxt{flex:1 1 auto;min-width:0}.egGuide .egGuideTxt b{font-weight:700}'
      + '.egGuide .btn,.engLeft .btn{flex:none;height:22px;padding:0 10px;font-size:12px;line-height:1;white-space:nowrap}.egGuide .egGuideLeave{color:var(--ink45,#7a7266)}'
      + '.egGuide[data-pulse]{animation:egGuidePulse .5s ease}@keyframes egGuidePulse{0%{box-shadow:0 0 0 0 rgba(169,130,63,.55)}100%{box-shadow:0 0 0 7px rgba(169,130,63,0)}}'
      + '.engLeft{display:flex;align-items:center;gap:8px 10px;flex-wrap:wrap;min-width:0;margin:8px 0 0;font:12.5px/1.35 var(--sans,system-ui);color:var(--ink70,#5b554c)}.engLeft .engLeftTxt{min-width:0}.engLeft .engLeftTxt b{font-weight:700;color:var(--ink,#1f1a14)}'
      + '@media (prefers-reduced-motion:reduce){.egGuide[data-pulse]{animation:none}}';
    (d.head || d.documentElement).appendChild(s);
  }
  const plural = (n, one, many) => (n === 1 ? one : many);
  function paint(host) {
    if (!host || !S) return false;
    const O = Ord(), g = leftOf(S.rid);
    if (!g.left.length || g.n < 2) { S = null; host.hidden = true; host.replaceChildren(); return false; }
    css();
    const nx = tryDo(() => O.nextUnapproved(S.rid, S.pieceKey), null), at = nx ? g.all.findIndex(p => p.pieceKey === nx.pieceKey) : -1, k = g.left.length;
    const canGo = !!(nx && root.Engrave && typeof root.Engrave.openPiece === 'function');
    host.innerHTML = `<span class="egGuideTxt" role="status"><b>${k}</b> ${plural(k, 'piece of this order is', 'pieces of this order are')} not approved yet</span>`
      + (canGo ? `<button type="button" class="btn ghost xs egGuideGo" data-g="go" title="${esc(`Open piece ${at + 1} of ${g.n}: ${nx.label || ''}`)}">Go to piece ${at + 1}</button>` : '')
      + `<button type="button" class="btn ghost xs egGuideLeave" data-g="leave" title="Leave this order as it is: its pieces that are not approved stay in Placements">Leave</button>`;
    host.hidden = false;
    const go = host.querySelector('[data-g=go]'), leave = host.querySelector('[data-g=leave]');
    if (go) go.onclick = () => { const s = S; S = null; host.hidden = true; host.replaceChildren(); if (s && nx) tryDo(() => root.Engrave.openPiece(s.rid, nx.pieceKey)); };
    if (leave) leave.onclick = () => { const s = S; S = null; host.hidden = true; host.replaceChildren(); if (s && typeof s.go === 'function') tryDo(() => s.go()); };
    return true;
  }
  /** A press that would leave the order: true = it is held back and the line says why. */
  function leaving(rid, pieceKey, go) {
    try {
      rid = String(rid == null ? '' : rid); if (!rid || !Ord()) return false;
      const g = leftOf(rid); if (g.n < 2 || !g.left.length) return false;      // (an order of one piece, or one with nothing left: nothing is asked)
      const host = liveHost(); if (!host) return false;                           // (no detail on screen to say it in: the press is never held back for nothing)
      const same = !!S && S.rid === rid && S.pieceKey === pieceKey && !host.hidden;
      S = { rid, pieceKey: pieceKey || '', go };
      if (!paint(host)) return false;
      if (same) { host.removeAttribute('data-pulse'); void host.offsetWidth; host.setAttribute('data-pulse', '1'); }   // (pressed again while it shows: it says so again, it does not give way)
      return true;
    } catch (e) { try { console.warn('engrave guide:', e); } catch (_) {} return false; }
  }
  /** The card was drawn again (the detail follows its jobs): the line stays while it is the same piece, and goes at once for another. */
  function mount(card, rid, pieceKey) {
    const host = hostIn(card); if (!host) return;
    if (S && S.rid === String(rid) && S.pieceKey === (pieceKey || '')) { if (!paint(host)) { host.hidden = true; } }
    else { if (S) S = null; host.hidden = true; host.replaceChildren(); }
  }
  const clear = () => { S = null; const h = liveHost(); if (h) { h.hidden = true; h.replaceChildren(); } };

  /* ── the line in the order window ── */
  function pointer(rid, o) {
    try {
      o = o || {}; const only = o.only, shown = o.shown || [];
      if (!only || !(only.line || only.side) || !shown.length || !shown.every(p => p && p.state === 'approved')) return null;   // (nothing picked, or the piece picked still has something to do: no line)
      const g = leftOf(rid), mine = new Set(shown.map(p => p.pieceKey)), other = g.left.filter(p => !mine.has(p.pieceKey)); if (!other.length) return null;   // (a piece, not a line: one disc or one ear of the line may be the pick)
      let at = -1; g.all.forEach((p, i) => { if (mine.has(p.pieceKey)) at = i; });
      const after = at < 0 ? g.all : g.all.slice(at + 1).concat(g.all.slice(0, at + 1)), next = after.find(p => p.state !== 'approved' && !mine.has(p.pieceKey)) || other[0], index = g.all.indexOf(next);
      const labels = g.all.map(p => p.label), unique = labels.filter(l => l === next.label).length === 1;
      return { k: other.length, key: next.pieceKey, lineKey: next.lineKey, label: unique && next.label ? next.label : `piece ${index + 1}`, index };
    } catch (e) { try { console.warn('engrave guide:', e); } catch (_) {} return null; }
  }
  /** The order window's line: draws it (or hides it) in `el`; `pick(pieceKey)` is what its button does (OrderWin.selectPiece takes a line, or "<line>#L" / "<line>#D2" for one ear or disc). */
  function drawPointer(el, p, pick) {
    if (!el) return; css();
    if (!p) { if (!el.hidden) { el.hidden = true; el.replaceChildren(); } el._sig = ''; return; }
    const sig = JSON.stringify([p.k, p.key || p.lineKey, p.label]); if (el._sig === sig && !el.hidden) return; el._sig = sig;
    el.innerHTML = `<span class="engLeftTxt" role="status"><b>${p.k}</b> more ${plural(p.k, 'piece', 'pieces')} of this order to approve</span><button type="button" class="btn ghost xs engLeftShow" data-g="show" title="${esc('Show ' + p.label)}">Show ${esc(p.label)}</button>`;
    el.hidden = false;
    const b = el.querySelector('[data-g=show]'); if (b) b.onclick = () => { const k = p.key || p.lineKey; tryDo(() => pick(k)); };
  }
  return { afterApprove, leaving, mount, clear, pointer, drawPointer, leftOf, OPEN };
});
