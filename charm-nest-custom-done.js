/* charm-nest-custom-done.js — THE CUSTOM PORTION DONE IN THE CHARM EDITOR (CPX1; Paul, 9 Nov 2026, order 4225890898).
 *
 *   Paul: "This specific order was completed using the charm editor, and that includes the Custom portion of this order. Once a user completes an order using the editor inside the pop-up
 *   modal there needs to be an option that also completes the Custom portion of a given order like this so that once it's done and the charm editor, it no longer requires the custom portion
 *   of such an order to be completed because it was already completed inside the charm editor. This needs to be clearly visible and not hidden somewhere in that modal, this is a very
 *   important option as it will release any custom order from being held for review."
 *
 * An order can carry a listing piece AND a Custom portion: an Etsy line that takes a design of its own (its Review card, Custom Orders: CharmNestCustomPiece.needsDesign), and / or a
 * custom piece sent to a sheet for the order (CustomOrders' row: pool rows custom: true, "Custom piece · design … · sent by …"). When the whole design was made in the Charm Editor on the
 * listing piece, that Custom portion is a duplicate: it holds the order in Review and, as a piece, would be cut a second time. The card after a piece's Save
 * (charm-nest-ce-masters.js afterPiece) shows, at its top, one tick per open Custom portion of the saved piece's order; confirmed (Done), they are completed here, as ONE action:
 *   · an Etsy custom line: its card's own Release Hold (CustomPrint.complete: its record, its ORDER RELEASED seal, its timeline step, Resolved): the Review hold of THAT line only;
 *   · a custom piece: taken off its sheet by the sheet's normal remove path (Orders.takeOffGone: the rest of the sheet stays as placed; a piece on no sheet of this page goes through the
 *     same take-off on the cloud, Pool.update), its pool rows marked coveredBy { poolId, lineKey, name, designId, version, by, at }: completed in the Charm Editor, on that piece;
 *     a piece whose sheet is cut, approved or sent is REFUSED, with the reason, before anything changes (nothing is half done);
 *   · the CUSTOM DONE IN EDITOR seal (the Seal component's face) on the card and on the piece's row in the order window;
 *   · every view drawn again through the paths that already draw them (Orders, the order window, Review, the custom piece strip): no new polling.
 * Undo (the toast's, for a while, and the order window's on the piece's row): the custom line reopened (CustomPrint.reopen) and the piece's pool rows live again, put back on the next
 * open sheet of its metal when this page still holds the piece (never on a cut sheet: CharmNestCustomPool.putBack picks an open one).
 *
 *   CharmNestCustomDone.openOf(rid, { savedKey, savedPoolIds }) -> [item]   the order's open Custom portions (the saved piece's own line left out)
 *       item = { id, kind: 'line' | 'piece', rid, key, name, wMm, hMm, poolIds, sheets: [{ id, name, why }], why, row, it, designId, version, metal }
 *   .ticked(items, { sameOrder }) -> boolean                                   ticked at first: exactly one Custom portion, and the saved piece is of the same order
 *   .blockOf(sheet) -> '' | why                                                 a sheet a piece cannot be taken off now (cut, approved, sent, being nested or saved)
 *   .complete({ rid, items, cover: { poolId, lineKey, name, designId, version }, by }) -> Promise<{ ok, why?, done, token }>
 *   .undo(token) -> Promise<{ ok, why? }>          .coverOf(x) -> cover | null (a pool row, a CustomOrders row, a line key)
 *   .blockHtml(items, o) / .wire(node, items)      the card's block (charm-nest-ce-masters.js)      .rowHtml(cover, o) / .sealHtml(cover)   the order window's row
 *
 * UMD: window.CharmNestCustomDone in the page; module.exports in node (the same functions, the page looked up when they run). */
(function (root, factory) {
  const api = factory(root);
  if (typeof module === 'object' && module.exports) module.exports = api;
  if (root) root.CharmNestCustomDone = api;
})(typeof self !== 'undefined' ? self : typeof globalThis !== 'undefined' ? globalThis : this, function (root) {
  'use strict';
  const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const tryDo = (f, d) => { try { const v = f(); return v === undefined ? d : v; } catch (_) { return d; } };
  const num = v => (Number.isFinite(+v) ? +v : 0);
  const GONE = new Set(['abandoned', 'superseded']);
  const REASON = 'completed in the Charm Editor';
  const UNDO_MS = 20000;
  const ridOf = x => String(x == null ? '' : x).trim();
  // (the page's own globals: a const of its inline script is no property of window, so each is reached by name, guarded)
  /* eslint-disable no-undef */
  const sheetsNow = () => tryDo(() => (root.__cdSheets ? root.__cdSheets() : typeof allSheets === 'function' ? allSheets() : []), []) || [];
  const toastNow = (t, k, ms) => { try { if (root.__cdToast) return root.__cdToast(t, k, ms); if (typeof toast === 'function') toast(t, k || '', ms || 6000); } catch (_) { /* said in the log */ } };
  /* eslint-enable no-undef */
  const B = () => root.B || null;
  const poolRow = id => tryDo(() => B().pool.rows.get(String(id)), null);
  const ordersRows = () => tryDo(() => root.Orders.rows(), []) || [];

  /* ── what the order still has open ── */
  /** The cover a piece carries (its pool rows' coveredBy, not undone), or null. x: a pool row, a CustomOrders row (its pool ids), a line key. */
  function coverOf(x) {
    if (!x) return null;
    const one = p => (p && p.coveredBy && typeof p.coveredBy === 'object' && num(p.coveredBy.at) > 0 && !num(p.coveredBy.undoneAt) ? p.coveredBy : null);
    if (typeof x === 'string') {
      const r = tryDo(() => (root.CustomOrders ? root.CustomOrders.rows() : []).find(y => y && y.key === x), null);
      return r ? coverOf(r) : null;
    }
    if (x.poolId && !x.poolIds) return one(x) || one(poolRow(x.poolId));
    if (x.custom && x.spec && x.spec.custom && x.spec.custom.coveredBy) return x.spec.custom.coveredBy;
    for (const id of x.poolIds || []) { const c = one(poolRow(id)); if (c) return c; }
    return null;
  }
  /** Where a custom piece is on the sheets this page holds: [{ sh, ids }]. */
  function pagesOf(poolIds) {
    const want = new Set((poolIds || []).map(String)), out = [];
    for (const sh of sheetsNow()) { const ids = (sh.charms || []).filter(c => c && want.has(String(c.poolId))).map(c => String(c.poolId)); if (ids.length) out.push({ sh, ids }); }
    return out;
  }
  const committed = sh => tryDo(() => (root.Sets && root.Sets.ofRun ? root.Sets.ofRun(sh.runId) : []).some(s => s && s.committedAt && (s.sheetIds || []).includes(sh.sheetId)), false);
  /** Why a piece cannot come off this sheet now ('' when it can). */
  function blockOf(sh) {
    if (!sh) return '';
    if (num(sh.laserDoneAt) > 0 || num(sh.roseCutAt) > 0) return 'it is already cut on the laser';
    if (sh.recalled || committed(sh)) return 'its set was already sent to the station';
    if (num(sh.releasedAt) > 0 || num(sh.approvedAt) > 0 || num(sh.laserReadyAt) > 0 || sh.status === 'released') return 'it is already approved for laser cutting';
    if (['nesting', 'finishing', 'queued'].includes(sh.status)) return 'it is being nested right now: try again in a moment';
    if (sh.persisted && !sh.persistedDone) return 'it is being saved right now: try again in a moment';
    return '';
  }
  const nameOfSheet = sh => tryDo(() => (root.CharmNestSheetName && root.CharmNestSheetName.name ? root.CharmNestSheetName.name(sh) : '') || (typeof sheetName === 'function' ? sheetName(sh) : ''), '') || (sh && (sh.name || sh.sheetId)) || 'its sheet';   // eslint-disable-line no-undef
  /** A piece on a saved sheet this page does not hold: the sheet its pool row names, read from what OrderPieces knows (cut or not). */
  function farSheet(poolIds) {
    for (const id of poolIds) {
      const p = poolRow(id); if (!p || !p.sheetId || GONE.has(p.state)) continue;
      const pl = tryDo(() => (root.OrderPieces && root.OrderPieces.placeOf ? root.OrderPieces.placeOf(id) : null), null);
      return { id: String(p.sheetId), name: (pl && pl.sheetLabel) || p.sheetName || String(p.sheetId), why: pl && (pl.state === 'cut' || pl.cut) ? 'it is already cut on the laser' : '' };
    }
    return null;
  }
  const needsDesign = (row, it) => tryDo(() => (root.CharmNestCustomPiece && root.CharmNestCustomPiece.needsDesign ? root.CharmNestCustomPiece.needsDesign(row, it) : false), false);

  /** The order's open Custom portions. o.savedKey: the saved piece's line (never offered); o.savedPoolIds: its pool ids. */
  function openOf(rid, o) {
    rid = ridOf(rid); o = o || {}; if (!rid) return [];
    const saved = new Set([o.savedKey].concat(o.savedPoolIds || []).filter(Boolean).map(String)), out = [];
    // an Etsy line that takes a design of its own, its Review card still open
    for (const r of ordersRows()) {
      if (!r || !r.order || ridOf(r.order.receiptId) !== rid || r.custom || saved.has(String(r.key)) || r.state === 'gone') continue;
      if ((r.poolIds || []).length) continue;   // (a line already on its way to the laser is cut there: CustomPrint.complete leaves it)
      const it = tryDo(() => (root.Review && root.Review.customItemFor ? root.Review.customItemFor(r.key, r) : null), null);
      if (!it || it.done || !needsDesign(r, it)) continue;
      const x = r.spec && r.spec.special;
      out.push({ id: 'line:' + r.key, kind: 'line', rid, key: r.key, name: (x && x.label) || (r.line && r.line.title) || 'Custom order', wMm: null, hMm: null, poolIds: [], sheets: [], why: '', row: r, it, metal: (r.spec && r.spec.material) || null });
    }
    // a custom piece sent for the order (CustomOrders' row), not covered yet
    for (const r of tryDo(() => (root.CustomOrders ? root.CustomOrders.rowsOf(rid) : []), []) || []) {
      if (!r || !r.custom || saved.has(String(r.key)) || coverOf(r) || r.state === 'gone') continue;
      const ids = (r.poolIds || []).map(String).filter(id => { const p = poolRow(id); return !(p && GONE.has(p.state)); });
      if (!ids.length || ids.some(id => saved.has(id))) continue;
      const cu = (r.spec && r.spec.custom) || {}, here = pagesOf(ids), sheets = here.map(x => ({ id: x.sh.sheetId || null, name: nameOfSheet(x.sh), why: blockOf(x.sh), sh: x.sh, ids: x.ids }));
      if (!sheets.length) { const far = farSheet(ids); if (far) sheets.push(far); }
      const bad = sheets.find(s => s.why);
      out.push({ id: 'piece:' + r.key, kind: 'piece', rid, key: r.key, name: cu.name || (cu.designId ? 'design ' + cu.designId : 'Custom piece'), wMm: cu.widthMm != null ? +cu.widthMm : null, hMm: cu.heightMm != null ? +cu.heightMm : null,
        poolIds: ids, sheets, why: bad ? `${bad.name}: ${bad.why}` : '', row: r, it: null, designId: cu.designId || null, version: num(cu.version) || null, metal: r.material || null, by: cu.by || '' });
    }
    return out;
  }
  /** Ticked when the card opens: exactly one Custom portion, and the saved piece belongs to the same order. Never applied without the person's Done. */
  const ticked = (items, o) => !!(o && o.sameOrder) && (items || []).filter(i => !i.why).length === 1 && (items || []).length === 1;

  /* ── the action ── */
  const undos = new Map();   // token -> { rid, cover, by, lines: [{ it, row }], pieces: [{ ids, charms, metal, before }], at, timer }
  let seq = 0;
  /** Everything ticked completed as ONE action: refused whole (nothing changed) when a piece's sheet cannot give it up. → { ok, why?, done: [item], token } */
  async function complete(o) {
    o = o || {}; const rid = ridOf(o.rid), items = (o.items || []).filter(Boolean), by = String(o.by || '').trim(), cover0 = o.cover || {};
    if (!rid || !items.length) return { ok: false, why: 'nothing was ticked', done: [] };
    if (!by) return { ok: false, why: 'nobody is signed in: sign in, then press it again', done: [] };
    // 1 · every piece checked first, on what this page holds now: one that cannot come off, and nothing is done
    for (const i of items) if (i.kind === 'piece') {
      const now = pagesOf(i.poolIds), bad = now.map(x => ({ n: nameOfSheet(x.sh), w: blockOf(x.sh) })).find(x => x.w) || (i.why ? { n: '', w: i.why } : null);
      if (bad) return { ok: false, why: `The Custom piece “${i.name}” stays: ${bad.n ? bad.n + ': ' : ''}${bad.w}. Nothing was changed.`, done: [] };
    }
    const at = Date.now(), cover = { poolId: String(cover0.poolId || ''), lineKey: String(cover0.lineKey || ''), name: String(cover0.name || 'the saved piece').slice(0, 80), designId: cover0.designId ? String(cover0.designId) : null, version: num(cover0.version) || null, by, at };
    const token = 'cd' + (++seq) + '.' + at.toString(36), job = { rid, cover, by, lines: [], pieces: [], at, timer: 0 }, done = [];
    // 2 · the pieces: off their sheets by the sheet's own remove path, their records saying what covered them
    const patch = { removedBy: by, removedReason: REASON, removedAt: at, coveredBy: cover };
    for (const i of items.filter(x => x.kind === 'piece')) {
      const before = new Map(i.poolIds.map(id => [id, Object.assign({}, poolRow(id) || {})])), charms = [];
      const O = root.Orders;
      let taken = [];
      if (O && typeof O.takeOffGone === 'function' && pagesOf(i.poolIds).length) {
        taken = await O.takeOffGone([{ order: { receiptId: rid }, poolIds: i.poolIds.slice(), reason: REASON }], { patch, why: REASON, by, at, strict: true, before: (sh, mine) => { for (const c of mine) charms.push(c); } });
      }
      const off = new Set((taken || []).flatMap(t => t.poolIds || [])), rest = i.poolIds.filter(id => !off.has(id));
      if (rest.length) {
        if (!root.Pool || typeof root.Pool.update !== 'function') throw new Error('the pool cannot be written on this page');
        const res = await root.Pool.update(rest, Object.assign({ state: 'abandoned', sheetId: null, setId: null }, patch));
        if (res && Array.isArray(res.kept) && res.kept.length) toastNow(`${res.kept.length} piece${res.kept.length === 1 ? '' : 's'} of “${i.name}” stay on a cut sheet: set ${res.kept.length === 1 ? 'it' : 'them'} aside`, 'bad', 9000);
      }
      // (the records this page shows say it at once: the Orders list, the order window and Review read CustomOrders' rows)
      const now = i.poolIds.map(id => Object.assign({}, before.get(id), { poolId: id, state: 'abandoned', sheetId: null, setId: null, updatedAt: at }, patch));
      tryDo(() => { if (root.CustomOrders && root.CustomOrders.note) root.CustomOrders.note(now); });
      job.pieces.push({ ids: i.poolIds.slice(), charms, metal: i.metal, before: [...before.values()], name: i.name });
      done.push(i);
    }
    // 3 · the Etsy custom lines: their own Release Hold (the Review hold of that line only; other holds stay)
    for (const i of items.filter(x => x.kind === 'line')) {
      const C = root.CustomPrint; if (!C || typeof C.complete !== 'function' || !i.it) continue;
      const ok = await Promise.resolve(C.complete(i.it)).catch(e => { console.warn('[custom done] release', e); return false; });
      if (ok) { job.lines.push({ it: i.it, row: i.row }); done.push(i); }
    }
    if (!done.length) return { ok: false, why: 'the Custom portion could not be completed', done: [] };
    undos.set(token, job); job.timer = setTimeout(() => { const j = undos.get(token); if (j) j.expired = true; }, UNDO_MS);
    tryDo(() => { if (root.CNDesignLog && root.CNDesignLog.event) root.CNDesignLog.event('customDoneInEditor', { orderId: rid, by, coveredBy: cover, items: done.map(x => ({ kind: x.kind, key: x.key, poolIds: x.poolIds })) }); });
    tryDo(() => { if (typeof agent === 'function') agent({ bridge: true }, 'DS', `${rid}: the Custom portion (${done.map(x => x.name).join(', ')}) completed in the Charm Editor on ${cover.name} by ${by}`); });   // eslint-disable-line no-undef
    refresh(rid);
    return { ok: true, done, token, cover };
  }
  /** Undo: the custom lines reopened, the pieces live again (back on the next open sheet of their metal when this page holds them; never on a cut sheet). */
  async function undo(token) {
    const job = typeof token === 'string' ? undos.get(token) : token; if (!job) return { ok: false, why: 'too late to undo here' };
    undos.delete(typeof token === 'string' ? token : [...undos].find(([, v]) => v === job)?.[0]);
    clearTimeout(job.timer);
    const back = Date.now(), said = [];
    for (const p of job.pieces) {
      const patch = { state: 'ready', sheetId: null, setId: null, repooledAt: back, coveredBy: Object.assign({}, job.cover, { undoneAt: back, undoneBy: job.by }) };
      if (root.Pool && typeof root.Pool.update === 'function') await root.Pool.update(p.ids, patch);
      const rows = p.ids.map(id => Object.assign({}, (p.before.find(b => b.poolId === id) || {}), { poolId: id }, patch, { updatedAt: back }));
      tryDo(() => { const b = B(); if (b && b.pool && b.pool.rows) for (const r of rows) b.pool.rows.set(r.poolId, r); });
      tryDo(() => { if (root.CustomOrders && root.CustomOrders.note) root.CustomOrders.note(rows); });
      const CP = root.CharmNestCustomPool;
      if (p.charms.length && CP && typeof CP.putBack === 'function') { const where = tryDo(() => CP.putBack(p.charms, p.metal, job.rid), null); said.push(`“${p.name}” is back${where ? ' on ' + where : ''}`); }
      else said.push(`“${p.name}” is live again: send it to a sheet from its order`);
    }
    for (const l of job.lines) { const C = root.CustomPrint; if (C && typeof C.reopen === 'function') tryDo(() => C.reopen(l.it)); }
    if (job.lines.length) said.push('its custom line is back in Review');
    tryDo(() => { if (root.CNDesignLog && root.CNDesignLog.event) root.CNDesignLog.event('customDoneUndone', { orderId: job.rid, by: job.by, coveredBy: job.cover }); });
    refresh(job.rid);
    return { ok: true, said };
  }
  /** The latest undo this page can still make for an order's piece (the order window's Undo), or null. */
  const undoFor = (rid, key) => { for (const [t, j] of [...undos].reverse()) if (j.rid === ridOf(rid) && (!key || j.pieces.some(p => p.ids.some(id => String(id).startsWith(String(key) + '_') || id === key)) || j.lines.some(l => l.row && l.row.key === key))) return t; return null; };
  /** Every view of the order drawn again, by the paths that already draw them (nothing read from the cloud for it). */
  function refresh(rid) {
    tryDo(() => root.Orders && root.Orders.render && root.Orders.render());
    tryDo(() => { if (root.OrderWin && root.OrderWin.isOpen && root.OrderWin.isOpen()) root.OrderWin.paint(); });
    tryDo(() => root.Review && root.Review.render && root.Review.render());
    tryDo(() => root.CharmNestCustomPiece && root.CharmNestCustomPiece.repaint && root.CharmNestCustomPiece.repaint(rid));
    tryDo(() => root.document && root.document.dispatchEvent(new root.CustomEvent('cn-custom-done', { detail: { orderId: rid } })));
  }

  /* ── what it looks like ── */
  const SEAL_MODEL = c => ({ family: 'fulfilment', action: 'CUSTOM DONE IN EDITOR', icon: 'complete', at: num(c && c.at), by: String((c && c.by) || '') });
  const sealTitle = c => `Custom piece completed in the Charm Editor${c && c.name ? ' on ' + c.name : ''}${c && c.by ? ' by ' + c.by : ''}${c && c.at ? ' · ' + new Date(+c.at).toLocaleString() : ''}`;
  /** The CUSTOM DONE IN EDITOR seal (the Seal component's own face), or '' on a page without it. */
  function sealHtml(c, size) {
    const S = root.Seal; if (!S || typeof S.face !== 'function' || !c) return '';
    const svg = tryDo(() => S.face(SEAL_MODEL(c)), ''); if (!svg) return '';
    return `<span class="seal seal-customDone cdSeal" style="--rot:-4deg;--sz:${size || 40}px" data-at="${num(c.at)}" tabindex="0" role="img" aria-label="${esc(sealTitle(c))}" title="${esc(sealTitle(c))}">${svg}</span>`;
  }
  /** The order window's words for a covered piece: "Completed in the Charm Editor · on DOG 9667", the name a button that shows that piece. */
  function rowHtml(c, o) {
    o = o || {}; if (!c) return '';
    const on = c.lineKey && o.canShow ? `<button type="button" class="cdOn" data-cd-show="${esc(c.lineKey)}" title="Show ${esc(c.name)}">${esc(c.name)}</button>` : `<b>${esc(c.name)}</b>`;
    return `<span class="cdDone" data-cd-done>${sealHtml(c, o.size || 34)}<span class="cdDoneT">Completed in the Charm Editor · on ${on}${c.version ? ` <i>v${esc(c.version)}</i>` : ''}</span>${o.undo ? `<button type="button" class="btn ghost xs" data-cd-undo="${esc(o.undo)}">Undo</button>` : ''}</span>`;
  }
  const mm = v => (Number.isFinite(+v) ? (+v).toFixed(1) : '');
  /** The card's block: one tick per open Custom portion, at the top. o: { rid, ticked }. */
  function blockHtml(items, o) {
    o = o || {}; if (!items || !items.length) return '';
    const many = items.length > 1, on = o.ticked;
    const rows = items.map(i => {
      const size = i.wMm && i.hMm ? `${mm(i.wMm)} × ${mm(i.hMm)} mm` : '', where = i.sheets && i.sheets[0] ? `on ${i.sheets[0].name}` : i.kind === 'line' ? 'held in Review' : 'not on a sheet';
      const what = i.kind === 'line' ? 'Custom order line' : 'Custom piece';
      return `<label class="cdItem${i.why ? ' is-off' : ''}" data-cd-item="${esc(i.id)}"><input type="checkbox" data-cd-tick="${esc(i.id)}"${on && !i.why ? ' checked' : ''}${i.why ? ' disabled' : ''}>`
        + `<span class="cdPic" data-cd-pic="${esc(i.id)}" aria-hidden="true"><span class="cdNo">${i.kind === 'line' ? '' : 'no image'}</span></span>`
        + `<span class="cdTxt"><b>${esc(i.name)}</b><span>${[what, size, where].filter(Boolean).map(esc).join(' · ')}</span>${i.why ? `<span class="cdWhy">Cannot be completed here: ${esc(i.why)}</span>` : ''}</span></label>`;
    }).join('');
    return `<section class="cdBlock" data-cd-block role="group" aria-labelledby="cdT"><div class="cdHead"><span class="cdIco" aria-hidden="true">✓</span><div><h4 id="cdT">Also complete the Custom ${many ? 'pieces' : 'piece'} of order #${esc(o.rid || items[0].rid)}</h4>`
      + `<p>The Custom design was done here. ${many ? 'Each piece ticked is' : 'Its custom piece is'} marked complete, taken off its sheet, and the order is released from Review.</p></div></div>`
      + `<div class="cdList">${rows}</div><p class="cdFoot" data-cd-foot aria-live="polite"></p></section>`;
  }
  /** The ticks' life in the card: the pictures drawn, the Done button saying what it does. → { picked() -> [item] } */
  function wire(node, items, o) {
    o = o || {};
    const picked = () => items.filter(i => { const b = node.querySelector(`[data-cd-tick="${cssEsc(i.id)}"]`); return !!(b && b.checked && !b.disabled); });
    const sync = () => { const n = picked().length, f = node.querySelector('[data-cd-foot]'); if (f) f.textContent = n ? `Done also completes ${n === 1 ? 'it' : n + ' of them'}.` : 'Not ticked: Done leaves the Custom portion as it is.'; if (typeof o.onChange === 'function') o.onChange(n); };
    node.addEventListener('change', e => { if (e.target && e.target.matches && e.target.matches('[data-cd-tick]')) sync(); });
    for (const i of items) pictureInto(node.querySelector(`[data-cd-pic="${cssEsc(i.id)}"]`), i);
    sync();
    return { picked, sync };
  }
  const cssEsc = s => String(s).replace(/["\\]/g, '\\$&');
  /** The piece's picture beside its tick: a custom piece's design as made (CharmNestCustomPiece's own drawing of it), an Etsy line's listing photo. */
  function pictureInto(host, i) {
    if (!host) return;
    if (i.kind === 'line') { const url = tryDo(() => root.Orders && root.Orders.imageFor ? root.Orders.imageFor(i.row) : null, null); if (url) host.innerHTML = `<img alt="" src="${esc(url)}">`; return; }
    const D = root.CharmNestDesigns, CP = root.CharmNestCustomPiece, draw = d => { const p = tryDo(() => CP._t.previewSvg(d, { fitMm: [12, 12] }), null); if (p && p.svg && host.isConnected) host.innerHTML = p.svg; };
    if (!i.designId || !D || !CP || !CP._t || typeof CP._t.previewSvg !== 'function') return;
    const local = tryDo(() => (D.peek ? D.peek(i.designId, i.version) : null), null);
    if (local && local.faces) { draw(local); return; }
    if (typeof D.get === 'function') Promise.resolve(D.get(i.designId, i.version)).then(d => { if (d && d.faces) draw(d); }, () => {});
  }
  const CSS = `
.cdBlock{margin:2px 0 12px;padding:12px 14px;border-radius:12px;background:#f3f8f2;border:1.5px solid #6f9a68;box-shadow:0 1px 0 rgba(0,0,0,.03)}
.cdHead{display:flex;gap:10px;align-items:flex-start}.cdIco{flex:none;width:26px;height:26px;border-radius:50%;display:grid;place-items:center;background:#2f6b3a;color:#fff;font:700 15px system-ui,sans-serif}
.cdBlock h4{margin:0;font:700 14.5px/1.3 system-ui,-apple-system,Segoe UI,sans-serif;color:#1f4a27}.cdBlock p{margin:3px 0 0;font:12.5px/1.4 system-ui,-apple-system,Segoe UI,sans-serif;color:#3c5a39}
.cdList{display:grid;gap:6px;margin-top:10px;max-height:38vh;overflow:auto}.cdItem{display:grid;grid-template-columns:auto 44px minmax(0,1fr);gap:10px;align-items:center;padding:7px 9px;border-radius:9px;background:#fff;border:1px solid #cfe0cb;cursor:pointer}
.cdItem input{width:18px;height:18px;accent-color:#2f6b3a;margin:0}.cdItem.is-off{opacity:.75;cursor:not-allowed}
.cdPic{width:44px;height:44px;box-sizing:border-box;border-radius:8px;border:1px solid #dfe5dc;background:#fbfcfa;display:grid;place-items:center;overflow:hidden}.cdPic svg,.cdPic img{max-width:100%;max-height:100%}
.cdNo{font:600 8.5px system-ui,sans-serif;color:#8a948a;text-transform:uppercase;text-align:center}
.cdTxt{display:grid;gap:1px;min-width:0}.cdTxt b{font:600 13px system-ui,sans-serif;color:#1d2a1f;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.cdTxt span{font:11.5px system-ui,sans-serif;color:#5b6a5a}.cdTxt .cdWhy{color:#a4432c;font-weight:600}
.cdBlock .cdFoot{margin-top:8px;font-weight:600;color:#2f6b3a}
.cdDone{display:inline-flex;align-items:center;gap:8px;flex-wrap:wrap;font:12px system-ui,sans-serif;color:#2f6b3a}.cdDone .seal{flex:none}.cdDoneT i{font-style:normal;color:#5b6a5a}
.cdOn{all:unset;cursor:pointer;font-weight:700;text-decoration:underline;text-underline-offset:2px}.cdOn:focus-visible{outline:2px solid #2f6b3a;outline-offset:2px}
.owPcRow.cdCovered{opacity:1}.owPcRow.cdCovered .owPcName b{text-decoration:line-through;text-decoration-color:rgba(47,107,58,.5)}
@media (max-width:560px){.cdItem{grid-template-columns:auto 36px minmax(0,1fr)}.cdPic{width:36px;height:36px}}
`;
  function injectCss(doc) {
    doc = doc || root.document; if (!doc || doc.getElementById('cdCss')) return;
    const s = doc.createElement('style'); s.id = 'cdCss'; s.textContent = CSS; (doc.head || doc.documentElement).appendChild(s);
  }

  return { openOf, ticked, blockOf, complete, undo, undoFor, coverOf, refresh, sealHtml, rowHtml, blockHtml, wire, injectCss, REASON, SEAL_MODEL, _t: { pagesOf, undos }, version: 1 };
});
