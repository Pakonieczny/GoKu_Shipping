/* SetEdit: adding a sheet to, and taking a sheet out of, a set that is already committed (Paul, 7 Oct 2026, Library).
 *
 *   "The User should be able to add/remove individual sheets from a set of sheets even in the Laser cutting process as long as
 *    the below are true:
 *    A. That there are NO shared pieces from a multi-piece order on that particular sheet shared with other sheets in the same Set.
 *    B. The sheet has NOT been marked by the user as completed (Laser cut)."
 *
 * Until now a committed set (committedAt, or a status that starts with "complete") was fixed: it took no sheet and gave none up
 * ("press Undo set first"). What that protected, and where each protection lives now:
 *   · seals, approvals and cut records are permanent: an edit only changes membership fields and adds history; it never removes a
 *     seal, a stamp or a cut record (the server step writes none of them, and says so in its answer);
 *   · a sheet marked completed, or with a recorded Rose Gold cut, keeps its set (rule B: sheetCut, sheetCompleted);
 *   · sheets that share a multi-piece order are always in ONE set (the cardinal rule; rule A when taking out, the same rule with
 *     "together" when adding);
 *   · a completed set takes and gives nothing; a set is never left empty (it keeps its seals: Undo set is for that).
 * ONE rule, for the server and the page (charmNestLibrary.js requires this file; the page's plan reads it too):
 *   SetEdit.verifyMoves({ moves:[{id, to:<setId>|null}], recs:{id:record}, sets:{setId:{doc, members:[record]}}, others:[record] })
 *     → { ok, reasons:[{key,label,detail,items?,sheetId?,mates?}], shared:[{orderId,here,there,hereIds,thereIds}] }
 * Records are Library rows or sheet documents (id, metal, page, sheetIndex, setId, draft, solidIncluded, poolIds, orders,
 * laserDoneAt, roseCutAt, archived). Nothing here reads or writes the cloud. */
(function (root, factory) { const api = factory(root); if (typeof module === 'object' && module.exports) module.exports = api; else root.SetEdit = api; })(typeof self !== 'undefined' ? self : this, function (root) {
  'use strict';
  let SRnode = null; try { SRnode = require('./charm-nest-shared-orders.js').core; } catch (_) { SRnode = null; }
  const SR = () => root.CharmNestSharedOrders || SRnode;
  const CODE = { gold: 'GF', silver: 'SS', rose: 'RG', gold10k: '10K', gold14k: '14K' };
  const str = v => String(v == null ? '' : v);
  const idOf = r => str(r && (r.id || r.sheetId));
  const wordOf = r => `${CODE[r.metal] || r.metalLabel || ''} Sheet ${r.sheetIndex || r.page || 1}`.trim();
  const setWord = s => s && (s.seq || s.setSeq) ? `Set ${s.seq || s.setSeq}` : (s && s.name) || 'This set';
  const committedSet = d => !!(d && (+d.committedAt > 0 || /^complete/.test(str(d.status))));
  const inSetOf = r => (r && r.setId && !r.draft && r.solidIncluded !== false) ? str(r.setId) : null;
  const isDone = r => !!(r && +r.laserDoneAt > 0);
  const finished = (set, members) => +(set && set.laserDoneAt) > 0 || (members.length > 0 && members.every(isDone));
  const count = (n, one, more) => `${n} ${n === 1 ? one : (more || one + 's')}`;
  const orderIdsOf = r => [...new Set([].concat(Array.isArray(r.orders) ? r.orders : [], (Array.isArray(r.poolIds) ? r.poolIds : []).map(k => (/^(\d{1,30})_/.exec(str(k)) || [])[1])).map(str).filter(x => /^\d{1,30}$/.test(x)))];

  /** Rule B: why a sheet cannot change set at all ({key,label,detail}), or null. The same words the plan used before. */
  function cutReason(rec) {
    const w = wordOf(rec);
    if (rec.roseCutAt) return { key: 'sheetCut', label: `${w} was already cut`, detail: 'A recorded cut is permanent: a cut sheet stays in the set it was cut in.' };
    if (isDone(rec)) return { key: 'sheetCompleted', label: `${w} is completed`, detail: 'It keeps its cut record and set until it is moved back to Laser cutting.' };
    return null;
  }
  const joinWords = labels => labels.length <= 1 ? (labels[0] || '') : `${labels.slice(0, -1).join(', ')} and ${labels[labels.length - 1]}`;
  /** "Order 4171450075 has pieces on GF Sheet 1 and SS Sheet 1: they stay in one set": orders with the same sheets are said together. */
  function sharedWords(items, tail) {
    const groups = new Map();
    for (const it of items) { const sheets = [...new Set([].concat(it.here, it.there))], k = sheets.join('|'); const g = groups.get(k) || { sheets, orders: [], words: [] }; g.orders.push(it.orderId); g.words.push(it.words || ''); groups.set(k, g); }
    // an order that knows what its pieces are says it ("has its left earring on GF Sheet 1 and its right earring on GF Sheet 2"); the others keep the plain words
    const parts = [...groups.values()].slice(0, 3).map(g => g.orders.length === 1 && g.words[0] ? `Order ${g.orders[0]} ${g.words[0]}` : `${g.orders.length === 1 ? 'Order ' + g.orders[0] + ' has' : 'Orders ' + g.orders.slice(0, 3).join(', ') + (g.orders.length > 3 ? ' and ' + (g.orders.length - 3) + ' more' : '') + ' have'} pieces on ${joinWords(g.sheets)}`);
    return `${parts.join('; ')}${groups.size > 3 ? '; and more' : ''}: ${tail || 'they stay in one set'}`;
  }

  /** The multi-piece orders the moves would separate (SharedOrders.between over where every sheet ends up): an order is split when a
   *  moving sheet holds a piece of it and another sheet that holds a piece stays in a different set (or in none, when the move is into a set). */
  function splitOrders(recs, moves, meta) {
    const S = SR(); if (!S) return [];
    const has = id => Object.prototype.hasOwnProperty.call(moves, id), seen = new Map();
    for (const r of recs) if (r && idOf(r) && !r.archived && !seen.has(idOf(r))) seen.set(idOf(r), r);
    const sheets = [...seen.values()].map(r => S.sheetOf(r, { label: wordOf(r), setId: has(idOf(r)) ? moves[idOf(r)] : inSetOf(r) })).filter(Boolean);
    const label = new Map(sheets.map(s => [s.id, s.label])), out = new Map();
    for (const dest of [...new Set(Object.values(moves))]) {
      const ids = Object.keys(moves).filter(id => moves[id] === dest);
      for (const it of S.between(sheets, ids, dest, meta ? { meta } : {})) {
        const o = out.get(it.orderId) || { orderId: it.orderId, here: [], there: [], hereIds: [], thereIds: [] };
        for (const id of it.hereIds) if (!o.hereIds.includes(id)) { o.hereIds.push(id); o.here.push(label.get(id)); }
        for (const id of it.thereIds) if (!o.thereIds.includes(id)) { o.thereIds.push(id); o.there.push(label.get(id)); }
        for (const k of ['kind', 'words', 'groups', 'total', 'pieces', 'locked']) if (it[k] !== undefined && (o[k] === undefined || k === 'words' || k === 'kind')) o[k] = it[k];
        out.set(it.orderId, o);
      }
    }
    return [...out.values()];
  }

  /** The one rule. See the head of this file. */
  function verifyMoves(inp) {
    const reasons = [], moves = {}, recs = inp.recs || {}, sets = inp.sets || {}, real = [];
    for (const m of inp.moves || []) moves[str(m.id)] = m.to ? str(m.to) : null;
    const add = (key, label, detail, extra) => reasons.push(Object.assign({ key, label, detail }, extra || {}));
    const leaving = new Map(), joining = new Map();
    for (const id of Object.keys(moves)) {
      const r = recs[id];
      if (!r || r.archived) { add('missing', 'That sheet could not be found', 'It may have been removed. Refresh the Library.', { sheetId: id }); continue; }
      const from = inSetOf(r), to = moves[id];
      if (from === to) { delete moves[id]; continue; }
      const w = wordOf(r), c = cutReason(r);
      if (c) { add(c.key, c.label, c.detail, { sheetId: id }); continue; }
      if (to && r.metal === 'rose' && !from) { add('roseSet', `${w} is a Rose Gold sheet`, 'Rose Gold joins a set by its own Cut Sheet press, so it cannot be added to a committed set from here.', { sheetId: id }); continue; }
      real.push(id);
      if (from) {
        const F = sets[from];
        if (!F || !F.doc) add('noSet', `The set of ${w} could not be found`, 'Refresh the Library and try again.', { sheetId: id });
        else {
          if (!committedSet(F.doc)) add('setOpen', `${setWord(F.doc)} is still being made by its run`, 'Its sheets are changed from the open run, not from here.', { sheetId: id });
          else if (finished(F.doc, F.members || [])) add('setCompleted', `${setWord(F.doc)} is completed`, 'A completed set keeps its sheets.', { sheetId: id });
          if (!(F.members || []).some(x => idOf(x) === id) && !(F.doc.sheetIds || []).includes(id)) add('notInSet', `${w} is not in ${setWord(F.doc)}`, 'The set changed since this was planned. Refresh the Library.', { sheetId: id });
        }
        (leaving.get(from) || leaving.set(from, []).get(from)).push(id);
      }
      if (to) {
        const T = sets[to];
        if (!T || !T.doc) add('noSet', 'That set could not be found', 'Refresh the Library and try again.', { sheetId: id });
        else if (!committedSet(T.doc)) add('setOpen', `${setWord(T.doc)} is still being made by its run`, 'Its sheets are changed from the open run, not from here.', { sheetId: id });
        else if (finished(T.doc, T.members || [])) add('setCompleted', `${setWord(T.doc)} is completed`, 'A completed set takes no more sheets.', { sheetId: id });
        else if (/superseded/.test(str(T.doc.status))) add('setClosed', `${setWord(T.doc)} was replaced`, 'Drop it on a current set.', { sheetId: id });
        (joining.get(to) || joining.set(to, []).get(to)).push(id);
      }
    }
    // a set is never left without a sheet
    for (const [setId, ids] of leaving) {
      const F = sets[setId]; if (!F || !F.doc) continue;
      const stay = (F.members || []).filter(x => !ids.includes(idOf(x)) && !(joining.get(setId) || []).includes(idOf(x)));
      if (!stay.length && !(joining.get(setId) || []).length) add('lastSheet', `${ids.length === 1 ? wordOf(recs[ids[0]]) : 'These sheets'} ${ids.length === 1 ? 'is' : 'are'} all that is in ${setWord(F.doc)}`, `A set keeps at least one sheet: press Undo set on ${setWord(F.doc)} to take it apart.`, { sheetId: ids[0] });
    }
    if (reasons.length || !real.length) return { ok: reasons.length === 0, reasons, shared: [], noop: !real.length && !reasons.length };
    // the cardinal rule over where every sheet ends up
    const all = [].concat(Object.values(recs), ...Object.values(sets).map(s => s.members || []), inp.others || []);
    const shared = splitOrders(all, Object.fromEntries(real.map(id => [id, moves[id]])), inp.meta);
    // rule A is per sheet: two sheets that share an order do not leave their set together either (each one is checked on its own)
    for (const id of real) if (inSetOf(recs[id]) && !moves[id]) for (const it of splitOrders(all, { [id]: null }, inp.meta)) if (!shared.some(x => x.orderId === it.orderId)) shared.push(it);
    if (shared.length) {
      const leaves = real.some(id => !!inSetOf(recs[id])), mates = [...new Set(shared.flatMap(i => i.thereIds))].filter(id => !real.includes(id));
      add('sharedOrders', 'Orders shared with another sheet', sharedWords(shared, leaves ? 'they stay in one set' : 'they go into one set together'), { items: shared, mates });
    }
    return { ok: reasons.length === 0, reasons, shared };
  }

  /** The number a sheet gets in the set it joins: its own, unless a sheet of the same metal there already has it (the set numbers each metal 1, 2, 3). */
  function indexFor(rec, members) {
    const mine = +rec.sheetIndex || +rec.page || 0, taken = new Set((members || []).filter(m => idOf(m) !== idOf(rec) && m.metal === rec.metal).map(m => +m.sheetIndex || +m.page || 0));
    if (mine && !taken.has(mine)) return mine;
    return 1 + Math.max(0, ...taken);
  }

  /** The set's own records after the edit, from its record (`doc`) and the sheets that leave / join. Pure: `at`, `by` are the caller's. */
  function setAfter(doc, edit) {
    const leave = edit.leave || [], join = edit.join || [], gone = new Set(leave.map(idOf)), skus = edit.skus || {}, sides = edit.sides || {}, by = edit.by || '', at = +edit.at || 0;
    const sheetIds = [...new Set((doc.sheetIds || []).filter(i => !gone.has(str(i))).concat(join.map(idOf)))];
    const stay = (edit.members || []).filter(m => !gone.has(idOf(m)));
    const metals = [...new Set(stay.map(m => m.metal).concat(join.map(m => m.metal)).filter(Boolean))];
    const materials = [...(doc.materials || []).filter(m => metals.includes(m)), ...metals.filter(m => !(doc.materials || []).includes(m))];
    // the order map: copies on a sheet that leaves come off; copies of a sheet that joins go on (a line is kept per transaction)
    const orders = {};
    for (const [rid, o] of Object.entries(doc.orders || {})) {
      const lines = (Array.isArray(o && o.lines) ? o.lines : Object.values((o && o.lines) || {})).map(l => Object.assign({}, l, { copies: (l.copies || []).filter(c => !gone.has(str(c.sheetId))) })).filter(l => l.copies.length);
      if (lines.length) orders[rid] = Object.assign({}, o, { lines });
    }
    for (const j of join) {
      const name = j.fileBase || j.folder || wordOf(j);
      for (const key of [...new Set((j.poolIds || []).map(str))]) {
        const m = /^(\d{1,30})_(\d{1,30})_(\d{1,4})$/.exec(key); if (!m) continue;
        const [, rid, tid, copy] = m, o = orders[rid] = orders[rid] || { held: null, lines: [] };
        let line = o.lines.find(l => str(l.transactionId) === tid); if (!line) { line = { transactionId: tid, sku: skus[tid] || skus[key] || '', copies: [] }; o.lines.push(line); }
        if (!line.copies.some(c => c.poolId === key)) line.copies.push({ copy: +copy, sheetId: idOf(j), sheet: name, poolId: key, backPoolId: null, ...(sides[key] ? { side: sides[key] } : {}) });
      }
    }
    const labelFiles = (doc.labelFiles || []).filter(f => !gone.has(str(f.sheetId))).concat(join.flatMap(j => ((j.label && j.label.files) || []).map(f => Object.assign({ sheetId: idOf(j), sheet: j.fileBase || f.sheet || null, metal: j.metal || null }, f))));
    const out = { sheetIds, materials, orders, labelFiles };
    if (leave.length) {
      const orderGone = new Set(leave.flatMap(orderIdsOf)), kept = new Set(stay.concat(join).flatMap(orderIdsOf));
      const left = [...orderGone].filter(o => !kept.has(o));
      if (Array.isArray(doc.committed)) out.committed = doc.committed.filter(o => !left.includes(str(o)));
      out.committedOut = (Array.isArray(doc.committedOut) ? doc.committedOut : []).concat(left.filter(o => Array.isArray(doc.committed) && doc.committed.map(str).includes(o)).map(o => ({ orderId: o, at, by }))).slice(-300);
    }
    return out;
  }

  /** The set's order map ({rid:{lines:[...]|{tid:line}}}) as the manifest says it. sideTag: " (Left)" for a piece of a mismatched pair, '' for every other piece (so a
   *  set without one prints exactly as before). spanLines: one line per order whose pieces sit on more than one sheet of the set, the pieces told apart by side. */
  const sideTag = c => c && c.side === 'L' ? ' (Left)' : c && c.side === 'R' ? ' (Right)' : '';
  const linesOf = od => Array.isArray(od && od.lines) ? od.lines : Object.values((od && od.lines) || {});
  function spanLines(orders, nameOf) {
    const out = [];
    for (const [rid, od] of Object.entries(orders || {}).sort((a, b) => (a[0] < b[0] ? -1 : 1))) {
      const pieces = linesOf(od).flatMap(l => (l.copies || []).map(c => ({ sku: l.sku || '', copy: c.copy, side: c.side || null, sheet: c.sheet || (nameOf && nameOf(c.sheetId)) || str(c.sheetId), n: (l.copies || []).length })));
      if (new Set(pieces.map(p => p.sheet)).size < 2) continue;
      out.push(`${rid}  ${pieces.map(p => `${p.sku}${p.n > 1 ? '#' + p.copy : ''}${sideTag(p)} ${p.sheet}`).join('  ')}`);
    }
    return out;
  }
  /** The one sentence a refusal gives: the shared-orders sentence for the cardinal rule, else the label and its detail. */
  const sayWhy = v => { const r = (v && v.reasons || [])[0]; return r ? (r.key === 'sharedOrders' ? r.detail : `${r.label}. ${r.detail}`) : ''; };
  /* ── the page's side (browser only; the server never runs any of this) ────────────────────────────────────────────────────────────
   *   SetEdit.relabel({sheetIds, setId}, {by})     the QR labels of sheets just put in a committed set (the set's name and the sheet's number,
   *                                                 the code of the orders placed on it: what Sets.onSheetSaved makes), saved with the sheet
   *   SetEdit.remakeFiles({setId}, {by})           the set's labels PDF, manifest and set.json made again from the set as it is now, uploaded
   *                                                 to the same places as when it was committed, and written by op flowApply (step setFiles)
   *   SetEdit.applyMembership(membership)          what flowApply wrote (answer.membership) laid on the page's own copies at once: the open
   *                                                 run's sheet pages, the run's set records, the Library's cards and rows (the other screens
   *                                                 follow through the Library's live read, whose shape check sees a set's sheet list change)
   * Each throws (relabel, remakeFiles) with the reason in words; LibraryFlow reports it as a warning and keeps the saved membership. */
  const cloud = (body, label) => { const C = root.CN; if (!C || typeof C.api !== 'function') return Promise.reject(new Error('The Library is not connected to the cloud')); return C.api('charmNestLibrary', body, { label: label || undefined, quiet: !label }); };
  const sound = r => { if (r && r.error) throw Object.assign(new Error(r.error), { status: r.status }); return r; };
  const labelsMade = new Map();       // sheetId -> the label files relabel made (remakeFiles puts them in the set's list)
  async function relabel(step, o) {
    const C = root.CN, O = root.CharmNestOrders, Sx = root.Sets; if (!C || !O || !Sx || !Sx.renderLabelPng) throw new Error('the page is not ready');
    const set = sound(await cloud({ op: 'setGet', setId: str(step.setId) })).set; if (!set) throw new Error('the set could not be read');
    for (const id of step.sheetIds || []) {
      const rec = sound(await cloud({ op: 'getSheet', id: str(id) }, 'Reading the sheet')).sheet; if (!rec) throw new Error('a sheet could not be read');
      const byId = new Map((rec.charms || []).map(c => [c.id, c])), ids = [...new Set((rec.placements || []).map(p => byId.get(p.id)).filter(Boolean).map(c => str(c.order || c.id).split('/')[0]).filter(Boolean))];
      if (!ids.length) throw new Error(`${wordOf(rec)} has no order placed on it`);
      const metal = O.CARD_TO_METAL[rec.metal] || rec.metal, parts = O.safeChunks(ids, metal, 1000, 500, 8), name = rec.fileBase || rec.folder || rec.id, no = rec.sheetIndex || rec.page || 1;
      const base = rec.outputs && rec.outputs.ai && rec.outputs.ai.path ? rec.outputs.ai.path.replace(/\/[^/]*$/, '') : `charmnest/sheets/${rec.day}/${name}`, files = [];
      for (const [i, slice] of parts.entries()) {
        const payload = O.encodeOrderList(slice, metal);
        const label = `${(C.METAL_TAG && C.METAL_TAG[rec.metal]) || ''} · ${set.name} · Sheet ${no}${parts.length > 1 ? ` [${i + 1}/${parts.length}]` : ''} · ${slice.length} order${slice.length === 1 ? '' : 's'}`;
        const png = await Sx.renderLabelPng(payload, label);
        const up = await C.uploadBytes(`${base}/${name}_label${parts.length > 1 ? `_${i + 1}of${parts.length}` : ''}.png`, png.blob, 'image/png', 'Saving the sheet label');
        files.push({ path: up.path, url: up.url, sheet: name, sheetId: rec.id, metal, part: i + 1, parts: parts.length, orders: slice, payload, ecc: png.ecc, label });
      }
      const made = { files: files.map(f => ({ path: f.path, url: f.url, sheet: f.sheet, part: f.part, parts: f.parts, orders: f.orders, payload: f.payload, ecc: f.ecc, label: f.label })), orders: ids };
      sound(await cloud({ op: 'putSheet', sheet: { id: rec.id, label: made } }, 'Saving the sheet label'));
      labelsMade.set(rec.id, files);
      try { for (const pg of (C.allSheets ? C.allSheets() : [])) if (pg.sheetId === rec.id) pg.label = made; } catch (_) { /* the page's copy follows at its next load */ }
      try { if (root.SheetEvents && root.SheetEvents.qrLabel) root.SheetEvents.qrLabel({ sheetId: rec.id, sheet: wordOf(rec), setId: set.setId, metal: rec.metal }, ids, files, { set: set.name, by: (o && o.by) || undefined }); } catch (_) { /* a record of the work never stops the work */ }
    }
  }
  /** The set's files, the way Sets.finalize makes them, from the records instead of the open run's pages. */
  async function remakeFiles(step, o) {
    const C = root.CN, PL = root.PDFLib, A = root.CharmNestAssets; if (!C || !PL || !A || !A.bytes) throw new Error('the page is not ready');
    const set = sound(await cloud({ op: 'setGet', setId: str(step.setId) })).set; if (!set) throw new Error('the set could not be read');
    const own = new Set((set.sheetIds || []).map(str)), sheets = (sound(await cloud({ op: 'listSheets', setId: set.setId, limit: 300 }, 'Reading the set')).sheets || []).filter(s => own.has(s.id) && !s.archived);
    const files = (set.labelFiles || []).filter(f => !labelsMade.has(str(f.sheetId))).concat([...own].flatMap(id => labelsMade.get(id) || [])).filter(f => own.has(str(f.sheetId)) && f.url);
    files.sort((a, b) => str(a.sheet).localeCompare(str(b.sheet)) || (+a.part || 1) - (+b.part || 1));
    const { PDFDocument, StandardFonts } = PL, labels = await PDFDocument.create();
    for (const f of files) { const img = await labels.embedPng(await A.bytes(f.url)); labels.addPage([145, 145]).drawImage(img, { x: 0, y: 0, width: 145, height: 145 }); }
    const man = await PDFDocument.create(), font = await man.embedFont(StandardFonts.Helvetica), bold = await man.embedFont(StandardFonts.HelveticaBold);
    let page = man.addPage([612, 792]), y = 756;
    const ansi = t => str(t).replace(/→/g, '->').replace(/↔/g, '<->').replace(/·/g, '-').replace(/[^\x20-\x7E\xA0-\xFF]/g, '?');
    const line = (t, opts = {}) => { if (y < 48) { page = man.addPage([612, 792]); y = 756; } page.drawText(ansi(t).slice(0, 110), Object.assign({ x: 36, y, size: 9.5, font }, opts)); y -= opts.size ? opts.size + 4 : 13; };
    const orders = Object.entries(set.orders || {}), names = new Map(sheets.map(s => [s.id, s.fileBase || s.folder || wordOf(s)]));
    line(`${set.name} · ${set.day} · run ${set.runId}`, { size: 15, font: bold });
    line(`${own.size} sheet(s) · materials ${(set.materials || []).join(', ')} · ${orders.length} order(s) · edited after commit`); y -= 6;
    line('Orders and sheets', { font: bold, size: 11 });
    for (const [rid, od] of orders.sort((a, b) => (a[0] < b[0] ? -1 : 1))) { const lines = Array.isArray(od.lines) ? od.lines : Object.values(od.lines || {}); line(`${rid}  ${lines.flatMap(l => (l.copies || []).map(c => `${l.sku || ''}${(l.copies || []).length > 1 ? '#' + c.copy : ''}${sideTag(c)}->${c.sheet || names.get(c.sheetId) || ''}`)).join('  ')}`); }
    const spans = spanLines(set.orders, id => names.get(id));
    if (spans.length) { y -= 6; line('Orders on more than one sheet (they stay in one set)', { font: bold, size: 11 }); spans.forEach(t => line(t)); }
    y -= 6; line('Engraving', { font: bold, size: 11 });
    const backs = sheets.flatMap(s => (s.backs || []).filter(b => b && !b.invalidated).map(b => `${names.get(s.id)}: ${b.order || ''} ${b.sku || ''} #${b.copy || 1} "${str(b.text).replace(/\n/g, ' / ')}" ${b.capMm ? b.capMm + ' mm' : ''} · ${b.approvedBy || '?'}`));
    if (backs.length) backs.forEach(b => line(b)); else line('no engraving in this set');
    y -= 6; line('Labels', { font: bold, size: 11 }); files.forEach(f => line(`${f.label || f.sheet}  ${f.path || '(not uploaded)'}`));
    const edits = (Array.isArray(set.flowHistory) ? set.flowHistory : []).filter(h => h && h.type === 'setEdit');
    if (edits.length) { y -= 6; line('Changes after the commit', { font: bold, size: 11 }); edits.slice(-12).forEach(h => line(`${new Date(+h.at || 0).toISOString().slice(0, 16).replace('T', ' ')}  ${h.by || ''}: ${h.note || ''}`)); }
    const json = { setId: set.setId, runId: set.runId, day: set.day, seq: set.seq, name: set.name, folder: set.folder, materials: set.materials, sheets: sheets.map(s => ({ sheetId: s.id, name: names.get(s.id), metal: s.metal, sheetIndex: s.sheetIndex, orders: s.orders || [], placements: s.placedCount || 0, backs: (s.backs || []).map(b => ({ poolId: b.poolId, order: b.order, sku: b.sku, copy: b.copy, text: b.text, approvedBy: b.approvedBy })), verification: s.verification ? { ok: !!s.verification.ok } : null, outputs: s.outputs || null })),
      orders: Object.fromEntries(orders.map(([rid, od]) => [rid, { held: od.held || null, lines: Array.isArray(od.lines) ? od.lines : Object.values(od.lines || {}) }])), labels: files.map(f => ({ sheet: f.sheet, part: f.part, parts: f.parts, orders: f.orders, payload: f.payload, path: f.path })), approvals: backs.length, edited: edits.slice(-12), generatedAt: new Date().toISOString() };
    const folder = set.folder, up = (p, bytes, type, label) => C.uploadBytes(`${folder}/${p}`, bytes, type, label);
    const [lp, mp, jp] = await Promise.all([up(`labels/${set.name}_labels.pdf`, await labels.save({ useObjectStreams: false }), 'application/pdf', "Saving the set's labels PDF"), up(`${set.name}_manifest.pdf`, await man.save({ useObjectStreams: false }), 'application/pdf', 'Saving the manifest'), up('set.json', new TextEncoder().encode(JSON.stringify(json, null, 1)), 'application/json', 'Saving the set data')]);
    const mine = { pdf: { path: lp.path, url: lp.url }, manifest: { path: mp.path, url: mp.url }, json: { path: jp.path, url: jp.url }, files: files.map(f => ({ path: f.path, url: f.url, sheet: f.sheet })) };
    const r = sound(await cloud({ op: 'flowApply', steps: [{ type: 'setFiles', setId: set.setId, labelFiles: files, labels: mine }], by: (o && o.by) || '', device: 'charm-nest-1', via: 'Library move' }, 'Saving the set files'));
    for (const id of own) labelsMade.delete(id);
    applyMembership({ sheets: [], sets: [{ setId: set.setId, patch: { labelFiles: files, labels: mine } }] });
    return r;
  }
  /** Where the page keeps a set's order lines: { rid: { held, lines: { transactionId: line } } } (the server keeps the lines as a list). */
  const pageOrders = orders => Object.fromEntries(Object.entries(orders || {}).map(([rid, o]) => [rid, { held: (o && o.held) || null, lines: Array.isArray(o && o.lines) ? Object.fromEntries(o.lines.map(l => [l.transactionId, l])) : ((o && o.lines) || {}) }]));
  function applyMembership(m) {
    if (!m) return;
    const C = root.CN, B = root.B, sheets = m.sheets || [], sets = m.sets || [];
    // the open run's own pages of these sheets (the run's release rules read them: a sheet taken out is not put back by them)
    try {
      for (const pg of (C && C.allSheets ? C.allSheets() : [])) {
        const x = sheets.find(s => s.id === pg.sheetId); if (!x) continue;
        const p = x.patch || {};
        if ('draft' in p) pg.draft = !!p.draft;
        if ('setId' in p) pg.setId = p.setId || null;
        if ('setSeq' in p) pg.seq = p.setSeq || null;
        if ('sheetIndex' in p) pg.sheetIndex = p.sheetIndex || null;
        if ('label' in p) pg.label = p.label || null;
        if ('fileBase' in p && p.fileBase) pg.fileBase = p.fileBase;
        if ('releaseFull' in p) pg.releaseFull = !!p.releaseFull;
        if ('solidIncluded' in p && (pg.metal === 'gold10k' || pg.metal === 'gold14k')) pg.solidPick = p.solidIncluded === true;
      }
    } catch (_) { /* the page's copy follows at its next load */ }
    // the run's records of the sets, in the shape the page keeps them
    try {
      if (B && B.sets && typeof B.sets.values === 'function') for (const st of B.sets.values()) {
        const x = sets.find(s => s.setId === st.setId); if (!x) continue;
        const p = x.patch || {};
        if (p.sheetIds) st.sheetIds = p.sheetIds.slice();
        if (p.materials) st.materials = p.materials.slice();
        if (p.orders) st.orders = pageOrders(p.orders);
        if (p.labelFiles) st.labelFiles = p.labelFiles.slice();
        if ('labels' in p) st.labels = p.labels || null;
        if ('committed' in p) st.committed = p.committed;
        if (p.committedOut) st.committedOut = p.committedOut;
      }
    } catch (_) { /* as above */ }
    // the Library's own cards and rows (LibraryFlow.hooks.sync does the same for a seal or a hold)
    try {
      const cards = typeof document !== 'undefined' ? [...document.querySelectorAll('.setCard[data-laser-card]')] : [];
      for (const c of cards) {
        for (const s of c._sheets || []) { const x = sheets.find(q => q.id === s.id); if (x) Object.assign(s, x.patch); }
        const x = c._laserSet && sets.find(q => q.setId === c._laserSet.setId); if (x) Object.assign(c._laserSet, x.patch);
      }
      const rows = (C && C.S && C.S.library && C.S.library.rows) || [];
      for (const r of rows) { const x = sheets.find(q => q.id === r.id); if (x) Object.assign(r, x.patch); }
    } catch (_) { /* not shown */ }
    // everything that draws a Library list reads again at once (the shape of a set changed: the live read of the other screens sees it by itself)
    try { if (root.LaserReview && root.LaserReview.changed) root.LaserReview.changed(); } catch (_) { /* the next poll draws it */ }
    try { if (sheets.length && C && C.loadLibrary) Promise.resolve(C.loadLibrary()).catch(() => {}); } catch (_) { /* refreshed at its next read */ }
  }

  return { relabel, remakeFiles, applyMembership, verifyMoves, sayWhy, spanLines, sideTag, splitOrders, sharedWords, cutReason, indexFor, setAfter, wordOf, setWord, committedSet, inSetOf, finished, orderIdsOf, isDone };
});
