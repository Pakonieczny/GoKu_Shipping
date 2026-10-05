// The surfaces, the transitions and the runner of tests/charm-nest/placement-oracle.cjs (read that file's header first).
const fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..');

/* ═══════════════════════════ the surfaces: how each is opened, and what it says ═══════════════════════════
   open(page, ctx)   put the surface on the screen, as a person would        read(page, ctx) -> claims   what its words say now
   close(page)       put it away                                              page: 'viewer' | 'owner' | null (a server read: no page)
   A claim: { s: the surface and the part of it, n: the piece (its transaction number) or kind: 'order' for the order as a whole, text or dots or counts }.
   They are judged against the cloud's own truth (judge, in the other file) at the moment they are read. */
const R = {   // browser-side readers (they run in the page)
  ow: rid => {
    const T = e => e ? e.textContent.replace(/\s+/g, ' ').trim() : '', q = s => document.querySelector(s), qa = s => [...document.querySelectorAll(s)], out = [];
    if (!q('#owNow')) return [{ s: 'ow', gone: true }];
    out.push({ s: 'ow.header', kind: 'order', text: T(q('#owNow')), required: false });   // (the header names the last step reached ("Restored", "Hold released"); where the pieces are is said by the chips, the rows and the rail, which are asserted, and a header that says the wrong place is still wrong)
    const seal = q('#owNowCard .tlNowSeal'); if (seal) out.push({ s: 'ow.holdcard', kind: 'order', text: (seal.getAttribute('aria-label') || '').split(' · ')[0] });
    for (const c of qa('#owNowCard .owShChip, #owNowCard .owChip')) out.push({ s: 'ow.chip', kind: 'order', text: T(c) });
    for (const r of qa('#owPcSum .owPcRow[data-piece]')) { const n = +String(r.dataset.piece).split(/[:_]/)[1] - 5000000000; out.push({ s: 'ow.row', n, text: T(r.querySelector('.st')), required: 'sheet,held,cancelled,done,waiting' }); out.push({ s: 'ow.dots', n, dots: r.querySelectorAll('.steps i.on').length }); }
    const rail = T(q('#owRail .tlNowT'));
    if (!rail || /^(Finding|Reading|Loading)/i.test(rail)) out.push({ s: 'ow.rail', gone: true }); else out.push({ s: 'ow.rail', kind: 'order', text: rail, required: 'sheet,held,cancelled,done,waiting' });
    return out;
  },
  owSheet: rid => {
    const T = e => e ? e.textContent.replace(/\s+/g, ' ').trim() : '', q = s => document.querySelector(s), qa = s => [...document.querySelectorAll(s)], out = [];
    if (!q('#owSheetPanel')) return [{ s: 'owSheet', gone: true }];
    const tabs = qa('#owSheetPanel .owShTabs button'), on = tabs.find(b => b.classList.contains('on'));
    out.push({ s: 'owSheet.tabs', kind: 'order', text: tabs.length ? tabs.map(T).join(' + ') : T(q('#owSheetPanel .owPlateNone')), required: 'sheet', somePlaces: true });   // (a piece on no sheet has the tab greyed and nothing drawn: no claim)
    for (const li of qa('#owSheetPanel .owPieces li')) { const n = +((/TEST-(\d+)/.exec(T(li.querySelector('.sku'))) || [])[1]) || +String(T(li.querySelector('.sku'))).replace(/\D/g, ''); /* (a copy of a split piece is its line's: TEST-10, copy 2) */ let t =T(li.querySelector('em')); if (/^this sheet$/i.test(t) && on) t = T(on); out.push({ s: 'owSheet.piece', n, text: t, required: true, copy: true }); }
    return out;
  },
  list: rid => {
    const T = e => e ? e.textContent.replace(/\s+/g, ' ').trim() : '', qa = s => [...document.querySelectorAll(s)], out = [];
    const rows = qa('#ordItems [data-key]').filter(n => n.dataset.rid === rid);
    for (const r of rows) { const n = +String(r.dataset.key).split('_')[1] - 5000000000; out.push({ s: 'list.pill', n, text: T(r.querySelector('.ost')), required: 'sheet,held,cancelled,done' }); const where = r.querySelector('.rowExcerpt.dim'); if (where) out.push({ s: 'list.where', n, text: T(where) }); }
    const hold = document.querySelector('#ordChips [data-pile="hold"]');
    out.push({ s: 'list.holdPile', kind: 'holdPile', count: hold ? +(T(hold).match(/(\d+)\s*$/) || [0, 0])[1] : 0 });
    if (!rows.length) out.push({ s: 'list.rows', gone: true });
    return out;
  },
  lib: rid => {
    const T = e => e ? e.textContent.replace(/\s+/g, ' ').trim() : '', qa = s => [...document.querySelectorAll(s)], out = [];
    const cards = qa('#libBody .libCard[data-id]');
    for (const c of cards) {
      const m = c.querySelector('.m'), sp = m ? [...m.children] : [], placed = (T(sp[0]).split('/') || []), orders = parseInt(T(sp[2])), left = T(c.querySelector('.sheetBackStatus'));
      out.push({ s: 'lib.card', kind: 'count', sheetId: c.dataset.id, charms: +placed[1] || 0, orders: isNaN(orders) ? null : orders, backs: (left.match(/\/\s*(\d+)/) || [])[1] != null ? +left.match(/\/\s*(\d+)/)[1] : null });
    }
    out.push({ s: 'lib.sheets', kind: 'ids', ids: cards.map(c => c.dataset.id).sort() });
    return out;
  },
  sw: rid => {
    const T = e => e ? e.textContent.replace(/\s+/g, ' ').trim() : '';
    const dlg = window.SheetWin && SheetWin._W && SheetWin._W.dlg; if (!dlg || !dlg.open) return [{ s: 'sw', gone: true }];
    const txt = T(dlg), m = /(\d+)\s*charms?\s*(\d+)\s*orders?/.exec(txt);
    if (!m && /no longer in the Library|could not open/i.test(txt)) return [{ s: 'sw.counts', kind: 'count', sheetId: SheetWin.current(), deleted: true }];   // (the window says its sheet is gone)
    if (!m) return [{ s: 'sw.counts', gone: true, text: txt.slice(0, 200) }];   // (still opening the sheet)
    return [{ s: 'sw.counts', kind: 'count', sheetId: SheetWin.current(), charms: +m[1], orders: +m[2] }];
  },
  se: rid => {
    const T = e => e ? e.textContent.replace(/\s+/g, ' ').trim() : '', out = [];
    const card = [...document.querySelectorAll('.cnsCard')].find(n => T(n.querySelector('.cnsNum')).includes(rid));
    if (!card) return [{ s: 'se', gone: true }];
    for (const [k, sel] of [['pill', '.cnsPill'], ['now', '.cnsNow'], ['sheet', '.cnsSheet']]) { const e = card.querySelector(sel); if (e) out.push({ s: 'se.' + k, kind: 'order', text: T(e), required: k === 'now' ? 'sheet,held,cancelled,done,waiting' : false }); }   // (the pill names the card's place ("In the pull", "In the cloud"), which is true of a waiting, a held or a placed piece alike; the line under it must say where the pieces are)
    return out;
  },
  rv: rid => {
    const T = e => e ? e.textContent.replace(/\s+/g, ' ').trim() : '', out = [];
    const cards = [...document.querySelectorAll('#rvList .reviewListRow')].filter(n => n.dataset.rid === rid);
    if (!cards.length) return [{ s: 'rv', gone: true }];
    for (const c of cards) {
      const n = +String(c.dataset.row || '').split('_')[1] - 5000000000;
      out.push({ s: 'rv.queue', n, text: T(c.querySelector('.queueLabel')) });
      out.push({ s: 'rv.sheetButton', n, kind: 'openSheet', shown: !!c.querySelector('[data-cu-sheet]') });
    }
    return out;
  },
  so: o => {
    const dlg = document.querySelector('dialog.soDlg'); if (!dlg || !dlg.open) return [{ s: 'so', gone: true }];
    return [{ s: 'so.cards', kind: 'shared', sheetId: o.sheetId, targetSetId: o.targetSetId, rids: [...dlg.querySelectorAll('.soCard[data-order]')].map(c => c.dataset.order).sort() }];
  },
};
const SHEET_FOR_SW = 'sh-gf1', SO = { sheetId: 'sh-gf1', targetSetId: 'set-2' };
const VIEWS = [
  { id: 'ow', label: 'Order window · header, piece rows, dots, Timeline NOW', page: 'v',
    open: async (p, c) => { await p.evaluate(rid => { OrderWin.openOrder(rid); }, c.rid); await p.waitForSelector('#owPcSum .owPcRow', { timeout: 15000 }).catch(() => {}); },
    close: p => p.evaluate(() => { try { OrderWin.close(); } catch (_) {} }), read: (p, c) => p.evaluate(R.ow, c.rid) },
  { id: 'owSheet', label: 'Order window · Sheet tab', page: 'v',
    open: async (p, c) => { await p.evaluate(rid => { OrderWin.openOrder(rid); }, c.rid); await p.waitForSelector('#owPcSum .owPcRow', { timeout: 15000 }).catch(() => {}); await p.evaluate(() => OrderWin.setView('sheet')); await p.waitForSelector('#owSheetPanel', { timeout: 8000 }).catch(() => {}); },
    close: p => p.evaluate(() => { try { OrderWin.close(); } catch (_) {} }), read: (p, c) => p.evaluate(R.owSheet, c.rid) },
  { id: 'list', label: 'Orders list · pills, where, On hold pile', page: 'v',
    open: async (p, c) => { await p.evaluate(() => document.querySelector('#modeSeg [data-mode="orders"]').click()); await p.waitForSelector('#ordItems [data-key]', { timeout: 10000 }).catch(() => {}); },
    close: async () => {}, read: (p, c) => p.evaluate(R.list, c.rid) },
  { id: 'lib', label: 'Library · sheet cards (charms, orders, what is left)', page: 'v',
    open: async (p, c) => { await p.evaluate(() => document.querySelector('#modeSeg [data-mode="library"]').click()); await p.waitForSelector('#libBody .libCard[data-id]', { timeout: 15000 }).catch(() => {}); },
    close: async () => {}, read: (p, c) => p.evaluate(R.lib, c.rid) },
  { id: 'sw', label: 'Sheet window · charms and orders on the sheet', page: 'v',
    open: async (p, c) => { await p.evaluate(id => { SheetWin.open(id); }, SHEET_FOR_SW); await p.waitForFunction(() => SheetWin.isOpen(), null, { timeout: 8000 }).catch(() => {}); await p.waitForTimeout(600); },
    close: p => p.evaluate(async () => { try { await SheetWin.close(); } catch (_) {} await new Promise(r => setTimeout(r, 80)); }), read: (p, c) => p.evaluate(R.sw, c.rid) },   // (closed for good before the next scenario opens it: the window's own 'close' event has run)
  { id: 'se', label: 'Search · result card', page: 'v',
    open: async (p, c) => { await p.evaluate(rid => { OrderSearch.open(rid); }, c.rid); await p.waitForSelector('.cnsCard', { timeout: 8000 }).catch(() => {}); },
    close: p => p.evaluate(() => { try { OrderSearch.close(); } catch (_) {} }), read: (p, c) => p.evaluate(R.se, c.rid) },
  { id: 'so', label: 'Shared-orders window · orders that tie the sheet to its set', page: 'v',
    // (opened over the Library, where the sheets are being read: the window asks the page's own state, and that is how a person gets to it)
    open: async (p, c) => { await p.evaluate(() => document.querySelector('#modeSeg [data-mode="library"]').click()); await p.waitForSelector('#libBody .libCard[data-id]', { timeout: 15000 }).catch(() => {}); await p.evaluate(o => { SharedOrdersModal.open(o); }, SO); await p.waitForSelector('dialog.soDlg[open]', { timeout: 8000 }).catch(() => {}); await p.waitForTimeout(300); },
    close: p => p.evaluate(() => { try { SharedOrdersModal.close(); } catch (_) {} }), read: (p, c) => p.evaluate(R.so, SO) },
  { id: 'rv', label: 'Review · cards (words, Open sheet button)', page: 'v', when: sc => sc.lines.some(l => l.custom),
    open: async (p, c) => { await p.evaluate(() => document.querySelector('#modeSeg [data-mode="review"]').click()); await p.waitForSelector('#rvList .reviewListRow', { timeout: 8000 }).catch(() => {}); },
    close: async () => {}, read: (p, c) => p.evaluate(R.rv, c.rid) },
  { id: 'srv', label: 'Server · order timeline `where`, cancel check (what the stations read)', page: null,
    open: async () => {}, close: async () => {},
    read: async (p, c, srv) => {
      const t = await srv.call({ op: 'timelineGet', orderId: c.rid }), cc = await srv.call({ op: 'cancelCheck', orderIds: [c.rid] });
      const w = t.where || {};
      return [{ s: 'srv.where', kind: 'order', text: w.label || w.stage || '', required: false }, { s: 'srv.cancel', kind: 'cancelFlag', cancelled: !!(cc.cancelled && (Array.isArray(cc.cancelled) ? cc.cancelled.length : Object.keys(cc.cancelled).length)) }];
    } },
];

/* ═══════════════════════════ the transitions ═══════════════════════════ */
const ON2 = [{ on: 'sh-gf1' }, { on: 'sh-ss1', metal: 'silver' }];
const ownerEval = (x, fn, arg) => x.owner.page.evaluate(fn, arg);
const hold = async x => { const r = await ownerEval(x, rid => OrderHold.run(rid, { name: 'Paul' }).then(r => ({ ok: r.ok, held: r.held, error: r.error })), x.rid); if (!r.ok) throw new Error('Hold did not go through: ' + JSON.stringify(r)); };
const release = async x => { const r = await ownerEval(x, rid => OrderHold.release(rid, { name: 'Paul' }).then(r => ({ ok: r.ok, released: r.released, placed: r.placed, error: r.error })), x.rid); if (!r.ok && !r.released) throw new Error('Release did not go through: ' + JSON.stringify(r)); };   // (the fixture has no master design file: the release is made, the placing is not, and the pieces wait for a sheet: the cloud says which)
const takeOff = (x, o) => ownerEval(x, o => SheetWin.takeOffOrder(o).then(r => ({ ok: r.ok, error: r.error })), Object.assign({ orderId: x.rid, by: 'Paul', note: 'oracle' }, o)).then(r => { if (!r.ok) throw new Error('Take off did not go through: ' + JSON.stringify(r)); });
const custom = (x, n, how) => x.srv.call({ op: 'customPut', key: x.O.lineKey(x.rid, n), receiptId: x.rid, transactionId: String(x.O.tx(n)), sku: '', title: 'Custom piece ' + n, how, by: 'Paul' }).then(r => { if (r.error) throw new Error(r.error); });
const reopen = (x, n) => x.srv.call({ op: 'customReopen', key: x.O.lineKey(x.rid, n), by: 'Paul' }).then(r => { if (r.error) throw new Error(r.error); });
/** a piece put on a sheet, as another computer's nest saves it: the sheet record first, the pool row after */
async function addToSheet(x, sheetId, n, copy = 1) {
  const O = x.O, cur = x.srv.st.doc(O.SHEETS, sheetId), poolId = O.pid(x.rid, n, copy), SD = O.SHEET[sheetId], i = (cur.charms || []).length;
  const charms = (cur.charms || []).concat({ id: `${sheetId}-n${n}${copy}`, poolId, order: x.rid, name: `${x.rid} · TEST-${n}` }), placements = (cur.placements || []).concat({ id: `${sheetId}-n${n}${copy}`, cxPt: 30 + (i % 6) * 40, cyPt: 130, angle: 0, wPt: 28, hPt: 28 });
  let r = await x.srv.call({ op: 'putSheet', sheet: { id: sheetId, metal: SD.metal, charms, placements, poolIds: charms.map(c => c.poolId), orders: [...new Set(charms.map(c => c.order))], placedCount: placements.length, charmCount: charms.length } }); if (r.error) throw new Error(r.error);
  r = await x.srv.call({ op: 'poolUpdate', poolIds: [poolId], patch: { sheetId, setId: SD.set, state: 'written', sheetName: O.fileBase(SD) } }); if (r.error) throw new Error(r.error);
}
const deleteSheet = async (x, id) => { const r = await x.srv.call({ op: 'deleteSheet', id, code: process.env.CHARM_NEST_DELETE_CODE }); if (r.error) throw new Error(r.error); };
const must = r => { if (r && r.error) throw new Error(r.error); return r; };
/** A hold made on ANOTHER computer, as the sheet window writes it: the order's pool rows taken off (abandoned, heldAt), the sheets rewritten without its charms, and the "held" event. */
async function holdCloud(x) {
  const O = x.O, mine = O.cloudDocs({ sheets: [], orders: x.orders }).pool.filter(p => p.orderId === x.rid).map(p => p.poolId), now = Date.now();
  must(await x.srv.call({ op: 'poolUpdate', poolIds: mine, patch: { state: 'abandoned', sheetId: null, setId: null, sheetName: null, heldBy: 'Paul', heldReason: 'on hold', heldAt: now } }));
  for (const sh of x.srv.st.list(O.SHEETS)) {
    if (!(sh.charms || []).some(c => mine.includes(c.poolId))) continue;   // (the drawing: the server's take-off has already cut the pool ids from the record's list, the charms are what this page rewrites)
    const keep = (sh.charms || []).filter(c => !mine.includes(c.poolId));
    must(await x.srv.call({ op: 'putSheet', sheet: { id: sh._id, metal: sh.metal, charms: keep, placements: (sh.placements || []).filter(p => keep.some(c => c.id === p.id)), poolIds: keep.map(c => c.poolId), orders: [...new Set(keep.map(c => c.order))], placedCount: keep.length, charmCount: keep.length } }));
  }
  must(await x.srv.call({ op: 'timelineAdd', events: [{ orderId: x.rid, type: 'held', at: now, by: 'Paul', text: 'Put on hold by Paul' }] }));
}
/** A set sent to the station (committed) and the same set undone (Undo set): the pieces are on their sheets throughout; only the set's own status and the rows' step move. */
async function commitSet(x) {
  const O = x.O, mine = O.cloudDocs({ sheets: [], orders: x.orders }).pool.filter(p => p.orderId === x.rid).map(p => p.poolId), now = Date.now();
  must(await x.srv.call({ op: 'poolUpdate', poolIds: mine, patch: { state: 'committed' } }));
  must(await x.srv.call({ op: 'setUpdate', setId: 'set-1', patch: { status: 'committed', committedAt: now, committed: [x.rid] } }));
}
async function undoSet(x) {
  const O = x.O, mine = O.cloudDocs({ sheets: [], orders: x.orders }).pool.filter(p => p.orderId === x.rid).map(p => p.poolId);
  must(await x.srv.call({ op: 'setUpdate', setId: 'set-1', patch: { status: 'labelled', committedAt: null, committed: [] } }));
  must(await x.srv.call({ op: 'poolUpdate', poolIds: mine, patch: { state: 'written' } }));
}
/** The server's take-off alone (the page that pressed Hold closed before it rewrote the sheets): the pool rows taken off with their hold marks, nothing else. */
async function holdServerOnly(x) {
  const O = x.O, mine = O.cloudDocs({ sheets: [], orders: x.orders }).pool.filter(p => p.orderId === x.rid).map(p => p.poolId), now = Date.now();
  must(await x.srv.call({ op: 'poolUpdate', poolIds: mine, patch: { state: 'abandoned', sheetId: null, setId: null, sheetName: null, heldBy: 'Paul', heldReason: 'on hold', heldAt: now } }));
  must(await x.srv.call({ op: 'timelineAdd', events: [{ orderId: x.rid, type: 'held', at: now, by: 'Paul', text: 'Put on hold by Paul' }] }));
}
/** A release made on another computer: the rows are live again (waiting), or, with `place`, put back on a sheet (the nest placed them). */
async function releaseCloud(x, place) {
  const O = x.O, rows = O.cloudDocs({ sheets: [], orders: x.orders }).pool.filter(p => p.orderId === x.rid), now = Date.now();
  must(await x.srv.call({ op: 'poolPut', pools: rows.map(p => Object.assign({}, p, { state: 'ready', sheetId: null, setId: null, sheetName: null, createdAt: undefined, updatedAt: undefined })) }));   // (a row made up again: how Release has always re-pooled)
  must(await x.srv.call({ op: 'timelineAdd', events: [{ orderId: x.rid, type: 'released', at: now, by: 'Paul', text: 'Released by Paul' }] }));
  if (place) for (const [n, sheetId] of place) await addToSheet(x, sheetId, n);
}

const SCENARIOS = [
  { id: 'onsheet', about: 'on a sheet (each piece on its own sheet)', lines: ON2 },
  { id: 'waiting', about: 'waiting for a sheet (nothing placed yet)', lines: [{}, { metal: 'silver' }] },
  { id: 'hold', about: 'Hold pressed: both pieces come off their sheets', lines: ON2, act: hold },
  { id: 'hold-cloud', about: 'Hold pressed on ANOTHER computer (every page here only reads the cloud)', lines: ON2, by: 'cloud', act: holdCloud },
  { id: 'release-waiting', about: 'held, then released: the pieces wait for a sheet again', lines: ON2, by: 'cloud', pre: holdCloud, act: x => releaseCloud(x) },
  { id: 'release-placed', about: 'held, then released and placed on a sheet again', lines: ON2, by: 'cloud', pre: holdCloud, act: x => releaseCloud(x, [[10, 'sh-gf1'], [11, 'sh-ss1']]) },
  { id: 'release', about: 'held, then released through the real Release flow (no design file in the fixture: the pieces wait for a sheet)', lines: ON2, pre: hold, act: release },
  { id: 'cancel', about: 'cancelled (taken off its sheets, the cancel record kept)', lines: ON2, act: x => takeOff(x, { mode: 'cancel', scope: 'order' }) },
  { id: 'remove', about: 'one piece taken off its sheet (the other stays)', lines: ON2, act: x => takeOff(x, { mode: 'hold', scope: 'sheet', sheetId: 'sh-gf1' }) },
  { id: 'add', about: 'a waiting piece put on a sheet by another computer', lines: [{}, { metal: 'silver' }], by: 'cloud', act: x => addToSheet(x, 'sh-gf1', 10) },
  { id: 'hand-button', about: 'completed by hand (Complete Order), no sheet', lines: [{ custom: true }, { custom: true, metal: 'silver' }], act: async x => { await custom(x, 10, 'button'); await custom(x, 11, 'button'); } },
  { id: 'hand-print', about: 'completed by its QR label printed, no sheet', lines: [{ custom: true }, { custom: true, metal: 'silver' }], act: async x => { await custom(x, 10, 'print'); await custom(x, 11, 'print'); } },
  { id: 'hand-reopen', about: 'completed by hand, then reopened', lines: [{ custom: true }, { custom: true, metal: 'silver' }], pre: async x => { await custom(x, 10, 'button'); await custom(x, 11, 'print'); }, act: async x => { await reopen(x, 10); await reopen(x, 11); } },
  { id: 'split', about: 'one piece split over two sheets (a copy on each)', lines: [{ on: ['sh-gf1', 'sh-gf2'] }, { on: 'sh-ss1', metal: 'silver' }] },
  { id: 'split-remove', about: 'a split piece loses its copy on one sheet', lines: [{ on: ['sh-gf1', 'sh-gf2'] }, { on: 'sh-ss1', metal: 'silver' }], by: 'cloud',
    act: async x => { const O = x.O, id = 'sh-gf2', cur = x.srv.st.doc(O.SHEETS, id), gone = O.pid(x.rid, 10, 2), keep = (cur.charms || []).filter(c => c.poolId !== gone);
      let r = await x.srv.call({ op: 'putSheet', sheet: { id, metal: 'gold', charms: keep, placements: (cur.placements || []).filter(p => keep.some(c => c.id === p.id)), poolIds: keep.map(c => c.poolId), orders: [...new Set(keep.map(c => c.order))], placedCount: keep.length, charmCount: keep.length } }); if (r.error) throw new Error(r.error);
      r = await x.srv.call({ op: 'poolUpdate', poolIds: [gone], patch: { state: 'ready', sheetId: null, setId: null, sheetName: null } }); if (r.error) throw new Error(r.error); } },
  { id: 'deleted', about: 'the sheet record deleted (its pieces are on no sheet any more)', lines: ON2, by: 'cloud', act: x => deleteSheet(x, 'sh-gf1') },
  { id: 'hold-waiting', about: 'a piece that was waiting for a sheet is put on hold (never placed)', lines: [{}, { metal: 'silver' }], by: 'cloud', act: holdServerOnly },
  { id: 'setundo', about: 'a committed set undone (the pieces stay on their sheets)', lines: ON2, by: 'cloud', pre: commitSet, act: undoSet },
  // C3's finding 1: a Hold made on a computer that closed before it rewrote the sheets: only the server's own take-off (the pool rows and the sheet records' lists, one commit) is there.
  // The open sheet's own row (Sheet tab), the Library cards and the sheet window must say what the cloud says without waiting for a page that is gone.
  { id: 'hold-closed', about: 'Hold pressed on a computer that closed before it rewrote the sheets (the server\'s take-off alone)', lines: ON2, by: 'cloud', act: holdServerOnly },
  // C3's finding 2: search keeps a number found in the cloud for 120 s; an order that is in no pull on this page must still follow a hold within seconds
  { id: 'cloud-only', about: 'an order that is in no pull on this page (search finds it by number in the cloud): Hold pressed elsewhere', lines: ON2, by: 'cloud', cloudOnly: true, views: ['se', 'srv'], act: holdServerOnly },
  { id: 'cloud-only-deleted', about: 'the same order, its sheet deleted elsewhere', lines: ON2, by: 'cloud', cloudOnly: true, views: ['se', 'srv'], act: x => deleteSheet(x, 'sh-gf1') },
];

/* ═══════════════════════════ the runner ═══════════════════════════ */
async function main(O) {
  const argv = process.argv.slice(2), flag = n => argv.includes('--' + n), opt = n => { const i = argv.indexOf('--' + n); return i >= 0 ? argv[i + 1] : ''; };
  const ONLY = opt('only') ? new Set(opt('only').split(',')) : null, SURF = opt('surfaces') ? new Set(opt('surfaces').split(',')) : null, DUMP = flag('dump'), LIST = flag('list'), WINDOW = +(opt('window') || 3000);
  const pwDir = process.env.PW_DIR || [path.join(root, 'node_modules'), '/opt/node22/lib/node_modules/playwright/node_modules'].find(d => fs.existsSync(path.join(d, 'playwright-core')));
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  - no playwright-core: the oracle was not run'); return; }
  const exe = process.env.CHROMIUM || (fs.existsSync('/opt/pw-browsers/chromium-1194/chrome-linux/chrome') ? '/opt/pw-browsers/chromium-1194/chrome-linux/chrome' : undefined);
  const { sleep } = O, allViews = VIEWS.filter(v => !SURF || SURF.has(v.id)), scenarios = SCENARIOS.filter(s => !ONLY || ONLY.has(s.id));
  const srv = await O.backend(), browser = await chromium.launch(Object.assign({ args: ['--no-sandbox'] }, exe ? { executablePath: exe } : {}));
  const results = [];   // { scenario, phase, view, who, ok, ms, issues, claims }
  const t00 = Date.now(), log = (...a) => console.log(((Date.now() - t00) / 1000).toFixed(1).padStart(6) + 's', ...a);
  try {
    log('opening the pages (one owner, one viewer per surface)');
    const pageViews = allViews.filter(v => v.page === 'v');
    const owner = await O.openPage(browser, srv, { owner: true, name: 'owner' }), viewers = [];
    for (const v of pageViews) viewers.push(await O.openPage(browser, srv, { owner: false, name: v.id }));   // (one after another: eight pages booting together starve each other)
    const viewerOf = Object.fromEntries(pageViews.map((v, i) => [v.id, viewers[i]]));
    const allPages = [owner, ...viewers];
    let serial = 0;
    for (const sc of scenarios) {
     try {
      const rid = String(4182000000 + ++serial), subj = O.subject(rid, sc.lines), orders = [...O.FILLER, subj], spec = O.specOf(orders), ctx = { rid, subj, orders };
      const views = allViews.filter(v => (!v.when || v.when(sc)) && (!sc.views || sc.views.includes(v.id)));   // (a surface that has nothing to say about this order is not asked: Review has cards only for pieces made by hand)
      const X = { O, srv, rid, owner, spec, orders }, mine = sc.by !== 'cloud';   // (by 'cloud': the change is made by another computer, so the owner page here is one more reader)
      log(`── ${sc.id}: ${sc.about}`);
      // the shop as the cloud holds it, then every page told about the pulled lines (the owner also holds the run's own state)
      for (const p of allPages) await Promise.all(views.filter(v => v.page === 'v' && viewerOf[v.id] === p).map(v => v.close(p.page, ctx)));
      await owner.page.evaluate(() => { try { OrderWin.close(); SheetWin.close(); OrderSearch.close(); SharedOrdersModal.close(); } catch (_) {} });
      O.seedCloud(srv, spec);
      // (cloudOnly: the order is in the cloud and in no pull this page holds, so the search finds it only by looking its number up in the cloud)
      const pulled = sc.cloudOnly ? O.specOf(orders.filter(o => o !== subj)) : spec;
      await Promise.all(allPages.map(p => O.seedPage(p.page, pulled, { owner: p.owner && mine })));
      if (sc.pre) { await sleep(2500); await sc.pre(X); await settle(srv); }
      await Promise.all(views.filter(v => v.page === 'v').map(v => v.open(viewerOf[v.id].page, ctx)));
      // BEFORE: what each surface says of the shop as it stands (a surface opened on a settled cloud must already be right, within 3 s)
      const before = await converge(O, srv, views, viewerOf, ctx, WINDOW, 'before', sc, DUMP);
      results.push(...before);
      if (sc.act) {
        await sleep(1200);                                   // (the page has its first read behind it: the change comes as a person makes it)
        const t0 = Date.now();
        await sc.act(X);
        const settledAt = await settle(srv);
        log(`   changed (${((Date.now() - t0) / 1000).toFixed(1)} s, cloud quiet since ${((Date.now() - srv.lastWrite) / 1000).toFixed(1)} s)`);
        const after = await converge(O, srv, views, viewerOf, ctx, WINDOW, 'after', sc, DUMP, settledAt);
        results.push(...after);
        // --shots <dir>: what each surface shows once the change has landed (fixture pages only: the fake backend, never the shop)
        if (opt('shots')) { fs.mkdirSync(opt('shots'), { recursive: true }); for (const v of views.filter(v => v.page === 'v')) await viewerOf[v.id].page.screenshot({ path: path.join(opt('shots'), `${sc.id}-${v.id}.png`) }).catch(() => {}); }
      }
      // the owner's own surfaces, one after another (the person who pressed the button sees them first): right at once
      const own = [];
      if (mine) for (const v of views.filter(v => v.page === 'v')) {
        await v.open(owner.page, ctx);
        const r = await converge(O, srv, [v], { [v.id]: owner }, ctx, WINDOW, sc.act ? 'after' : 'before', sc, DUMP, null, true);
        own.push(...r.map(x => Object.assign(x, { who: 'owner' }))); await v.close(owner.page, ctx);
      }
      results.push(...own);
     } catch (e) {   // (one scenario failing to be set up or made is a finding, not the end of the run)
      log(`   ✗ ${sc.id} stopped: ${String(e.message).slice(0, 200)}`);
      results.push({ scenario: sc.id, phase: 'transition', view: 'transition', label: 'the transition itself', who: 'viewer', ok: false, ms: null, issues: ['did not complete: ' + String(e.message).slice(0, 200)], claims: [] });
      for (const p of allPages) await p.page.evaluate(() => { try { OrderWin.close(); SheetWin.close(); OrderSearch.close(); SharedOrdersModal.close(); } catch (_) {} }).catch(() => {});
     }
    }
    // the guards: no Etsy call, no paid call, nothing outside the fake
    const etsy = srv.st.calls.filter(c => ['listOpenOrders', 'etsyOrderProxy', 'etsyImages', 'refreshEtsyToken', 'etsySandbox'].includes(c.name)), paid = srv.paid + srv.st.calls.filter(c => /charmNestAgent|charmEngrave|charmMaster/.test(c.name)).length, blocked = srv.agentAsked || 0;   // (a call to an AI function is turned back in the browser before the fake sees it: it counts as blocked, never as made)
    const errs = allPages.flatMap(p => p.errors.map(e => `${p.name}: ${e}`));
    report(results, { etsy: etsy.length, paid, blocked, outside: srv.outside.filter(u => !/fonts\.g|gstatic|qrcode/.test(u)).length, errs }, LIST, scenarios);
  } finally { await browser.close(); srv.close(); }
}
/** The cloud is quiet: the transition's own writes are all in (no write for 900 ms). Returns when the last write was made. */
async function settle(srv, quiet = 900, max = 25000) { const t0 = Date.now(); while (Date.now() - srv.lastWrite < quiet && Date.now() - t0 < max) await new Promise(r => setTimeout(r, 100)); return srv.lastWrite; }
/** Reads every view until it is right, or until `window` ms after `since` (the cloud's last write): the first time right is how long the surface took. */
async function converge(O, srv, views, viewerOf, ctx, window, phase, sc, dump, since, fresh) {
  const start = since || Date.now(), out = new Map(views.map(v => [v.id, { scenario: sc.id, phase, view: v.id, label: v.label, who: 'viewer', ok: false, ms: null, issues: [], claims: [] }]));
  const deadline = Math.max(Date.now() + 400, start + window + 300);
  for (;;) {
    const T = O.truthOf(srv.st, ctx.orders), S = O.sheetTruth(srv.st);
    await Promise.all(views.filter(v => !out.get(v.id).ok).map(async v => {
      const rec = out.get(v.id); let claims = [];
      try { claims = (await v.read(v.page === 'v' ? viewerOf[v.id].page : null, ctx, srv)).map(c => Object.assign({ rid: ctx.rid }, c)); } catch (e) { rec.issues = ['could not be read: ' + String(e.message).slice(0, 120)]; rec.claims = []; return; }
      const mineT = Object.values(T).filter(t => t.rid === ctx.rid), cancelledAll = mineT.length > 0 && mineT.every(t => t.state === 'cancelled');
      // (a cancelled order leaves the Orders list: that is the list agreeing, not a surface missing)
      const doneAll = mineT.length > 0 && mineT.every(t => t.state === 'done');   // (pieces completed by hand leave Review's open list for its Completed tab: no card there is the list agreeing)
      const gone = claims.filter(c => c.gone && !(c.s === 'list.rows' && cancelledAll) && !(c.s === 'rv' && doneAll)), issues = gone.length ? [`not on the screen (${gone.map(c => c.s + (c.text ? ': ' + JSON.stringify(c.text) : '')).join(', ')})`] : [];
      // (KNOWN: the server's own `where` (what the stations read) says "Waiting" of a piece made by hand: its stage ignores a Complete Order press by design (_orderTimeline.whereOf). Asked of C3 in api.md; counted and printed, never hidden, but it is not a surface of this page)
      const known = [];
      for (const c of claims) if (!c.gone) for (const b of O.judge(Object.assign({}, c, { all: T, sheets: S }), T, S, ctx)) (c.s === 'srv.where' && doneAll ? known : issues).push(`${c.s}${c.n != null ? ' piece ' + c.n : ''}: ${b}`);
      rec.claims = claims; rec.issues = issues; rec.known = known;
      if (!issues.length) { rec.ok = true; rec.ms = Math.max(0, Date.now() - start); }
    }));
    if ([...out.values()].every(r => r.ok) || Date.now() > deadline) break;
    await new Promise(r => setTimeout(r, 150));
  }
  const list = [...out.values()];
  for (const r of list) { r.late = r.ok && r.ms > window; if (dump) console.log(`   ${phase} ${r.view}${fresh ? ' (owner)' : ''}: ${r.ok ? 'agrees after ' + r.ms + ' ms' : 'DISAGREES'}\n      ${r.claims.map(c => `${c.s}${c.n != null ? '#' + c.n : ''}=${JSON.stringify(c.text != null ? c.text : c.dots != null ? 'dots ' + c.dots : c.kind === 'count' ? c : c.rids || c.ids || c.cancelled || c.count)}`).join('\n      ')}`); }
  return list;
}
function report(results, guards, listOnly, scenarios) {
  const bad = results.filter(r => !r.ok || r.late), seen = new Set();
  console.log('\n══ placement oracle ══');
  const byView = {};
  for (const r of results) { const k = r.view + (r.who === 'owner' ? ' (owner)' : ''); (byView[k] = byView[k] || { ok: 0, bad: 0, slow: 0, max: 0 }); r.ok ? byView[k].ok++ : byView[k].bad++; if (r.late) byView[k].slow++; if (r.ms != null) byView[k].max = Math.max(byView[k].max, r.ms); }
  for (const [k, v] of Object.entries(byView)) console.log(`  ${k.padEnd(16)} ${String(v.ok).padStart(3)} agree · ${String(v.bad).padStart(3)} disagree · ${v.slow} slower than 3 s · slowest ${v.max} ms`);
  for (const r of bad) { const key = `${r.scenario}/${r.phase}/${r.view}/${r.who}`; if (seen.has(key)) continue; seen.add(key); console.log(`\n  ✗ ${r.scenario} (${r.phase}) · ${r.label}${r.who === 'owner' ? ' · on the owner page' : ''}${r.late ? ` · agreed only after ${r.ms} ms` : ''}`); for (const i of r.issues.slice(0, 6)) console.log('      ' + i); }
  const knownN = results.filter(r => r.known && r.known.length).length; if (knownN) console.log(`\n  known, not counted above: ${knownN} reading(s) of the server's timeline \`where\` that say "Waiting" of a piece completed by hand (asked of C3: api.md)`);
  console.log(`\n  guards: ${guards.etsy} Etsy calls · ${guards.paid} paid calls (${guards.blocked || 0} AI asks turned back in the browser) · ${guards.outside} requests outside the fake · ${guards.errs.length} page errors`);
  for (const e of guards.errs.slice(0, 5)) console.log('      ' + e);
  const failed = bad.length > 0 || guards.etsy || guards.paid || guards.outside;
  console.log(failed ? `\n${bad.length} surface reading(s) disagree with the cloud (or came late)` : '\nevery surface agrees with the cloud in every state, within 3 s');
  if (!listOnly && failed) process.exitCode = 1;
}
module.exports = { main, VIEWS, SCENARIOS };
