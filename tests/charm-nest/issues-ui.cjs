/* The Issues list ON THE SCREEN against the independent oracle: the real LaserReview (the step rail and its '!' buttons), the real set card code and the real
 * '!' issues panel (charm-nest-library-issues.js) run in jsdom over random shops, with the real CharmNestReadiness.issues(). For each sheet:
 *   - the feed the panel reads (LaserReview.issuesOf) agrees with the oracle (every check of issues-property.cjs: no false alarm, none missed, no
 *     duplicate, the right sheet names, the right piece counts, the sheet's own blocker, the 'not checked yet' entry)
 *   - a '!' is on the rail exactly when something REAL holds the sheet back (and on no step that is done); round 8 (Paul: "only related items to that
 *     particular sheet"): a sheet that is done and only waits for a mate sheet of its own set (a set advances as ONE) carries NOTHING on its rail (no '!',
 *     no clock, no mark of any kind), and no panel names or opens a sheet of its own set: no "Waiting for SS Sheet 1" row, no 'Waiting' header; the
 *     set's wait is said once, under the grey Approve button
 *   - the panel a '!' opens lists exactly the oracle's orders (rows and folded groups together), each with its real reason, only issues: no completed
 *     step, no engraving or back-count rows, no "N of M", no "lines"; the Engraving '!' shows the single link; nothing is written (no network call)
 *
 *   NODE_PATH=<dir with jsdom> node tests/charm-nest/issues-ui.cjs [--shops 40] [--seed 1] [--verbose]
 *   ISSUES_UI_ROOT=<dir>   take the page code (bridge, panel) from another checkout; ISSUES_IMPL=<file>  the readiness build the page and the oracle test
 * Skips (exit 0) when jsdom or the panel module is not there. Offline: ListMedia is a stub, the api stub answers nothing. */
'use strict';
const fs = require('fs'), path = require('path');
let JSDOM; try { ({ JSDOM } = require('jsdom')); } catch (_) { console.log('SKIP: jsdom is not installed (set NODE_PATH)'); process.exit(0); }
const ROOT = process.env.ISSUES_UI_ROOT || path.join(__dirname, '../..');
if (!fs.existsSync(path.join(ROOT, 'charm-nest-library-issues.js'))) { console.log('SKIP: the Issues panel (charm-nest-library-issues.js) is not in this build yet'); process.exit(0); }
const READINESS = process.env.ISSUES_IMPL || path.join(__dirname, '../../charm-nest-readiness.js');
const O = require('./issues-oracle.cjs');
const S = require('./issues-shop.cjs');
const P = require('./issues-property.cjs');
const { paulShop, paulRound8Shop } = require('./issues-paul.cjs');

const argv = (name, dflt) => { const i = process.argv.indexOf('--' + name); if (i < 0) return dflt; const v = process.argv[i + 1]; return v == null || v.startsWith('--') ? true : isNaN(+v) ? v : +v; };
const sleep = n => new Promise(r => setTimeout(r, n));
const until = async (f, ms = 3000) => { const t = Date.now(); for (;;) { const v = f(); if (v) return v; if (Date.now() - t > ms) return null; await sleep(10); } };

/** One page: the app's Library code around real LaserReview, set card and panel; the page's own helpers stubbed, never a network call. */
function boot() {
  const dom = new JSDOM('<body><div class="topbar"></div><main id="libBody"></main></body>', { url: 'https://example.test/', runScripts: 'outside-only', pretendToBeVisual: true });
  const w = dom.window, d = w.document, calls = [];
  w.matchMedia = () => ({ matches: true });
  w.Element.prototype.getClientRects = function () { return [{}]; };   // (jsdom has no layout: everything is visible)
  const net = []; w.fetch = (...a) => { net.push(a[0]); return Promise.reject(new Error('offline')); };
  Object.assign(w, { S: { mode: 'library', library: { rows: [], kind: 'sets', metal: 'all' }, cloud: { ok: false } }, api: async () => { net.push('api'); return { sheets: [], sets: [] }; }, allSheets: () => [], Orders: { rows: () => w.__rows || [] },
    Engrave: { items: () => new Map(), backsMarkup: () => '' }, esc: x => String(x ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c])), el: (tag, cls) => { const e = d.createElement(tag); e.className = cls; return e; },
    Gate: { projectLibraryRecords: x => x }, RoseStock: {}, CODE: { gold: 'GF', silver: 'SS', rose: 'RG' }, toast: () => null, CNEmployee: { name: () => 'Tester' }, cors: x => x, pvRatio: () => '',
    sheetHead: r => `<div class="h"><span class="nm">${r.metal} Sheet ${r.sheetIndex}</span></div>`,
    openLibrarySheet: id => { calls.push(['sheet', id]); return true; }, openOrderFrom: (btn, rid, o) => { calls.push(['order', rid, o && o.poolId || null]); return true; }, setMode: m => calls.push(['mode', m]),
    ListMedia: { peek: () => null, listing: () => Promise.resolve(null) }, LibraryFlow: { approve: async () => ({ ok: true, auto: [], needs: [], confirm: [], notes: [] }) } });   // (LibraryFlow: so every sheet carries its Approve button, where the set's wait is said)
  w.eval(fs.readFileSync(path.join(ROOT, 'charm-nest-orders.js'), 'utf8')); w.O = w.CharmNestOrders;
  w.eval(fs.readFileSync(READINESS, 'utf8'));
  w.eval(fs.readFileSync(path.join(ROOT, 'charm-nest-activity.js'), 'utf8'));
  w.eval(fs.readFileSync(path.join(ROOT, 'charm-nest-motion.js'), 'utf8'));
  const bridge = fs.readFileSync(path.join(ROOT, 'charm-nest-bridge.js'), 'utf8');
  w.eval(bridge.slice(bridge.indexOf('const LaserReview ='), bridge.indexOf('/* ═══ 22 · Sets — one run')));
  const a = bridge.indexOf('  function libraryGroups('), b = bridge.indexOf('  /** A set card whose completion', a), c = bridge.indexOf('  async function renderLibrary(body, opts)', b), e = bridge.indexOf('  return { releaseIssue', c);
  w.eval('window.Sets=(()=>{' + bridge.slice(a, b) + bridge.slice(c, e) + ';return {libraryCard,libraryGroups};})();');
  w.eval(fs.readFileSync(path.join(ROOT, 'charm-nest-library-issues.js'), 'utf8'));
  const plant = argv('plant', ''), R = w.CharmNestReadiness, real = R.issues;
  if (plant) R.issues = function (sheet, ctx) {
    const list = real.call(this, sheet, ctx) || [], first = list.findIndex(i => i && i.step === 'orders' && i.orderId);
    if (plant === 'drop') return first < 0 ? list : list.filter((x, i) => i !== first);                                    // one real issue left out
    if (plant === 'extra') return list.concat([{ step: 'orders', key: 'pooled', orderId: '4199999999', orderLabel: 'Order 4199999999', customer: 'X', pieceCount: 2, pieces: [{ index: 2, poolId: '4199999999_1_2', label: 'x', sheetLabel: null, kind: 'pooled' }], open: { type: 'order', id: '4199999999' } }]);   // an invented issue
    if (plant === 'wait') { const mate = ((ctx && ctx.sheets) || []).find(m => (m.id || m.sheetId) !== (sheet.id || sheet.sheetId)); return mate ? list.concat([{ step: 'laser', key: 'waitsOnSheet', quiet: true, label: 'Mate', stepLabel: 'Engraving', open: { type: 'sheet', id: mate.id || mate.sheetId } }]) : list; }   // the set's wait put back in the list
    if (plant === 'completed') return list.concat([{ step: 'engraving', key: 'approvalsNeeded', orderId: '4188888888', customer: 'Y', pieces: [], open: { type: 'order', id: '4188888888' } }]);   // an engraving ROW (must never be listed)
    return list;
  };
  return { w, d, calls, net, L: w.LaserReview, LI: w.LibraryIssues, body: d.getElementById('libBody') };
}

const COV = { ordersBehindEarlierStep: 0, unfolded: 0, matePanels: 0, matesListed: 0, matesExpected: 0, sheets: 0, held: 0, bangs: 0, panels: 0, orderPanels: 0, rowsListed: 0, rowsExpected: 0, ownPanels: 0, notes: 0, groups: 0, setHeld: 0, waitOnly: 0, approveLines: 0 };
const SHOW = s => JSON.stringify(s).slice(0, 220);
// (round 13: the QR label is part of Order check, said by its one plain row "QR label not made yet": checked apart below, so it is no bad word here)
const BAD_WORDS = [/\bNesting\b/, /\bBack files\b/, /Layout verified/, /\b\d+ of \d+\b/, /\blines?\b/i, /back engraving/i, /\bEngraving\b/];

/** What the oracle says about one sheet, in the terms the screen uses. A set mate that is not ready to be approved is the SET's wait (waits), never an issue of this sheet and
 *  (round 8) no part of its list or rail at all: `any` (a real '!') is only the sheet's own trouble and its orders; `waitOnly` is a sheet that is done and has only the set's wait. */
function want(t, shop, sid) {
  const r = t.sheets[sid], raw = shop.sheets.find(s => s.id === sid), done = r.done, own = done ? [] : O.ownTruth(raw, t.lines), orders = r.completedBefore ? [] : Object.keys(r.orders).sort(), ghosts = r.completedBefore ? [] : r.ghosts;
  const set = shop.sets.find(x => x.sheetIds.includes(sid)), p = r.physical;
  const itself = r.included && (r.completedBefore || (p.layout && p.front && p.approval && p.backs && p.qr && !orders.length && !ghosts.length));
  const waits = !done && !raw.draft && raw.solidIncluded !== false && set ? O.setWaits(shop, t, sid) : [];
  const any = !done && (own.length > 0 || orders.length > 0 || ghosts.length > 0);
  return { done, own, orders, ghosts, waits, waitOnly: !done && !any && itself && waits.length > 0, any, nesting: own.some(k => ['layout', 'notInSet', 'roseLine'].includes(k)) };
}

async function checkShop(shop, tag) {
  const T0 = Date.now(), lap = m => { if (argv('debug', false)) console.error(`  [${tag}] ${m} +${Date.now() - T0}ms`); };
  const out = [], pg = boot(), { w, d, L, LI, body, net } = pg, t = O.truth(shop), recs = JSON.parse(JSON.stringify(P.serverLike(shop)));
  lap('booted');
  w.__rows = JSON.parse(JSON.stringify(S.uiRows(shop)));
  const sets = shop.sets.map(x => ({ ...x, status: 'saved', name: 'Set ' + x.seq, day: '2026-10-05', seq: x.seq }));
  L.sections(body);
  for (const r of recs) L.record(r);
  const groups = w.Sets.libraryGroups(sets, recs);
  for (const g of groups) { const card = w.Sets.libraryCard(g, g.sheets, g.sheets); L.place(card, L.group(g, g.sheets).ready, body); }
  lap('cards placed'); L.changed(); await sleep(40); lap('changed');

  // 1 · the feed the panel reads, against every check of the property harness
  const feed = {};
  for (const r of recs) { try { const f = L.issuesOf(r.id); feed[r.id] = f ? f.issues || [] : []; } catch (e) { out.push({ type: 'throws', sheet: r.id, detail: e.message }); feed[r.id] = []; } }
  for (const x of P.listChecks(w.CharmNestReadiness, shop, 'ui-feed', JSON.parse(JSON.stringify(feed)))) out.push(x);

  lap('feed checked');
  // 2 · the '!' of every sheet, and the panel it opens
  const bangs = new Map();
  for (const b of body.querySelectorAll('button[data-issues-open]')) { const id = b.getAttribute('data-issues-id'); (bangs.get(id) || bangs.set(id, []).get(id)).push(b); }
  for (const r of recs) {
    const sid = r.id, wt = want(t, shop, sid), all = bangs.get(sid) || [], quietBtns = all.filter(b => b.hasAttribute('data-issues-quiet')), mine = all.filter(b => !b.hasAttribute('data-issues-quiet')), steps = mine.map(b => b.getAttribute('data-issues-step'));
    COV.sheets++; if (wt.any) COV.held++; COV.bangs += mine.length;
    const drawn = !!w.document.querySelector(`[data-laser-card] [data-flow-for="sheet:${sid}"]`);
    // round 8: the set's wait is no mark on the rail: no clock, no quiet mark, nothing at all on a sheet that is done and only waits for a mate of its own set
    if (wt.waits.length) COV.setHeld++;
    if (quietBtns.length) out.push({ type: 'quietMarkDrawn', sheet: sid, detail: `${quietBtns.length} quiet mark(s): ${SHOW(quietBtns[0].outerHTML)}` });
    if (wt.waitOnly && drawn) { COV.waitOnly++; if (all.length) out.push({ type: 'markOnWaitOnlySheet', sheet: sid, detail: `done, only the set waits for ${wt.waits.join(',')}: ${all.length} mark(s) on its rail` }); }
    if (w.document.querySelector(`[data-flow-for="sheet:${sid}"] .flowWait`)) out.push({ type: 'clockDrawn', sheet: sid, detail: 'a clock on the rail' });
    // the set's wait is said once, under the grey Approve button (when the page draws one for this sheet): it names a sheet that holds the set
    const ab = w.document.querySelector(`.approveBox[data-approve-for="sheet:${sid}"]`);
    if (ab && wt.waits.length) {
      const say = (ab.querySelector('[data-approve-why]') || {}).textContent || '', names = [sid].concat(wt.waits).map(z => O.labelOf(shop.sheets.find(q => q.id === z))).concat('A sheet of this set');   // (the last: a sheet of the set that cannot be found, named first by the gate)
      if (ab.querySelector('[data-approve-btn]').getAttribute('aria-disabled') !== 'true') out.push({ type: 'approveNotGrey', sheet: sid, detail: `a mate holds the set (${wt.waits.join(',')}) but the button is not grey` });
      else if (!names.some(n => say.startsWith(n + ' · '))) out.push({ type: 'approveLine', sheet: sid, detail: `the line says ${SHOW(say)}; the sheets that hold the set: ${names.join(', ')}` });
      else COV.approveLines++;
    }
    if (wt.orders.length && mine.length && !steps.includes('orders')) COV.ordersBehindEarlierStep++;   // (informational: real order issues the '!' shows once the earlier step is done: the rail is in order)
    if (!wt.any && mine.length) out.push({ type: 'bangWithoutIssue', sheet: sid, detail: `'!' on ${steps.join(',')}; the oracle sees nothing holding the sheet` });
    if (wt.any && !wt.nesting && !mine.length && drawn) out.push({ type: 'noBang', sheet: sid, detail: `held by ${JSON.stringify({ own: wt.own, orders: wt.orders.length, ghosts: wt.ghosts.length })}` });
    if (!mine.length) continue;
    const pick = mine.find(b => b.getAttribute('data-issues-step') === 'orders') || mine[0], step = pick.getAttribute('data-issues-step');
    pick.click();
    const panel = await until(() => d.getElementById('libIssuesPanel'));
    if (!panel) { out.push({ type: 'panelDidNotOpen', sheet: sid, detail: `step ${step}` }); continue; }
    const rows = [...panel.querySelectorAll('.lisRow[data-issue-step="orders"]')].map(x => x.getAttribute('data-issue-order'));
    const folded = [...panel.querySelectorAll('.lisGroup[data-issue-orders]')].flatMap(x => (x.getAttribute('data-issue-orders') || '').split(',').filter(Boolean));
    COV.panels++;
    const listed = [...new Set(rows.concat(folded))].sort(), own = [...panel.querySelectorAll('.lisOwn[data-issue-step]:not(.lisNote)')].map(x => x.getAttribute('data-issue-key')), note = [...panel.querySelectorAll('.lisNote[data-issue-key="unverified"]')];
    // round 8: no wait row of any kind, no 'Waiting' header, and no word of a sheet of this sheet's own set (the set's wait is said under the Approve button, not here)
    if (panel.querySelector('.lisWait,[data-quiet],[data-issue-key="waitsOnSheet"],.lisCount.quiet')) out.push({ type: 'panelWaitRow', sheet: sid, detail: 'a wait row or a quiet header is in the list' });
    {
      const me = O.effSet(shop.sheets.find(q => q.id === sid)), said = panel.textContent.replace(/\s+/g, ' ');
      if (me) for (const z of shop.sheets) if (z.id !== sid && !z.archived && O.effSet(z) === me && said.includes(O.labelOf(z))) out.push({ type: 'panelNamesMate', sheet: sid, detail: `the list names ${O.labelOf(z)}, a sheet of its own set: ${SHOW(said.slice(0, 160))}` });
      if (/\bWaiting\b/.test(said)) out.push({ type: 'panelWaitingWord', sheet: sid, detail: SHOW(said.slice(0, 160)) });
    }
    if (panel.querySelector('.lisRow[data-issue-key="otherSheetNotReady"]') && wt.orders.length === 0) out.push({ type: 'panelOldWait', sheet: sid, detail: 'an order row says another sheet is not ready, with no real order issue' });
    const text = panel.textContent.replace(/\s+/g, ' ');
    // ("Waits on RG Sheet 1, in no set" is the round-8 wording for a real piece on a not-ready sheet that is in NO set: a real wait, said as that; only the plain "Waits on" for the same set is the old one)
    if (/\bWaits on\b/.test(panel.textContent.replace(/Waits on [A-Z0-9]{2,3} Sheet \d+, in no set/g, ''))) out.push({ type: 'panelWaitsOn', sheet: sid, detail: 'the old words "Waits on" for a sheet of the same set' });
    if (step === 'orders') {
      // round 13: the one own row Order check carries is the sheet's QR label, in its plain words; the words appear nowhere else (the rail has five steps). A '!' on Order check that has
      // nothing of its own to list (the QR label made, or an earlier step still behind: a hard label gap marks Order check too) falls back to the sheet's current blocker, as every panel does
      if (own.length && own[0] !== 'qrMissing' && !(wt.own.includes(own[0]) && !wt.orders.length)) out.push({ type: 'panelOrdersOwn', sheet: sid, detail: `an own row under Order check that is neither the QR label nor the sheet's current blocker: ${own.join(',')}` });
      if (own.includes('qrMissing') !== /QR label not made yet/.test(text)) out.push({ type: 'panelQrWords', sheet: sid, detail: `own rows ${own.join(',')}; text "${text.slice(0, 120)}"` });
      if (!own.includes('qrMissing') && /QR labels?/.test(text)) out.push({ type: 'panelQrWords', sheet: sid, detail: `QR words with no QR row: "${text.slice(0, 120)}"` });
      COV.orderPanels++; COV.rowsListed += listed.length; COV.rowsExpected += wt.orders.length; COV.notes += note.length;
      if (folded.length) {
        COV.groups++;
        for (const g of panel.querySelectorAll('.lisGroup')) {
          const n = (g.getAttribute('data-issue-orders') || '').split(',').filter(Boolean).length, m = /, (\d+) orders?$/.exec(g.getAttribute('aria-label') || '');
          if (!m || +m[1] !== n) out.push({ type: 'panelGroupCount', sheet: sid, detail: `${g.getAttribute('aria-label')} but names ${n} orders` });
        }
        for (let k = 0; k < 12; k++) {   // unfold every group and press every "Show N more"
          const closed = panel.querySelector('.lisGroup[aria-expanded="false"]') || panel.querySelector('.lisMore[aria-expanded="false"]');
          if (!closed) break;
          closed.click(); await sleep(15);
        }
        const every = [...new Set([...panel.querySelectorAll('.lisRow[data-issue-step="orders"]')].map(x => x.getAttribute('data-issue-order')))].sort();
        COV.unfolded += every.length;
        if (JSON.stringify(every) !== JSON.stringify(wt.orders)) out.push({ type: 'panelUnfolded', sheet: sid, detail: `everything unfolded lists ${every.length}, the oracle ${wt.orders.length}` });
      }
      for (const id of listed) if (!wt.orders.includes(id)) out.push({ type: 'panelFalseRow', sheet: sid, order: id, detail: 'listed in the Order check panel, not an issue for this sheet' });
      for (const id of wt.orders) if (!listed.includes(id)) out.push({ type: 'panelMissingRow', sheet: sid, order: id, detail: 'a real issue the Order check panel does not list' });
      if (new Set(rows).size !== rows.length) out.push({ type: 'panelDuplicateRow', sheet: sid, detail: rows.join(',') });
      if (wt.ghosts.length && !note.length) out.push({ type: 'panelNoNote', sheet: sid, detail: wt.ghosts.join(',') });
      if (!wt.ghosts.length && note.length) out.push({ type: 'panelFalseNote', sheet: sid, detail: SHOW(note[0].textContent) });
      for (const bad of BAD_WORDS) if (bad.test(text)) out.push({ type: 'panelCompletedWords', sheet: sid, detail: `${bad} in "${text.slice(0, 160)}"` });
      for (const row of panel.querySelectorAll('.lisRow[data-issue-step="orders"]')) {
        const id = row.getAttribute('data-issue-order'), kinds = new Set(((wt.orders.includes(id) && t.sheets[sid].orders[id].offenders) || []).flatMap(o => [...o.reasons]));
        if (kinds.size && !kinds.has(row.getAttribute('data-issue-key'))) out.push({ type: 'panelWrongReason', sheet: sid, order: id, detail: `${row.getAttribute('data-issue-key')} vs real ${[...kinds].join('/')}` });
      }
    } else if (step === 'laser') {
      // (the sheet rows are the REAL set trouble: a sheet of the set that cannot be found, the set not loaded; none in these shops. The set's wait is no row of this list.)
      const mates = [...panel.querySelectorAll('.lisRow[data-issue-step="laser"]')].map(x => x.getAttribute('data-issue-sheet')).sort();
      COV.matePanels++; COV.matesListed += mates.length;
      if (mates.length) out.push({ type: 'panelMates', sheet: sid, detail: `lists real set trouble ${JSON.stringify(mates)}; the oracle sees none` });
      if (rows.length) out.push({ type: 'panelLaserOrderRows', sheet: sid, detail: `${rows.length} order rows under Laser cutting` });
    } else if (step === 'engraving') {
      COV.ownPanels++;
      if (/\b\d+\b/.test(text.replace(/Sheet \d+|SS|GF|RG/g, ''))) out.push({ type: 'panelEngravingCount', sheet: sid, detail: text.slice(0, 160) });
      // round 13: Engraving carries two plain rows: the link to the approvals while a back waits for one, and "Saving back files" (no link to approvals) once they are all approved
      if (own[0] === 'backFilesMissing') { if (!/Saving back files/.test(text) || /Open engraving approvals/.test(text)) out.push({ type: 'panelSavingWords', sheet: sid, detail: text.slice(0, 160) }); }
      else if (!/Open engraving approvals/.test(text)) out.push({ type: 'panelEngravingLink', sheet: sid, detail: text.slice(0, 160) });
      if (own.length && !wt.own.includes(own[0])) out.push({ type: 'panelFalseOwn', sheet: sid, detail: `engraving panel says ${own.join(',')}; the oracle's own trouble: ${wt.own.join(',')}` });
      if (rows.length) out.push({ type: 'panelEngravingRows', sheet: sid, detail: `${rows.length} order rows under the Engraving step` });
    }
    if (own.length && !wt.own.length) out.push({ type: 'panelFalseOwn', sheet: sid, detail: `${own.join(',')}; the oracle's own trouble: none` });
    if (panel.querySelector('svg path[d*="M2.6 6.3"]')) out.push({ type: 'panelTick', sheet: sid, detail: 'a completed tick' });
    LI.close(); await sleep(5); d.querySelectorAll('#libIssuesPanel').forEach(n => n.remove());
  }
  if (net.length) out.push({ type: 'networkCall', sheet: '-', detail: SHOW(net.slice(0, 3)) });
  w.close();
  return out;
}

/** Round 8, Paul's order 4170837249 on the screen: GF Sheet 1's '!' Order check panel for RG Sheet 1 held / not ready in no set, in the same set and in another set, with the chain
 *  completed by hand by each button (print only, Complete Order only, both) or not at all (and reopened). The panel lists the order ONCE for the real piece on the real sheet, says "in no set"
 *  / the split in its own words, and never names or blames the chain; nothing is listed when RG Sheet 1 is in GF's own set (the set's wait, said where Approve is). */
async function panelOf(shop, sid) {
  const pg = boot(), { w, d, L, LI, body, net } = pg, recs = JSON.parse(JSON.stringify(P.serverLike(shop)));
  w.__rows = JSON.parse(JSON.stringify(S.uiRows(shop)));
  const sets = shop.sets.map(x => ({ ...x, status: 'saved', name: 'Set ' + x.seq, day: '2026-10-05', seq: x.seq }));
  L.sections(body); for (const r of recs) L.record(r);
  for (const g of w.Sets.libraryGroups(sets, recs)) { const card = w.Sets.libraryCard(g, g.sheets, g.sheets); L.place(card, L.group(g, g.sheets).ready, body); }
  L.changed(); await sleep(40);
  const bangs = [...body.querySelectorAll('button[data-issues-open]')].filter(b => b.getAttribute('data-issues-id') === sid && !b.hasAttribute('data-issues-quiet')), res = { steps: bangs.map(b => b.getAttribute('data-issues-step')), rows: [], text: '', net };
  const open = bangs.find(b => b.getAttribute('data-issues-step') === 'orders');
  if (open) {
    open.click(); const panel = await until(() => d.getElementById('libIssuesPanel'));
    if (panel) { res.rows = [...panel.querySelectorAll('.lisRow[data-issue-step="orders"]')].map(x => ({ order: x.getAttribute('data-issue-order'), key: x.getAttribute('data-issue-key'), chip: ((x.querySelector('.lisChip') || {}).textContent || '').trim(), dots: x.querySelectorAll('.lisDot').length })); res.text = panel.textContent.replace(/\s+/g, ' '); res.count = ((panel.querySelector('.lisCount') || {}).textContent || '').trim(); }
    LI.close(); await sleep(5);
  }
  res.net = net.length; w.close();
  return res;
}
async function roundEight() {
  const out = [], seen = { panels: 0, noSet: 0, split: 0, same: 0 };
  const PRESS = [['print only', ['print']], ['Complete Order only', ['button']], ['both buttons', ['print', 'button']]];
  for (const own of ['held', 'noQr', 'unverified']) for (const [pname, presses] of PRESS) {
    for (const [name, setId, chip] of [['in no set', null, 'Waits on RG Sheet 1, in no set'], ['in another set', 'set-2', 'Split from RG Sheet 1'], ['in the same set', 'set-1', null]]) {
      const tag = `round 8: RG Sheet 1 ${own}, ${name}, chain ${pname}`, r = await panelOf(paulRound8Shop({ own, setId, presses }), 'gf-sheet-1');
      const say = (type, detail) => out.push({ type, sheet: 'gf-sheet-1', detail: `${tag}: ${detail}` });
      if (r.net) say('networkCall', `${r.net} calls`);
      if (chip === null) { if (r.steps.length) say('round8SameSetBang', `a '!' on ${r.steps.join(',')} for a wait inside GF's own set`); seen.same++; continue; }
      seen.panels++; seen[setId === null ? 'noSet' : 'split']++;
      if (!r.steps.includes('orders')) { say('round8NoBang', `no '!' on Order check (steps ${r.steps.join(',')})`); continue; }
      if (r.rows.length !== 1 || r.rows[0].order !== '4170837249' || r.rows[0].key !== 'otherSheetNotReady') say('round8Rows', `the panel lists ${JSON.stringify(r.rows)}`);
      else if (r.rows[0].chip !== chip) say('round8Chip', `chip "${r.rows[0].chip}", wanted "${chip}"`);
      if (/CABLE|chain|Completed/i.test(r.text)) say('round8NamesChain', SHOW(r.text));
      if (setId === null && !/in no set/.test(r.text)) say('round8NoSetWords', SHOW(r.text));
      if (/\blines?\b/i.test(r.text)) say('panelCompletedWords', 'the word "lines": ' + SHOW(r.text));
      if (/^(1|2)\b/.test(r.count) && !/^1\b/.test(r.count)) say('round8Count', `header ${SHOW(r.count)}: one issue, the real piece`);
    }
  }
  // never pressed / reopened: the chain holds GF Sheet 1 as a piece with no SKU, and the RG piece is told beside it as what it is
  for (const [pname, spec] of [['never pressed', { presses: null }], ['pressed, then reopened', { presses: ['print', 'button'], reopened: true }]]) {
    const r = await panelOf(paulRound8Shop({ own: 'held', setId: null, ...spec }), 'gf-sheet-1');
    if (r.rows.length !== 1 || r.rows[0].key !== 'noSku') out.push({ type: 'round8Unpressed', sheet: 'gf-sheet-1', detail: `${pname}: the chain holds it (No SKU): ${JSON.stringify(r.rows)}` });
    else if (r.rows[0].chip !== 'No SKU') out.push({ type: 'round8Unpressed', sheet: 'gf-sheet-1', detail: `${pname}: chip ${r.rows[0].chip}` });
  }
  return { out, seen };
}

/** Round 8 (Paul's newest screenshot: RG Sheet 1, GF Sheet 1 and SS Sheet 1 in ONE card): a Rose Gold sheet is in a set when its own record says so (setId, not a draft), which the card and the
 *  order check both read. GF Sheet 1's '!' on Order check names RG Sheet 1 "in no set" while RG Sheet 1 is a draft or not in the set; the Library's next answer (RG Sheet 1 joined the set by the Cut
 *  Sheet press, ready or not) takes the '!' away at once, with the card read from the same record. Replays the answers the live read hands the page (LaserReview.record + changed). */
async function joinOnScreen() {
  const out = [], seen = { before: 0, after: 0, together: 0 }, bangsOf = (body, sid) => [...body.querySelectorAll('button[data-issues-open]')].filter(b => b.getAttribute('data-issues-id') === sid && !b.hasAttribute('data-issues-quiet')).map(b => b.getAttribute('data-issues-step'));
  const cards = shop => shop.sets.map(x => ({ ...x, status: 'saved', name: 'Set ' + x.seq, day: '2026-10-05', seq: x.seq }));
  for (const [name, a, b] of [['a draft that joins ready', { own: 'draft', setId: null }, { own: 'ok', setId: 'set-1' }], ['a sheet with no QR labels that joins', { own: 'noQr', setId: null }, { own: 'noQr', setId: 'set-1' }], ['an unverified sheet that joins', { own: 'unverified', setId: null }, { own: 'unverified', setId: 'set-1' }]]) {
    const pg = boot(), { w, d, L, LI, body } = pg, shopA = paulRound8Shop({ ...a, presses: ['button'] }), shopB = paulRound8Shop({ ...b, presses: ['button'] });
    const recsA = JSON.parse(JSON.stringify(P.serverLike(shopA))), recsB = JSON.parse(JSON.stringify(P.serverLike(shopB)));
    w.__rows = JSON.parse(JSON.stringify(S.uiRows(shopA)));
    L.sections(body); for (const r of recsA) L.record(r);
    for (const g of w.Sets.libraryGroups(cards(shopA), recsA)) L.place(w.Sets.libraryCard(g, g.sheets, g.sheets), L.group(g, g.sheets).ready, body);
    L.changed(); await sleep(40);
    const say = (type, detail) => out.push({ type, sheet: 'gf-sheet-1', detail: `${name}: ${detail}` });
    if (!bangsOf(body, 'gf-sheet-1').includes('orders')) say('joinBeforeNoBang', `no '!' on Order check before the join (${bangsOf(body, 'gf-sheet-1').join(',')})`); else seen.before++;
    const open = [...body.querySelectorAll('button[data-issues-open]')].find(x => x.getAttribute('data-issues-id') === 'gf-sheet-1' && x.getAttribute('data-issues-step') === 'orders');
    if (open) { open.click(); const panel = await until(() => d.getElementById('libIssuesPanel')); const text = panel ? panel.textContent.replace(/\s+/g, ' ') : ''; if (!/Waits on RG Sheet 1, in no set/.test(text)) say('joinBeforeWords', SHOW(text)); LI.close(); await sleep(5); }
    // the answer after the Cut Sheet press: RG Sheet 1's own record now carries the set, and GF Sheet 1's answer no longer waits for it
    for (const r of recsB) L.record(r);
    L.changed(); await sleep(60);
    const after = bangsOf(body, 'gf-sheet-1');
    if (after.includes('orders')) say('joinAfterBang', `'!' still on Order check after RG Sheet 1 joined the set (${after.join(',')})`); else seen.after++;
    const rg = recsB.find(r => r.id === 'rg-sheet-1'), gf = recsB.find(r => r.id === 'gf-sheet-1');
    if (w.CharmNestOrders.libraryGroup(rg).key !== w.CharmNestOrders.libraryGroup(gf).key || w.CharmNestReadiness.setOf(rg) !== w.CharmNestReadiness.setOf(gf)) say('joinCardApart', 'the card or the order check puts RG Sheet 1 and GF Sheet 1 in different sets'); else seen.together++;
    w.close();
  }
  return { out, seen };
}

/** One gold sheet with 18 two-piece orders whose other piece is pooled, has no SKU, is held, or sits on a silver sheet that is not ready. */
function bigShop() {
  const sheets = [{ id: 'big-sheet-1', metal: 'gold', index: 1, own: 'ok', setId: 'set-big' }, { id: 'big-sheet-2', metal: 'silver', index: 1, own: 'noQr', setId: 'set-big' }];
  const orders = []; let tx = 9000, oid = 4175000000;
  const base = { problems: [], sku: 'SKU', hold: null, change: false, noDesign: false, kind: 'big', engrave: 'plain', state: 'written' };
  const mine = () => ({ ...base, tx: ++tx, metal: 'gold', q: 1, copies: [{ sheet: 'big-sheet-1', pooled: true }] });
  for (let i = 0; i < 18; i++) {
    const other = [
      { ...base, tx: ++tx, metal: 'gold', q: 1, copies: [{ pooled: true }], state: 'pooled' },
      { ...base, tx: ++tx, metal: 'gold', q: 1, copies: [{}], state: 'unmatched', problems: ['unmatchedSku'], sku: '' },
      { ...base, tx: ++tx, metal: 'silver', q: 1, copies: [{ sheet: 'big-sheet-1', pooled: true }], hold: { at: 1, by: 'Paul', note: 'check' } },
      { ...base, tx: ++tx, metal: 'silver', q: 1, copies: [{ sheet: 'big-sheet-2', pooled: true }] }
    ][i % 4];
    orders.push({ id: String(++oid), buyer: 'Big buyer ' + i, lines: [mine(), other] });
  }
  return S.materialize({ seed: 11, sandbox: false, sheets, orders, archivedSheets: [] });
}

async function main() {
  const shops = argv('shops', 40), seed0 = argv('seed', 1), t0 = Date.now(), counts = {}; let ran = 0, bad = 0, pairs = 0;
  const run = async (shop, tag) => {
    const d = await checkShop(shop, tag); ran++;
    if (!d.length) return;
    bad++; for (const x of d) counts[x.type] = (counts[x.type] || 0) + 1;
    if (bad <= 4 || argv('verbose', false)) console.log(`\nUI DISAGREEMENT (${tag}):\n  ${d.slice(0, 5).map(x => `${x.type} sheet ${x.sheet} ${x.order || ''} ${x.detail || ''}`).join('\n  ')}`);
  };
  await run(paulShop(), 'Paul\'s shop');
  await run(bigShop(), 'a long list (18 orders on one sheet)');
  let r8 = null;
  { r8 = await roundEight(); ran++; if (r8.out.length) { bad++; for (const x of r8.out) counts[x.type] = (counts[x.type] || 0) + 1; console.log(`\nUI DISAGREEMENT (Paul's order 4170837249, round 8):\n  ${r8.out.slice(0, 5).map(x => `${x.type} ${x.detail}`).join('\n  ')}`); } }
  { const j = await joinOnScreen(); ran++; r8.seen.join = j.seen; if (j.out.length) { bad++; for (const x of j.out) counts[x.type] = (counts[x.type] || 0) + 1; console.log(`\nUI DISAGREEMENT (RG Sheet 1 joining the set):\n  ${j.out.slice(0, 5).map(x => `${x.type} ${x.detail}`).join('\n  ')}`); } }
  for (let i = 0; i < shops && Date.now() - t0 < argv('budget-ms', 60000); i++) { const spec = S.makeSpec(seed0 * 9973 + i, { noLost: false, ghost: true }); pairs += spec.sheets.length; await run(S.materialize(spec), 'seed ' + spec.seed); }
  // the set's wait must have been met (a harness that never meets a sheet whose set waits for a mate proves nothing): sheets that only wait carry no mark, and their Approve line says what holds the set
  if (!COV.setHeld || !COV.waitOnly || !COV.approveLines) { bad++; counts.coverage = 1; console.log(`\nCOVERAGE: the set's wait was never met ${JSON.stringify({ setHeld: COV.setHeld, waitOnly: COV.waitOnly, approveLines: COV.approveLines })}`); }
  console.log(`${bad ? 'FAIL' : 'PASS'}: the Issues list on screen (real LaserReview, set cards and panel) vs the oracle: ${ran} shops (${pairs} random sheets + Paul's), ${bad} disagree ${JSON.stringify(counts)} in ${Date.now() - t0} ms\n  covered: ${JSON.stringify(COV)}\n  round 8 (Paul's order 4170837249 with the chain completed by hand): ${JSON.stringify(r8 && r8.seen)}`);
  process.exit(bad ? 1 : 0);
}
main().catch(e => { console.error(e); process.exit(1); });
