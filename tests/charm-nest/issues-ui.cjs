/* The Issues list ON THE SCREEN against the independent oracle: the real LaserReview (the step rail and its '!' buttons), the real set card code and the real
 * '!' issues panel (charm-nest-library-issues.js) run in jsdom over random shops, with the real CharmNestReadiness.issues(). For each sheet:
 *   - the feed the panel reads (LaserReview.issuesOf) agrees with the oracle (every check of issues-property.cjs: no false alarm, none missed, no
 *     duplicate, the right sheet names, the right piece counts, the sheet's own blocker, the 'not checked yet' entry)
 *   - a '!' is on the rail exactly when something holds the sheet back (and on no step that is done)
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
const { paulShop } = require('./issues-paul.cjs');

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
    ListMedia: { peek: () => null, listing: () => Promise.resolve(null) } });
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
    if (plant === 'completed') return list.concat([{ step: 'engraving', key: 'approvalsNeeded', orderId: '4188888888', customer: 'Y', pieces: [], open: { type: 'order', id: '4188888888' } }]);   // an engraving ROW (must never be listed)
    return list;
  };
  return { w, d, calls, net, L: w.LaserReview, LI: w.LibraryIssues, body: d.getElementById('libBody') };
}

const COV = { ordersBehindEarlierStep: 0, unfolded: 0, matePanels: 0, matesListed: 0, matesExpected: 0, sheets: 0, held: 0, bangs: 0, panels: 0, orderPanels: 0, rowsListed: 0, rowsExpected: 0, ownPanels: 0, notes: 0, groups: 0 };
const SHOW = s => JSON.stringify(s).slice(0, 220);
const BAD_WORDS = [/\bNesting\b/, /\bBack files\b/, /\bQR label\b/, /Layout verified/, /\b\d+ of \d+\b/, /\blines?\b/i, /back engraving/i, /\bEngraving\b/];

/** What the oracle says about one sheet, in the terms the screen uses. A ready sheet of a set still waits for the set mates that are not laser-ready. */
function want(t, shop, sid) {
  const r = t.sheets[sid], raw = shop.sheets.find(s => s.id === sid), done = r.done, own = done ? [] : O.ownTruth(raw, t.lines), orders = r.completedBefore ? [] : Object.keys(r.orders).sort(), ghosts = r.completedBefore ? [] : r.ghosts;
  const set = shop.sets.find(x => x.sheetIds.includes(sid)), p = r.physical;
  const itself = r.included && (r.completedBefore || (p.layout && p.front && p.approval && p.backs && p.qr && !orders.length && !ghosts.length));
  const live = new Set(shop.sheets.filter(x => !x.archived).map(x => x.id));
  const mates = !done && !raw.draft && raw.solidIncluded !== false && set && itself ? set.sheetIds.filter(m => m !== sid && live.has(m) && !t.sheets[m].laserReady).sort() : [];
  return { done, own, orders, ghosts, mates, any: !done && (own.length > 0 || orders.length > 0 || ghosts.length > 0 || mates.length > 0), nesting: own.some(k => ['layout', 'notInSet', 'roseLine'].includes(k)) };
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
    const sid = r.id, wt = want(t, shop, sid), mine = bangs.get(sid) || [], steps = mine.map(b => b.getAttribute('data-issues-step'));
    COV.sheets++; if (wt.any) COV.held++; COV.bangs += mine.length;
    if (wt.orders.length && mine.length && !steps.includes('orders')) COV.ordersBehindEarlierStep++;   // (informational: real order issues the '!' shows once the earlier step is done: the rail is in order)
    if (!wt.any && mine.length) out.push({ type: 'bangWithoutIssue', sheet: sid, detail: `'!' on ${steps.join(',')}; the oracle sees nothing holding the sheet` });
    if (wt.any && !wt.nesting && !mine.length && w.document.querySelector(`[data-laser-card] [data-flow-for="sheet:${sid}"]`)) out.push({ type: 'noBang', sheet: sid, detail: `held by ${JSON.stringify({ own: wt.own, orders: wt.orders.length, ghosts: wt.ghosts.length })}` });
    if (!mine.length) continue;
    const pick = mine.find(b => b.getAttribute('data-issues-step') === 'orders') || mine[0], step = pick.getAttribute('data-issues-step');
    pick.click();
    const panel = await until(() => d.getElementById('libIssuesPanel'));
    if (!panel) { out.push({ type: 'panelDidNotOpen', sheet: sid, detail: `step ${step}` }); continue; }
    const rows = [...panel.querySelectorAll('.lisRow[data-issue-step="orders"]')].map(x => x.getAttribute('data-issue-order'));
    const folded = [...panel.querySelectorAll('.lisGroup[data-issue-orders]')].flatMap(x => (x.getAttribute('data-issue-orders') || '').split(',').filter(Boolean));
    COV.panels++;
    const listed = [...new Set(rows.concat(folded))].sort(), own = [...panel.querySelectorAll('.lisOwn[data-issue-step]:not(.lisNote)')].map(x => x.getAttribute('data-issue-key')), note = [...panel.querySelectorAll('.lisNote[data-issue-key="unverified"]')];
    const text = panel.textContent.replace(/\s+/g, ' ');
    if (step === 'orders') {
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
      const mates = [...panel.querySelectorAll('.lisRow[data-issue-step="laser"]')].map(x => x.getAttribute('data-issue-sheet')).sort();
      COV.matePanels++; COV.matesListed += mates.length; COV.matesExpected += wt.mates.length;
      if (JSON.stringify(mates) !== JSON.stringify(wt.mates)) out.push({ type: 'panelMates', sheet: sid, detail: `lists ${JSON.stringify(mates)}; the oracle: ${JSON.stringify(wt.mates)}` });
      if (rows.length) out.push({ type: 'panelLaserOrderRows', sheet: sid, detail: `${rows.length} order rows under Laser cutting` });
    } else if (step === 'backFiles' || step === 'qr') {
      COV.ownPanels++;
      const key = { backFiles: 'backFilesMissing', qr: 'qrMissing' }[step];
      if (!own.includes(key) && !rows.length) out.push({ type: 'panelOwnStep', sheet: sid, detail: `'!' on ${step}: the panel lists ${JSON.stringify(own)}` });
      if (own.length && !wt.own.includes(key)) out.push({ type: 'panelFalseOwn', sheet: sid, detail: `${step} panel says ${own.join(',')}; the oracle's own trouble: ${wt.own.join(',')}` });
    } else if (step === 'engraving') {
      COV.ownPanels++;
      if (/\b\d+\b/.test(text.replace(/Sheet \d+|SS|GF|RG/g, ''))) out.push({ type: 'panelEngravingCount', sheet: sid, detail: text.slice(0, 160) });
      if (!/Open engraving approvals/.test(text)) out.push({ type: 'panelEngravingLink', sheet: sid, detail: text.slice(0, 160) });
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
  for (let i = 0; i < shops && Date.now() - t0 < argv('budget-ms', 60000); i++) { const spec = S.makeSpec(seed0 * 9973 + i, { noLost: false, ghost: true }); pairs += spec.sheets.length; await run(S.materialize(spec), 'seed ' + spec.seed); }
  console.log(`${bad ? 'FAIL' : 'PASS'}: the Issues list on screen (real LaserReview, set cards and panel) vs the oracle: ${ran} shops (${pairs} random sheets + Paul's), ${bad} disagree ${JSON.stringify(counts)} in ${Date.now() - t0} ms\n  covered: ${JSON.stringify(COV)}`);
  process.exit(bad ? 1 : 0);
}
main().catch(e => { console.error(e); process.exit(1); });
