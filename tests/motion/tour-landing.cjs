// The Send to Sheet tour's landing (VISUAL, Paul 26 Oct 2026: "I don't see you actually visually testing the outcome of animations and transitions"):
// a queued piece (Manual run: no place on the sheet yet) must come down ON its thumbnail in the sheet's queue. It came down about 330 px away, on the
// sheet's spot, and its thumbnail then popped in under the sheet (charm-nest-tour.js flyOnto / the leg's flights; fixed 26 Oct 2026).
// Taken from the frame-timing runner tour-adversarial.cjs (its fixture, its in-page probe and its run(), unchanged; no screencast, no frame budgets):
// each start plays the real tour on its own fresh fake site (bridge-server.cjs) and every animation frame records the flying piece and the spot the
// sheet has for it. Pass: the piece's last solid frame is within 12 px of its thumbnail (or of its place on the sheet), and every piece sent is seen landing.
//   node tests/motion/tour-landing.cjs      (PW_DIR=<playwright-core's node_modules>, CHROMIUM=<chrome>; ONLY=review-custom,review-open)
'use strict';
const path = require('path'), fs = require('fs'), os = require('os');
const here = path.join(__dirname, '../..');
const pwDir = process.env.PW_DIR || path.join(here, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const OUT = process.env.SHOTS || require("fs").mkdtempSync(require("path").join(require("os").tmpdir(), "tour-landing-"));
const ONLY = (process.env.ONLY || '').split(',').map(s => s.trim()).filter(Boolean);
const STRIP = false;
const LAYOUT = /^(top|left|right|bottom|width|height|maxHeight|minHeight|maxWidth|minWidth|margin.*|padding.*|border.*Width|inset|flex.*|gap)$/;

/* ── the fixture: custom orders, an Unknown SKU and an Options question (each with a neighbour, so its chip stays) ── */
const DG = (...kv) => { let t = ''; for (let i = 0; i < kv.length; i += 2) t += `${kv[i]}\n${kv[i + 1]}\n`; return t; };
const DXF = (w, h = 20) => DG(0, 'SECTION', 2, 'HEADER', 9, '$INSUNITS', 70, 4, 0, 'ENDSEC', 0, 'SECTION', 2, 'ENTITIES',
  0, 'LWPOLYLINE', 8, 'CUT', 90, 4, 70, 1, 10, 0, 20, 0, 10, w, 20, 0, 42, 0.4, 10, w, 20, h, 10, 0, 20, h,
  0, 'CIRCLE', 8, 'CUT', 10, w / 2, 20, h - 4, 40, 1.2, 0, 'ENDSEC', 0, 'EOF');
const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 2, 17) / 1000);
const mkOrder = (rid, age, lines) => ({ receiptId: rid, orderNumber: rid, createTs: SHIP - age * DAY, updateTs: SHIP - age * DAY + 60, shipBy: SHIP, buyer: { name: 'Buyer ' + rid.slice(-4) }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines });
const cuLine = rid => ({ transactionId: rid + '1', listingId: '18000' + rid.slice(-5), sku: 'CUSTOM-N-001-' + rid.slice(-6), title: 'Custom Name Necklace, Personalized Gold Charm Necklace', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: '14k Gold Filled' }], metalKey: 'gold', metalLabel: 'GF 14/20', personalization: [], buyerMessage: '' });
const line = (tid, sku, title, vars, metal) => ({ transactionId: tid, listingId: String(1800000000 + (+tid.slice(-5))), sku, title, quantity: 1, expectedShipDate: SHIP, variations: vars.map(([name, value]) => ({ name, value })), metalKey: metal || '', metalLabel: metal === 'silver' ? 'Sterling Silver' : metal === 'gold' ? 'GF 14/20' : '', personalization: [], buyerMessage: '' });
const CUSTOM = '4175423829', UNKNOWN = '4178100001', OPTIONS = '4178100002';
const ORDERS = [
  ...['4175423810', '4175423811', '4175423812', '4175423813'].map((rid, i) => mkOrder(rid, 9 - i, [cuLine(rid)])),
  mkOrder(CUSTOM, 5, [cuLine(CUSTOM)]),
  ...['4175423814', '4175423815', '4175423816'].map((rid, i) => mkOrder(rid, 4 - i, [cuLine(rid)])),
  mkOrder(UNKNOWN, 6, [line(UNKNOWN + '1', 'ROSE_77', 'Rose Charm', [['Metal', 'Sterling Silver']], 'silver')]),
  mkOrder('4175892473', 7, [line('41758924731', 'DAVID STAR', 'David Star Necklace', [['Metal Choice', 'Gold Filled']], 'gold')]),
  mkOrder(OPTIONS, 6, [line(OPTIONS + '1', 'BLOOMING_20239', 'Blooming Flower Charm Necklace', [['Metal', '14k Gold Filled'], ['Style', 'Wavy']], 'gold')]),
  mkOrder('4178100003', 5, [line('41781000031', 'BLOOMING_20239', 'Blooming Flower Charm Necklace', [['Metal', '14k Gold Filled'], ['Style', 'Wavy']], 'gold')])
];
const ONE = [{ metal: 'gold' }];

/* ── the starting points ──
   rid: the order sent; chip: the Review filter (null: Open, everything); designs: [{metal, qty, w, h}];
   from: card | designs (its designs window's footer) | orderwin (the order window, on view);
   run: false = no run open (it waits); placed: its sheet nested before the tour (a live run), not only queued;
   act: something done mid-flight; same: the key of starting points that send the same thing (compared) */
const SCENARIOS = [
  { name: 'review-custom', title: 'Custom Orders card, 1 piece, GF (queued, Manual)', rid: CUSTOM, chip: 'customOrder', designs: ONE, from: 'card', same: 'one-gf' },
  { name: 'review-unknown', title: 'Unknown SKU card, 1 piece, GF', rid: UNKNOWN, chip: 'needResolution', designs: ONE, from: 'card', same: 'one-gf' },
  { name: 'review-options', title: 'Options card, 1 piece, GF', rid: OPTIONS, chip: 'needResolution', designs: ONE, from: 'card', same: 'one-gf' },
  { name: 'review-open', title: 'Open list (no filter), custom card, 1 piece, GF', rid: CUSTOM, chip: null, designs: ONE, from: 'card', same: 'one-gf' },
  { name: 'designs-window', title: 'Designs window footer (#cuDlg), 1 piece, GF', rid: CUSTOM, chip: 'customOrder', designs: ONE, from: 'designs', same: 'one-gf' },
  { name: 'orderwin-overview', title: 'Order window, Overview tab, 1 piece, GF', rid: CUSTOM, chip: 'customOrder', designs: ONE, from: 'orderwin', view: 'info', same: 'one-gf' },
  { name: 'orderwin-sheet', title: 'Order window, Sheet tab, 1 piece, GF', rid: CUSTOM, chip: 'customOrder', designs: ONE, from: 'orderwin', view: 'sheet', same: 'one-gf' },
  { name: 'orderwin-timeline', title: 'Order window, Timeline tab, 1 piece, GF', rid: CUSTOM, chip: 'customOrder', designs: ONE, from: 'orderwin', view: 'timeline', same: 'one-gf' },
  { name: 'multi-piece', title: 'Custom card, 1 design x 3 pieces, one metal (GF)', rid: CUSTOM, chip: 'customOrder', designs: [{ metal: 'gold', qty: 3 }], from: 'card' },
  { name: 'multi-metal', title: 'Custom card, 2 designs on GF and SS (two sheets)', rid: CUSTOM, chip: 'customOrder', designs: [{ metal: 'gold' }, { metal: 'silver', w: 16, h: 22 }], from: 'card' },
  { name: 'rg-sheet', title: 'Custom card, 1 piece, RG sheet', rid: CUSTOM, chip: 'customOrder', designs: [{ metal: 'rose' }], from: 'card' },
  { name: 'placed', title: 'Custom card, 1 piece, GF, sheet nested (placed, not queued)', rid: CUSTOM, chip: 'customOrder', designs: ONE, from: 'card', placed: true },
  { name: 'new-sheet', title: 'Custom card, 1 piece, SS, whose sheet is an earlier run\'s (a new sheet is started)', rid: CUSTOM, chip: 'customOrder', designs: [{ metal: 'silver' }], from: 'card', fresh: 'silver' },
  { name: 'waiting', title: 'Custom card, 1 piece, GF, no run open (it waits)', rid: CUSTOM, chip: 'customOrder', designs: ONE, from: 'card', run: false },
  { name: 'double-click', title: 'Custom card, double-click on Send to Sheet', rid: CUSTOM, chip: 'customOrder', designs: ONE, from: 'card', dbl: true },
  { name: 'esc-mid', title: 'Esc mid-flight (two sheets)', rid: CUSTOM, chip: 'customOrder', designs: [{ metal: 'gold' }, { metal: 'silver' }], from: 'card', act: { kind: 'esc', after: 'nest', ms: 900 } },
  { name: 'skip-mid', title: 'Skip (a click) mid-flight (two sheets)', rid: CUSTOM, chip: 'customOrder', designs: [{ metal: 'gold' }, { metal: 'silver' }], from: 'card', act: { kind: 'skip', after: 'nest', ms: 900 } },
  { name: 'resize-mid', title: 'Window resized while a piece flies', rid: CUSTOM, chip: 'customOrder', designs: [{ metal: 'gold', qty: 2 }], from: 'card', placed: true, act: { kind: 'resize', after: 'piece', ms: 120, to: { width: 1180, height: 820 } } },
  { name: 'narrow', title: 'Narrow window (760 x 900), 1 piece, GF', rid: CUSTOM, chip: 'customOrder', designs: ONE, from: 'card', viewport: { width: 760, height: 900 } },
  { name: 'reduced-motion', title: 'prefers-reduced-motion, 1 piece, GF', rid: CUSTOM, chip: 'customOrder', designs: ONE, from: 'card', reduced: true }
];

/* ── in the page: every animation frame measured (installed before the page's own scripts) ── */
function probe() {
  const P = window.__P = { rec: false, t0: 0 };
  const A = Element.prototype.animate;
  Element.prototype.animate = function (k, o) {
    if (P.rec) {
      const props = new Set(); for (const f of Array.isArray(k) ? k : k ? [k] : []) for (const p of Object.keys(f)) if (!['offset', 'easing', 'composite'].includes(p)) props.add(p);
      const cls = typeof this.className === 'string' ? this.className : (this.getAttribute && this.getAttribute('class')) || this.tagName;
      P.anims.push({ cls: String(cls || this.id || this.tagName).slice(0, 40), props: [...props], t: Math.round(performance.now() - P.t0), ms: typeof o === 'number' ? o : o && o.duration });
    }
    return A.call(this, k, o);
  };
  try { new PerformanceObserver(l => { if (P.rec) for (const e of l.getEntries()) P.long.push([Math.round(e.startTime - P.t0), Math.round(e.duration)]); }).observe({ entryTypes: ['longtask'] }); } catch (_) {}
  try { new PerformanceObserver(l => { if (P.rec) for (const e of l.getEntries()) if (/^tour:/.test(e.name)) P.marks.push([e.name.slice(5), Math.round(e.startTime - P.t0)]); }).observe({ entryTypes: ['mark'] }); } catch (_) {}
  addEventListener('error', e => { if (P.rec) P.errs.push(String(e.message)); });
  const ids = new WeakMap(); let nid = 0;
  const idOf = n => { let i = ids.get(n); if (!i) { i = ++nid; ids.set(n, i); } return i; };
  const r1 = v => Math.round(v * 10) / 10, r2 = v => Math.round(v * 100) / 100;
  const desc = el => !el ? 'nothing (off screen)' : (el.closest('dialog') ? 'dialog#' + el.closest('dialog').id + ' ' : '') + el.tagName.toLowerCase() + (el.id ? '#' + el.id : '') + (typeof el.className === 'string' && el.className ? '.' + el.className.trim().split(/\s+/).slice(0, 2).join('.') : '');
  // effective opacity: the node's and every ancestor's (a caption inside a fading view fades with it)
  // (a caption inside a view fades with it: its own opacity, its card's and its view's; nothing else up the tree fades)
  const eff = (n, v) => { let o = +getComputedStyle(n).opacity; const card = n.parentElement && n.parentElement.closest('.sheetCard'); if (card) o *= +getComputedStyle(card).opacity; if (n.closest('#sheets')) o *= v[1]; else if (n.closest('#reviewView')) o *= v[0]; return o; };
  const viewOp = el => { if (!el || el.classList.contains('hidden') || el.hidden || !el.getClientRects().length) return 0; return +getComputedStyle(el).opacity; };
  const scaleOf = tr => { const m = /matrix\(([^)]+)\)/.exec(tr || ''); if (!m) return 1; const v = m[1].split(',').map(Number); return Math.hypot(v[0], v[1]); };
  const scrollerOf = el => { for (let a = el && el.parentElement; a && a !== document.body; a = a.parentElement) { const o = getComputedStyle(a).overflowY; if ((o === 'auto' || o === 'scroll') && a.scrollHeight > a.clientHeight + 1) return a; } return document.scrollingElement; };
  P.spots = () => {
    const out = [], rid = P.rid; if (!rid || typeof allSheets !== 'function') return out;   // eslint-disable-line no-undef
    for (const pg of allSheets()) for (const c of pg.charms || []) {   // eslint-disable-line no-undef
      if (!c.poolId || !String(c.poolId).startsWith(rid + '_')) continue;
      const prim = S.sheets[pg.metal], card = prim && prim.cardEl, shown = activePage(pg.metal) === pg;   // eslint-disable-line no-undef
      const p = pg.roseCutAt ? null : (pg.placements || []).find(q => q.id === c.id), cv = card && card.querySelector('[data-r="canvas"]'), v = pg._view;
      if (p && shown && cv && v && cv.width) {
        const r = cv.getBoundingClientRect(), s = r.width / cv.width, back = typeof faceOf === 'function' && faceOf(cv) === 'back', W = stockFor(pg.metal, pg).wPt * v.k;   // eslint-disable-line no-undef
        const cx = v.R + (back ? W - p.cxPt * v.k : p.cxPt * v.k), cy = v.R + p.cyPt * v.k, k = v.k * s * (p.scale || 1);
        out.push({ pid: c.poolId, kind: 'placed', x: r1(r.left + cx * s), y: r1(r.top + cy * s), w: r1(c.widthPt * k), h: r1(c.heightPt * k), rot: back ? -(+p.angle || 0) : +p.angle || 0, page: pg.page || 1, metal: pg.metal });
      } else {
        const q = card && [...card.querySelectorAll('[data-r="queue"] img[data-cid]')].find(i => i.dataset.cid === String(c.id));
        const r = q && q.getClientRects().length ? q.getBoundingClientRect() : null;
        out.push(r ? { pid: c.poolId, kind: 'queue', x: r1(r.left + r.width / 2), y: r1(r.top + r.height / 2), w: r1(r.width), h: r1(r.height), rot: 0, page: pg.page || 1, metal: pg.metal } : { pid: c.poolId, kind: shown ? 'none' : 'other-page', page: pg.page || 1, metal: pg.metal });
      }
    }
    return out;
  };
  const CAPS = '.nfCap, .mNote, .toasts > *, .mPlus, .cuTip';
  // a caption's words, each text node apart ("Order 4175423829" and "1 piece" are two)
  const words = n => { const w = document.createTreeWalker(n, NodeFilter.SHOW_TEXT), out = []; for (let x = w.nextNode(); x; x = w.nextNode()) if (x.textContent.trim()) out.push(x.textContent.trim()); return out.join(' ').replace(/\s+/g, ' ').slice(0, 90); };
  function sample(now) {
    const c0 = performance.now(), f = { t: r1(now - P.t0) };
    f.m = window.CN && CN.S ? CN.S.mode : '';
    const rv = document.getElementById('reviewView'), sh = document.getElementById('sheets');
    f.v = [r2(viewOp(rv)), r2(viewOp(sh))];
    f.dl = [...document.querySelectorAll('dialog[open]')].map(d => d.id || d.className).join(',');
    const L = document.getElementById('tourLayer');
    const nodes = L ? [...L.children].filter(n => !/tourSpark|tourRing/.test(n.className)) : [];
    for (const n of document.querySelectorAll(CAPS)) nodes.push(n);
    for (const n of document.querySelectorAll('#motionLayer .nfPiece, #motionLayer > .mGhost, .motionLayer > .mGhost')) nodes.push(n);
    f.n = [];
    let probe = null;
    for (const n of nodes) {
      const cs = getComputedStyle(n); if (cs.display === 'none' || cs.visibility === 'hidden') continue;
      const op = n.closest('#tourLayer') ? +cs.opacity : eff(n, f.v); if (op < .03) continue;
      const r = n.getBoundingClientRect(); if (!r.width || !r.height) continue;
      const id = idOf(n);
      if (!P.info[id]) P.info[id] = { cls: (c => c.find(x => /^tour/.test(x)) || c[0])(String(n.className || '').split(/\s+/)) || n.tagName, text: words(n), w0: n.offsetWidth, h0: n.offsetHeight, first: f.t };
      const row = [id, r1(r.left + r.width / 2), r1(r.top + r.height / 2), r1(r.width), r1(r.height), r2(op), r2(scaleOf(cs.transform))];
      if (/tourCoin|tourPiece/.test(n.className) && op > .3) (probe = probe || []).push([row, n]);
      f.n.push(row);
    }
    // the coin and the pieces: is each seen at its centre? (the tour layer lets pointers through: probed, for this frame only)
    if (probe) {
      L.classList.add('__probe');
      f.hit = probe.map(([row, n]) => {
        const x = row[1], y = row[2], el = document.elementFromPoint(x, y);
        if (el && (el === n || n.contains(el))) return [row[0], ''];
        // (under another piece leaving the same coin, or under the coin: the carried design itself, not hidden)
        if (el && el.closest('#tourLayer > .tourCoin, #tourLayer > .tourPiece')) return [row[0], ''];
        const dlg = el && el.closest('dialog'), inBox = dlg && (() => { const b = dlg.firstElementChild ? dlg.firstElementChild.getBoundingClientRect() : dlg.getBoundingClientRect(); return x >= b.left && x <= b.right && y >= b.top && y <= b.bottom; })();
        return [row[0], el && el.closest('#tourLayer') ? 'tour caption/overlay ' + desc(el) : dlg && !inBox ? 'behind the backdrop of ' + desc(dlg) : desc(el)];
      });
      L.classList.remove('__probe');
    }
    const cd = P.rid && document.querySelector(`#rvList .reviewListRow[data-rid="${P.rid}"]`);
    if (cd && cd.getClientRects().length && f.v[0] > .02) { const r = cd.getBoundingClientRect(); f.cd = [r1(r.left + r.width / 2), r1(r.top + r.height / 2), r1(r.width), r1(r.height), r2(+getComputedStyle(cd).opacity * f.v[0])]; }
    const fc = document.querySelector('.sheetCard.nfFocus');
    if (fc) { const r = fc.getBoundingClientRect(); f.fc = [r1(r.left), r1(r.top), r1(r.width), r1(r.height)]; }
    const rs = document.querySelector('#reviewView .egPane.scroll'), card0 = document.querySelector('.sheetCard'), ns = card0 ? scrollerOf(card0) : null;
    f.sc = [rs ? Math.round(rs.scrollTop) : -1, ns ? Math.round(ns.scrollTop) : -1];
    // where each view's box stands (a rail folding or unfolding moves it)
    if (f.v[0] > .02 && rv) { const r = rv.getBoundingClientRect(); f.bx = [r1(r.left), r1(r.width)]; }
    else if (f.v[1] > .02 && sh) { const r = sh.getBoundingClientRect(); f.bx = [r1(r.left), r1(r.width)]; }
    if (f.v[1] > .02 && typeof allSheets === 'function') f.pend = allSheets().filter(p => p._drawFrame || p._redrawPending).length;   // eslint-disable-line no-undef
    if (f.n.some(r => P.info[r[0]].cls === 'tourPiece')) f.sp = P.spots();
    P.frames.push(f);
    P.cost.push(performance.now() - c0);
  }
  const loop = t => { if (P.rec) { try { sample(t); } catch (e) { P.errs.push('probe: ' + e.message); } } requestAnimationFrame(loop); };
  requestAnimationFrame(loop);
  const css = () => { if (document.getElementById('__probeCss')) return; const s = document.createElement('style'); s.id = '__probeCss'; s.textContent = '#tourLayer.__probe > :is(.tourCoin, .tourPiece, .tourCap, .tourPlus), #tourLayer.__probe > :is(.tourCoin, .tourPiece, .tourCap, .tourPlus) * { pointer-events: auto !important }'; (document.head || document.documentElement).appendChild(s); };
  window.__start = rid => {
    css(); Object.assign(P, { rec: true, rid, t0: performance.now(), frames: [], long: [], anims: [], marks: [], errs: [], info: {}, cost: [], card0: null });
    const cd = document.querySelector(`#rvList .reviewListRow[data-rid="${rid}"]`), rs = document.querySelector('#reviewView .egPane.scroll');
    if (cd && cd.getClientRects().length) { const r = cd.getBoundingClientRect(); P.card0 = { box: [r1(r.left), r1(r.top), r1(r.width), r1(r.height)], scroll: rs ? Math.round(rs.scrollTop) : -1, vh: innerHeight }; }
    return performance.timeOrigin + P.t0;
  };
  window.__stop = () => { P.rec = false; return { frames: P.frames, long: P.long, anims: P.anims, marks: P.marks, errs: P.errs, info: P.info, cost: P.cost, card0: P.card0 }; };
}

/* ── the page, as the user left it ── */
function viewState() {
  const q = s => document.querySelector(s);
  const ow = window.OrderWin && OrderWin.isOpen() ? { key: OrderWin.key(), view: (q('#orderWin [data-ow-view][aria-selected="true"]') || {}).dataset?.owView || null, draft: (q('#owInput') || {}).value || '' } : null;
  return { mode: CN.S.mode, chip: (q('#reviewView .ordBar .egTab.on') || {}).dataset?.k || null, cseg: (q('#reviewView .rvSeg .on') || {}).dataset?.cseg || null,
    scroll: Math.round(q('#reviewView .egPane.scroll')?.scrollTop ?? -1), maxScroll: (e => e ? Math.round(e.scrollHeight - e.clientHeight) : -1)(q('#reviewView .egPane.scroll')), orderWin: ow, designs: !!q('#cuDlg')?.open,
    dialogs: [...document.querySelectorAll('dialog[open]')].map(d => d.id).join(',') || '',
    nestPages: METALS.map(m => m.key + ':' + S.sheets[m.key].active).join(' ') };   // eslint-disable-line no-undef
}
/** The sheets: each of the order's pieces once, nothing held back, the Rose Gold lines and cuts. */
function sheetState(rid) {
  const pages = allSheets(), count = new Map();   // eslint-disable-line no-undef
  for (const p of pages) for (const c of p.charms || []) if (c.poolId) count.set(c.poolId, (count.get(c.poolId) || 0) + 1);
  const placeDup = [];
  for (const p of pages) { const seen = new Set(); for (const q of p.placements || []) { if (seen.has(q.id)) placeDup.push(`${p.metal}:${p.page || 1}:${q.id}`); seen.add(q.id); } }
  const mine = [...count].filter(([id]) => String(id).startsWith(rid + '_'));
  return { mine: mine.map(([id, n]) => ({ id, n, where: pages.filter(p => (p.charms || []).some(c => c.poolId === id)).map(p => `${p.metal}:${p.page || 1}`) })),
    dup: [...count].filter(([, n]) => n > 1).map(([id, n]) => id + '×' + n), placeDup,
    rose: pages.filter(p => p.metal === 'rose').map(p => ({ page: p.page || 1, cutAt: p.roseCutAt || null, hist: (p.roseHistory || []).length, plan: !!p.rosePlan, prot: !!p.roseProtected, lines: p.rosePlan && p.rosePlan.lines ? p.rosePlan.lines.length : null })),
    hidden: pages.filter(p => p._tourHide && p._tourHide.size).map(p => `${p.metal}:${p.page || 1}`),
    rows: B.orders.rows.filter(r => String(r.order.receiptId) === rid).map(r => ({ key: r.key, state: r.state, reason: r.reason || null, ids: (r.poolIds || []).length })) };   // eslint-disable-line no-undef
}
const leftovers = () => ({ layer: document.querySelectorAll('#tourLayer > *').length, lit: document.querySelectorAll('.sheetCard.nfFocus, .nfCap, .nfGlow, .nfPiece, .nfRing').length,
  dim: ['nfDim', 'nfOn'].filter(c => document.getElementById('sheets').classList.contains(c)), nfOpen: !!(window.NestFocus && NestFocus.isOpen && NestFocus.isOpen()),
  views: [...document.querySelectorAll('#sheets, #reviewView')].map(v => v.getAnimations().filter(a => a.playState !== 'finished').length).reduce((a, b) => a + b, 0),
  inert: [...document.querySelectorAll('#reviewView, #sheets')].filter(v => getComputedStyle(v).opacity !== '1' && !v.classList.contains('hidden')).map(v => v.id) });

/* ── one starting point ── */
async function run(browser, sc) {
  const { start } = require(path.join(here, 'tests/charm-nest/bridge-server.cjs'));
  const srv = await start({ receipts: [] });
  const vp = sc.viewport || { width: 1440, height: 950 }, cores = os.cpus().length;
  const context = await browser.newContext({ viewport: vp, reducedMotion: sc.reduced ? 'reduce' : 'no-preference' });
  const res = { name: sc.name, title: sc.title, sc, errors: [], notes: [], load: [os.loadavg()[0] / cores] };
  const shots = [];
  let cdp = null;
  try {
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.g/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; });
    await context.addInitScript(probe);
    const page = await context.newPage(); page.setDefaultTimeout(30000);
    page.on('pageerror', e => res.errors.push(String(e.message || e).slice(0, 300)));
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.Review && window.CustomSheet && window.SendTour && window.OrderWin && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await page.evaluate(async ({ orders, run }) => {
      await Orders.loadMaps(true);
      for (const order of orders) for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
      Orders.interpretAll(); Review.syncOrderItems(); CN.setMode('review'); Review.render();
      if (run) { const day = new Date().toISOString().slice(0, 10); B.run = { runId: `run-${day}-adv`, day, setId: null, releasePolicy: 2, solidIncluded: {}, step: 'nest', status: 'running', mode: 'manual', startedAt: Date.now(), updatedAt: Date.now(), lines: {}, sheets: {}, holds: {}, errors: [], resumable: true, stoppedBy: null, fix: null, orders: [] }; }
    }, { orders: ORDERS, run: sc.run !== false });
    // (a chip is a toggle: a second click on the chip that is on turns it off, which is the Open list)
    const chipOn = () => page.evaluate(() => (document.querySelector('#reviewView .ordBar .egTab.on') || {}).dataset?.k || null);
    const chip = async k => { const cur = await chipOn(); if (k) { if (cur !== k) await page.click(`#reviewView .egTab[data-k="${k}"]`); } else if (cur) await page.click('#reviewView .rvSeg [data-cseg="open"]'); await page.waitForTimeout(250); };
    const card = `#rvList .reviewListRow[data-rid="${sc.rid}"]`;
    // its designs dropped on its card, each on its metal, with its count of copies
    await chip(sc.chip || (sc.rid === UNKNOWN || sc.rid === OPTIONS ? 'needResolution' : 'customOrder'));
    await page.waitForSelector(card);
    await page.evaluate(({ sel, files }) => { const dt = new DataTransfer(); for (const f of files) dt.items.add(new File([f.text], f.name)); const n = document.querySelector(sel); n.scrollIntoView({ block: 'center' }); for (const t of ['dragenter', 'dragover', 'drop']) n.dispatchEvent(new DragEvent(t, { bubbles: true, cancelable: true, dataTransfer: dt, clientX: 700, clientY: 400 })); { const a = document.querySelector('#cuAsk [data-ask-yes]'); if (a) a.click(); } },
      { sel: card, files: sc.designs.map((d, i) => ({ name: `design-${i + 1}.dxf`, text: DXF(d.w || 18, d.h || 20) })) });
    await page.waitForFunction(n => document.querySelectorAll('#cuDlg[open] .cuFile .cuThumb img').length === n, sc.designs.length, { timeout: 30000 });
    for (const [i, d] of sc.designs.entries()) {
      await page.click(`#cuDlg .cuFile:nth-child(${i + 1}) .cuM[data-m="${d.metal}"]`);
      for (let q = 1; q < (d.qty || 1); q++) await page.click(`#cuDlg .cuFile:nth-child(${i + 1}) [data-q="1"]`);
    }
    await page.waitForTimeout(400);
    if (sc.from !== 'designs') {
      await page.click('#cuDlg [data-x]');
      await page.waitForFunction(sel => document.querySelector(sel + ' .cuDesigns.ready') && !document.querySelector('dialog[open]'), card, { timeout: 15000 });
      await page.waitForTimeout(700);
      if (sc.chip !== undefined) await chip(sc.chip);
    }
    if (sc.chip && (await chipOn()) !== sc.chip) res.notes.push(`the ${sc.chip} chip is not on: ${await chipOn()}`);
    // the list scrolled so the card stands in the middle (the scroll to come back to)
    await page.evaluate(sel => { const n = document.querySelector(sel); if (n) n.scrollIntoView({ block: 'center' }); }, card);
    if (sc.from === 'orderwin') {
      await page.click(card + ' .purchaseSummary');
      await page.waitForFunction(() => OrderWin.isOpen(), null, { timeout: 10000 });
      await page.waitForTimeout(800);
      if (sc.view !== 'info') { await page.click(`#orderWin [data-ow-view="${sc.view}"]`); await page.waitForTimeout(900); }
      // something typed, as a person leaves it mid-thought: the Team message box
      if (await page.isVisible('#owInput')) { await page.fill('#owInput', 'Draft for the team: check the lion'); res.typed = true; }
    }
    // a live run: its sheet nested at once, before the tour shows it (Review.repool, then the sheet's Nest, awaited)
    if (sc.placed) await page.evaluate(() => {
      const rp = Review.repool;
      Review.repool = async function (r) {
        const out = await rp.apply(this, arguments), ids = r.poolIds || [], t0 = Date.now();
        const pagesOf = () => allSheets().filter(p => (p.charms || []).some(c => ids.includes(c.poolId)));   // eslint-disable-line no-undef
        for (const p of pagesOf()) try { startNest(p); } catch (e) { window.__nestErr = e.message; }   // eslint-disable-line no-undef
        while (Date.now() - t0 < 90000) {
          const pgs = pagesOf();
          if (pgs.length && pgs.every(p => !['nesting', 'finishing', 'queued'].includes(p.status)) && ids.every(id => pgs.some(p => { const c = p.charms.find(x => x.poolId === id); return !c || (p.placements || []).some(q => q.id === c.id); }))) break;
          for (const p of pgs) if (!['nesting', 'finishing', 'queued'].includes(p.status) && p.charms.some(c => ids.includes(c.poolId) && !(p.placements || []).some(q => q.id === c.id)) && Date.now() - t0 > 1500) try { startNest(p); } catch (_) {}   // eslint-disable-line no-undef
          await new Promise(res => setTimeout(res, 150));
        }
        (window.__nested = window.__nested || []).push({ ms: Date.now() - t0, pages: pagesOf().map(p => `${p.metal}:${p.page || 1}:${p.status}:${(p.placements || []).length}/${p.charms.length}`) });
        return out;
      };
    });
    // a metal whose last sheet is an earlier run's: the pool starts a new sheet for what is sent now (attachPool)
    if (sc.fresh) res.freshPages = await page.evaluate(m => { const pg = pagesOf(m).at(-1); pg.runId = 'run-earlier'; return pagesOf(m).length; }, sc.fresh);   // eslint-disable-line no-undef
    // the Rose Gold cuts and lines the fake cloud has recorded so far
    const roseCalls = () => srv.st.calls.filter(c => /rose|cut|green/i.test(String(c.op || '')) || /rose/i.test(c.name)).map(c => `${c.name}:${c.op}`);
    const rose0 = roseCalls().length;
    const before = await page.evaluate(viewState), sheets0 = await page.evaluate(sheetState, sc.rid);
    res.before = before;
    await page.waitForTimeout(500);
    // the screen, as a screencast (its frames: the strip, and each frame's change from the one before)
    if (STRIP) {
      cdp = await context.newCDPSession(page);
      cdp.on('Page.screencastFrame', e => { shots.push({ ts: e.metadata.timestamp * 1000, data: e.data }); cdp.send('Page.screencastFrameAck', { sessionId: e.sessionId }).catch(() => {}); });
      await cdp.send('Page.startScreencast', { format: 'jpeg', quality: 55, maxWidth: 720, maxHeight: 720, everyNthFrame: 1 });
      await page.waitForTimeout(250);
    }
    const origin = await page.evaluate(rid => __start(rid), sc.rid);
    // the press
    let how = 'button';
    if (sc.from === 'designs') await page.click('#cuDlg [data-send]');
    else if (sc.from === 'orderwin') {
      const btn = await page.evaluateHandle(() => [...document.querySelectorAll('#orderWin [data-cu-send], #orderWin [data-send], #orderWin button')].find(b => /send to sheet/i.test(b.textContent || '') && b.getClientRects().length && getComputedStyle(b).visibility !== 'hidden' && b.getAttribute('aria-disabled') !== 'true') || null);
      if (btn && await btn.evaluate(b => !!b)) await btn.click();
      else { how = 'CustomSheet.send (the order window has no Send to Sheet of its own)'; await page.evaluate(sel => { const n = document.querySelector(sel); CustomSheet.send(n._cuIt); }, card); }
    } else if (sc.dbl) await page.dblclick(card + ' [data-cu-send]');
    else await page.click(card + ' [data-cu-send]');
    res.how = how;
    // mid-flight
    if (sc.act) {
      try {
        if (sc.act.after === 'nest') await page.waitForFunction(() => CN.S.mode === 'nest', null, { timeout: 20000, polling: 'raf' });
        else if (sc.act.after === 'piece') await page.waitForSelector('#tourLayer .tourPiece', { timeout: 120000 });
        await page.waitForTimeout(sc.act.ms || 0);
        res.actAt = await page.evaluate(() => Math.round(performance.now() - __P.t0));
        if (sc.act.kind === 'esc') await page.keyboard.press('Escape');
        else if (sc.act.kind === 'skip') { const skip = await page.$('.tourSkip'); if (skip && await skip.isVisible()) { await skip.click(); res.notes.push('pressed the tour\'s own Skip'); } else { await page.mouse.click(Math.round(vp.width * .5), Math.round(vp.height * .55)); res.notes.push('no Skip button on screen: a click in the middle of the page'); } }
        else if (sc.act.kind === 'resize') await page.setViewportSize(sc.act.to);
      } catch (e) { res.notes.push('mid-flight action not done: ' + e.message.split('\n')[0]); }
    }
    // until home (or, with no tour, a moment), then a settle
    const began = await page.waitForFunction(() => __P.marks.some(m => m[0] === 'start') || SendTour.playing(), null, { timeout: sc.placed ? 150000 : sc.reduced ? 4000 : 25000, polling: 50 }).then(() => true).catch(() => false);
    if (began) await page.waitForFunction(() => !SendTour.playing() && __P.marks.some(m => m[0] === 'end'), null, { timeout: 60000, polling: 50 }).catch(() => res.notes.push('the tour did not end within 60 s'));
    else await page.waitForTimeout(2500);
    await page.waitForTimeout(1300);
    res.after = await page.evaluate(viewState);
    res.left = await page.evaluate(leftovers);
    res.rec = await page.evaluate(() => __stop());
    res.sheets0 = sheets0; res.sheets1 = await page.evaluate(sheetState, sc.rid);
    res.rose = roseCalls().slice(rose0);
    res.nested = await page.evaluate(() => window.__nested || null);
    res.vp = vp; res.origin = origin; res.began = began; res.load.push(os.loadavg()[0] / cores);
    if (cdp) { await cdp.send('Page.stopScreencast').catch(() => {}); }
    // a still of the end, too
    res.endShot = path.join(OUT, `${sc.name}-end.png`);
    await page.screenshot({ path: res.endShot });
  } catch (e) {
    res.crash = String(e && e.stack || e).split('\n').slice(0, 3).join(' | ');
  } finally { await context.close().catch(() => {}); try { await srv.close(); } catch (_) {} }
  res.shots = shots.map(s => ({ t: Math.round(s.ts - (res.origin || 0)), data: s.data }));
  return res;
}

/* ── the analysis of one run ── */
const pct = (a, p) => { if (!a.length) return 0; const s = [...a].sort((x, y) => x - y); return s[Math.min(s.length - 1, Math.floor(p * s.length))]; };
const med = a => pct(a, .5);
const R = v => Math.round(v);

/* ── landing only: each piece's last solid frame against the nearest spot of the order the sheet has (its place, or its thumbnail in the queue) ── */
function landing(res) {
  if (res.crash) return { err: 'the starting point did not run: ' + res.crash, landed: [] };
  const rec = res.rec, F = rec.frames, info = rec.info, clsOf = id => (info[id] || {}).cls || '?';
  const tracks = new Map();
  for (const f of F) for (const row of f.n) { const id = row[0]; if (!tracks.has(id)) tracks.set(id, []); tracks.get(id).push({ t: f.t, x: row[1], y: row[2], w: row[3], h: row[4], op: row[5], f }); }
  const landed = [];
  for (const [id, tr] of tracks) {
    if (clsOf(id) !== 'tourPiece') continue;
    const solid = tr.filter(p => p.op > .9), last = solid[solid.length - 1] || tr[tr.length - 1];
    const spots = (last.f.sp || []).filter(s => s.x != null);
    if (!spots.length) { landed.push({ err: 'no spot of the order on screen at its last solid frame' }); continue; }
    const best = spots.map(s => ({ s, d: Math.hypot(s.x - last.x, s.y - last.y) })).sort((a, b) => a.d - b.d)[0];
    landed.push({ px: +best.d.toFixed(1), kind: best.s.kind, at: [R(last.x), R(last.y)], spot: [R(best.s.x), R(best.s.y)] });
  }
  return { landed, nPieces: (res.sheets1.mine || []).length };
}
(async () => {
  const t0 = Date.now();
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1243/chrome-linux64/chrome', args: ['--no-sandbox'] });
  const want = ONLY.length ? ONLY : ['review-custom', 'review-open'], fails = [];
  try {
    for (const name of want) {
      const sc = SCENARIOS.find(s => s.name === name); if (!sc) { fails.push(name + ': no such start'); continue; }
      const t = Date.now(), res = await run(browser, sc), L = landing(res);
      const far = L.err ? [L.err] : L.landed.filter(l => l.err || l.px > 12).map(l => l.err || `${l.px} px from its ${l.kind} spot (ended at ${l.at}, the spot at ${l.spot})`);
      if (!L.err && L.landed.length < L.nPieces) far.push(`${L.landed.length} piece flight(s) for ${L.nPieces} piece(s) sent`);
      if (!L.err && !L.landed.length) far.push('no piece flew');
      console.log(`  ${far.length ? '✗' : '✓'} ${name} (${Math.round((Date.now() - t) / 1000)} s): ${far.length ? far.join('; ') : L.landed.map(l => `${l.px} px from its ${l.kind === 'queue' ? 'thumbnail in the queue' : l.kind}`).join(', ')}`);
      if (far.length) fails.push(name);
    }
  } finally { await browser.close(); }
  console.log(`\ntour-landing: ${fails.length ? fails.length + ' failed' : 'all passed'} (${Math.round((Date.now() - t0) / 1000)} s)`);
  process.exit(fails.length ? 1 : 0);
})().catch(e => { console.error(e); process.exit(2); });
