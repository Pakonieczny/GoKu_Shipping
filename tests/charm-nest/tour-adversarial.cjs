// The Send to Sheet tour, played from every place it can start and measured frame by frame (Paul, 29 Sep 01:30: "an
// adversarial agent to make sure that this animation is absolutely perfect. All the timings doesn't matter where it
// starts from. Everything has to be super smooth and nice and feel great and currently feels jagged at this jointed and
// jumpy"; from the order window, "half the animation is not visible because it's blocked by the pop-up").
//
// Each starting point runs on its own fresh fake site (bridge-server.cjs: no Etsy, no paid AI, no live data) in a real
// Chromium. While the tour plays, every animation frame records the coin, the pieces, the captions, the views, the
// windows, the spotlit sheet card and the scroll positions, and the screen is recorded as a screencast. The checks
// (one line each, PASS / FAIL / WARN):
//   frames    requestAnimationFrame deltas and long tasks, from the press until home
//   jumps     the coin or a piece moving or resizing more in one frame than a smooth curve allows; the hand-off from
//             the coin to a piece; a caption or the spotlit card snapping; the screencast's biggest one-frame changes
//   copies    anything flying besides the tour's own coin and pieces (a second copy of the card, a "+1" at the tab)
//   window    the coin or a piece behind an open window (the order window, the designs window: modal top layer)
//   covered   the coin (or a piece) hidden at its centre (document.elementFromPoint, the tour layer probed) by a
//             note, a caption or anything else, or off screen, when no window is over it
//   captions  two captions over each other, a caption over the coin, a caption shown too briefly to read (< 1.2 s)
//   tabs      a tab switch that pops, flashes, leaves the screen blank, shows a Nest tab not drawn yet, or moves the page
//   home      the view at the end against the view at the start: tab, filter, Open/Completed, scroll, the order window
//             (open, which tab, what was typed), the designs window
//   leftovers nothing left on the tour layer, spotlit, dimmed or held back from a sheet
//   pacing    the total and each phase's duration; the coin's top speed; each move's length
//   landing   each piece comes down where the sheet really has it (its placement, or its thumbnail in the queue)
//   integrity no piece of the order twice on the sheets, no Rose Gold green line or cut made by the send or the tour
//   motion    only transform, opacity and clip-path animated
//   errors    no page errors
//   skip / reduced / press   Esc and a click end the tour at once with everything shown; reduced motion plays the
//             gentle version (nothing flies or glides: the Nest tab and the sheet shown, the same captions over short
//             fades, then home with a note); how Send to Sheet was pressed (a real button, or a call)
//   (pacing allows one arc: up into the Nest tab and down onto the sheet; any other turn back is flagged)
// and, across the starting points that send the same order (one piece, GF): the same total and phases (consistency).
// No Etsy calls, no AI calls, no live data: the fake's Rose Gold cloud calls are recorded to prove none were made.
//
// Writes, to SHOTS (default /mnt/project-files/plans/tour-adversarial): findings.md (ranked, worst first, each with
// what the user sees, the numbers, a frame strip, a suggested fix and an owner), results.json, and a frame strip per
// starting point (<scenario>.png).
//   ONLY=<name,name>   run only those starting points (names in SCENARIOS below), writing findings-partial.md
//                      (FINDINGS=1: write findings.md anyway);  STRIP=0  no screencast (frame times unperturbed)
//   RAW=1              also keep every frame's data (raw-<name>.json);  REPORT=1  write findings.md again from
//                      results.json without playing anything
//   NODE_PATH=$(npm root -g) PW_DIR=$(npm root -g)/playwright/node_modules \
//     CHROMIUM=$(ls -d /opt/pw-browsers/chromium-*/chrome-linux/chrome|head -1) SHOTS=<dir> \
//     node tests/charm-nest/tour-adversarial.cjs          (exit 1 when any check FAILs)
'use strict';
const path = require('path'), fs = require('fs'), os = require('os');
const here = path.join(__dirname, '../..');
const pwDir = process.env.PW_DIR || path.join(here, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const OUT = process.env.SHOTS || '/mnt/project-files/plans/tour-adversarial';
const ONLY = (process.env.ONLY || '').split(',').map(s => s.trim()).filter(Boolean);
const STRIP = process.env.STRIP !== '0';
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
  { name: 'review-unknown', title: 'Unknown SKU card, 1 piece, GF', rid: UNKNOWN, chip: 'unmatchedSku', designs: ONE, from: 'card', same: 'one-gf' },
  { name: 'review-options', title: 'Options card, 1 piece, GF', rid: OPTIONS, chip: 'needsMapping', designs: ONE, from: 'card', same: 'one-gf' },
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
  const ow = window.OrderWin && OrderWin.isOpen() ? { key: OrderWin.key(), view: (q('#orderWin [data-ow-view][aria-selected="true"]') || {}).dataset?.owView || null, draft: (q('#owInput') || {}).value || '', note: (q('#owNote') || {}).value || '' } : null;
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
    await chip(sc.chip || (sc.rid === UNKNOWN ? 'unmatchedSku' : sc.rid === OPTIONS ? 'needsMapping' : 'customOrder'));
    await page.waitForSelector(card);
    await page.evaluate(({ sel, files }) => { const dt = new DataTransfer(); for (const f of files) dt.items.add(new File([f.text], f.name)); const n = document.querySelector(sel); n.scrollIntoView({ block: 'center' }); for (const t of ['dragenter', 'dragover', 'drop']) n.dispatchEvent(new DragEvent(t, { bubbles: true, cancelable: true, dataTransfer: dt, clientX: 700, clientY: 400 })); },
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
function analyse(res) {
  const out = { checks: [], metrics: {} }, sc = res.sc;
  const check = (id, status, text, data) => out.checks.push({ id, status, text, data: data || null });
  if (res.crash) { check('ran', 'FAIL', 'the starting point did not run: ' + res.crash); return out; }
  const rec = res.rec, F = rec.frames, info = rec.info, mk = n => rec.marks.filter(m => m[0] === n).map(m => m[1]);
  const t0 = 0, tStart = mk('start')[0], tEnd = mk('end').slice(-1)[0];
  const tourOn = tStart != null && tEnd != null;
  const clsOf = id => (info[id] || {}).cls || '?';
  // ── pacing: the phases ──
  const sw = mk('switch:nest'), back = mk('switch:review'), lands = mk('land');
  const ph = {};
  if (tourOn) {
    ph.pressToStart = tStart; ph.total = tEnd - tStart; ph.pressToHome = tEnd;
    if (sw[0] != null) ph.liftToNest = sw[0] - tStart;
    if (lands.length && sw[0] != null) ph.nestToFirstLanding = lands[0] - sw[0];
    if (lands.length > 1) ph.landingGaps = lands.slice(1).map((t, i) => t - lands[i]);
    if (back[0] != null && lands.length) ph.lastLandingToHome = back[0] - lands[lands.length - 1];
    if (back[0] != null) ph.homeSwitchToEnd = tEnd - back[0];
    ph.inNest = back[0] != null && sw[0] != null ? back[0] - sw[0] : null;
  }
  out.metrics.phases = ph; out.metrics.marks = rec.marks;
  // ── frames ──
  const win = F.filter(f => f.t >= t0 && f.t <= (tEnd != null ? tEnd + 300 : 1e9));
  const d = []; for (let i = 1; i < win.length; i++) d.push([win[i].t, win[i].t - win[i - 1].t]);
  const dd = d.map(x => x[1]), over34 = d.filter(x => x[1] > 34), long = rec.long.filter(l => l[1] > 50 && l[0] + l[1] >= t0 && l[0] <= (tEnd != null ? tEnd : 1e9));
  const tourD = d.filter(x => tStart != null && x[0] >= tStart);
  const phaseAt = t => { const ms = rec.marks.filter(m => m[1] <= t); return ms.length ? ms[ms.length - 1][0] + ' +' + R(t - ms[ms.length - 1][1]) : 'press +' + R(t); };
  out.metrics.frames = { n: dd.length, worst: R(Math.max(0, ...dd)), p95: R(pct(dd, .95)), over34: over34.length, tourWorst: R(Math.max(0, ...tourD.map(x => x[1]))), tourOver34: tourD.filter(x => x[1] > 34).length, tourOver50: tourD.filter(x => x[1] > 50).length, tourN: tourD.length, long: long.map(l => ({ at: l[0], ms: l[1], phase: phaseAt(l[0]) })), probeMs: +(rec.cost.reduce((a, b) => a + b, 0) / Math.max(1, rec.cost.length)).toFixed(2) };
  const fm = out.metrics.frames;
  // A stutter the eye catches: a long task while something moves, or a frame over 50 ms (three frames' worth). Frames
  // over 34 ms are counted too, but the probe (rAF sampling) and the screencast take some of those themselves, and a
  // busy machine (load per core over 1.2) drops them on its own: both soften the verdict to a warning.
  const busy = Math.max(...(res.load || [0])) > 1.2;
  const stutter = fm.long.length || fm.tourOver50 > Math.max(2, fm.tourN * .02);
  if (!sc.reduced || tourOn) check('frames', stutter ? (busy ? 'WARN' : 'FAIL') : fm.tourOver34 > Math.max(3, fm.tourN * .05) ? 'WARN' : 'PASS',
    (busy ? `busy machine (load per core ${res.load.map(x => x.toFixed(1)).join(' → ')}): ` : '') + `worst frame ${fm.worst} ms (in the tour ${fm.tourWorst} ms), ${fm.tourOver50}/${fm.tourN} tour frames over 50 ms, ${fm.tourOver34} over 34 ms, p95 ${fm.p95} ms, long tasks ${fm.long.length ? fm.long.map(l => `${l.ms} ms at ${l.phase}`).join('; ') : 'none'} (probe ${fm.probeMs} ms/frame${STRIP ? ', screencast on' : ''})`, { slow: d.filter(x => x[1] > 34).map(x => [R(x[0]), R(x[1]), phaseAt(x[0])]) });
  // ── tracks: each node's frames ──
  const tracks = new Map();
  for (const f of F) for (const row of f.n) { const id = row[0]; if (!tracks.has(id)) tracks.set(id, []); tracks.get(id).push({ t: f.t, x: row[1], y: row[2], w: row[3], h: row[4], op: row[5], s: row[6], f }); }
  // jumps: a one-frame move far beyond its neighbours' (a spike), or so far that no eye follows it; a size snap
  const jumps = [];
  for (const [id, tr] of tracks) {
    const cls = clsOf(id); if (!/tourCoin|tourPiece|tourCap|tourPlus|tourCard|mGhost|nfCap|mNote|nfPiece/.test(cls)) continue;
    const dist = []; for (let i = 1; i < tr.length; i++) dist.push(Math.hypot(tr[i].x - tr[i - 1].x, tr[i].y - tr[i - 1].y) / Math.max(1, (tr[i].t - tr[i - 1].t) / 16.7));
    for (let i = 1; i < tr.length; i++) {
      const a = tr[i - 1], b = tr[i]; if (b.t - a.t > 80) continue;   // (not seen for a while: a new appearance, judged below)
      const step = Math.hypot(b.x - a.x, b.y - a.y), perFrame = dist[i - 1];
      const around = dist.slice(Math.max(0, i - 5), i - 1).concat(dist.slice(i, i + 4)), base = around.length ? Math.max(...around) : 0;
      // (a size change per 16.7 ms frame, as a log ratio, against its neighbours': a long frame shrinks it more, evenly)
      const sizeA = Math.sqrt(a.w * a.h), lr = k => k < 1 || k >= tr.length ? 0 : Math.log(Math.sqrt(tr[k].w * tr[k].h) / Math.max(1, Math.sqrt(tr[k - 1].w * tr[k - 1].h))) * 16.7 / Math.max(8, tr[k].t - tr[k - 1].t);
      const lri = Math.abs(lr(i)), lrBase = Math.max(0, ...[i - 3, i - 2, i - 1, i + 1, i + 2, i + 3].map(k => Math.abs(lr(k)))), size = Math.exp(lr(i));
      const vis = Math.min(a.op, b.op) > .35;
      if (vis && step > 36 && perFrame > 2.4 * Math.max(8, base)) jumps.push({ id, cls, at: b.t, px: R(step), dt: R(b.t - a.t), around: R(base), kind: 'move', phase: phaseAt(b.t) });
      else if (vis && perFrame > 90) jumps.push({ id, cls, at: b.t, px: R(step), dt: R(b.t - a.t), around: R(base), kind: 'too fast to follow', phase: phaseAt(b.t) });
      if (vis && /tourCoin|tourPiece|tourCard|mGhost/.test(cls) && lri > Math.log(1.15) && lri > 2.5 * lrBase && sizeA > 8) jumps.push({ id, cls, at: b.t, ratio: +size.toFixed(2), kind: 'size (per frame)', phase: phaseAt(b.t) });
      if (/tourCoin|tourPiece/.test(cls) && a.op > .8 && b.op < .25 && tr[i + 1] && tr[i + 1].op > .8) jumps.push({ id, cls, at: b.t, kind: 'flicker', phase: phaseAt(b.t) });
    }
  }
  // the hand-off: where the carried thing (the coin, then each piece) appears against where it was a frame before
  const carried = F.map(f => { const rows = f.n.filter(r => /tourCoin|tourPiece/.test(clsOf(r[0])) && r[5] > .35); return { t: f.t, rows }; });
  const handoffs = [];
  const seen = new Set();
  for (let i = 1; i < carried.length; i++) for (const r of carried[i].rows) {
    if (seen.has(r[0])) continue; seen.add(r[0]);
    if (clsOf(r[0]) !== 'tourPiece') continue;
    const prevCoin = carried[i - 1].rows.find(x => clsOf(x[0]) === 'tourCoin') || carried.slice(0, i).reverse().map(c => c.rows.find(x => clsOf(x[0]) === 'tourCoin')).find(Boolean);
    if (!prevCoin) continue;
    const dist = Math.hypot(r[1] - prevCoin[1], r[2] - prevCoin[2]), ratio = Math.sqrt(r[3] * r[4]) / Math.max(1, Math.sqrt(prevCoin[3] * prevCoin[4]));
    handoffs.push({ at: carried[i].t, px: R(dist), size: +ratio.toFixed(2), phase: phaseAt(carried[i].t) });
    if (dist > 24 || ratio > 1.35 || ratio < .65) jumps.push({ id: r[0], cls: 'tourPiece', at: carried[i].t, px: R(dist), ratio: +ratio.toFixed(2), kind: 'hand-off from the coin', phase: phaseAt(carried[i].t) });
  }
  // the spotlit card: a snap (not a glide)
  for (let i = 1; i < F.length; i++) { const a = F[i - 1].fc, b = F[i].fc; if (!a || !b || F[i].t - F[i - 1].t > 60) continue; const step = Math.hypot(b[0] - a[0], b[1] - a[1]); if (step > 60) jumps.push({ cls: 'the spotlit sheet card', at: F[i].t, px: R(step), kind: 'snap', phase: phaseAt(F[i].t) }); }
  out.metrics.jumps = jumps; out.metrics.handoffs = handoffs;
  // the screencast's biggest one-frame changes (filled in after the strip is read: visual)
  // speed: the coin and the pieces, px/s, over 3 frames
  let top = 0, topAt = 0; const moves = [];
  for (const [id, tr] of tracks) { if (!/tourCoin|tourPiece/.test(clsOf(id))) continue;
    for (let i = 3; i < tr.length; i++) { const dt = tr[i].t - tr[i - 3].t; if (dt <= 0 || dt > 90) continue; const v = Math.hypot(tr[i].x - tr[i - 3].x, tr[i].y - tr[i - 3].y) / dt * 1000; if (v > top) { top = v; topAt = tr[i].t; } }
    // each move: a run of frames in motion (> 30 px/s), its length in ms and px
    // (a move ends once it has stood still, under 30 px/s, for 60 ms)
    let m0 = null, px = 0, still = 0, a0 = null, last = null;
    const done = () => { moves.push({ cls: clsOf(id), from: R(m0), ms: R(last.t - m0), px: R(px), phase: phaseAt(m0), a: [R(a0.x), R(a0.y)], b: [R(last.x), R(last.y)] }); m0 = null; };
    for (let i = 1; i < tr.length; i++) {
      const step = Math.hypot(tr[i].x - tr[i - 1].x, tr[i].y - tr[i - 1].y), v = step / Math.max(1, tr[i].t - tr[i - 1].t) * 1000;
      if (v > 30) { if (m0 == null) { m0 = tr[i - 1].t; px = 0; a0 = tr[i - 1]; } px += step; still = 0; last = tr[i]; }
      else if (m0 != null) { still += tr[i].t - tr[i - 1].t; if (still >= 60) done(); }
    }
    if (m0 != null) done();
  }
  out.metrics.speed = { topPxS: R(top), at: R(topAt), phase: phaseAt(topAt) }; out.metrics.moves = moves.filter(m => m.px > 20).sort((a, b) => a.from - b.from);
  // the path the eye follows (the coin, then each piece in turn): how often it stops, and how often it turns back
  { const mv = out.metrics.moves, stops = [], turns = [];
    for (let i = 1; i < mv.length; i++) {
      const p = mv[i - 1], q = mv[i], gap = q.from - (p.from + p.ms);
      if (gap >= 80) stops.push({ at: R(p.from + p.ms), ms: R(gap), phase: phaseAt(p.from + p.ms) });
      const u = [p.b[0] - p.a[0], p.b[1] - p.a[1]], w = [q.b[0] - q.a[0], q.b[1] - q.a[1]], nu = Math.hypot(...u), nw = Math.hypot(...w);
      // (the tour's own shape is one arc: up into the Nest tab, then down onto the sheet from where it stopped there. That
      // turn, its apex at the tab, is marked: the pacing check lets one of it through, and every other turn counts)
      if (nu > 20 && nw > 20) { const ang = Math.acos(Math.max(-1, Math.min(1, (u[0] * w[0] + u[1] * w[1]) / nu / nw))) * 180 / Math.PI;
        const apex = u[1] < -20 && w[1] > 20 && Math.hypot(q.a[0] - p.b[0], q.a[1] - p.b[1]) < 40;
        if (ang > 100) turns.push({ at: q.from, deg: R(ang), phase: phaseAt(q.from), apex, from: p.cls + ' ' + p.a.join(',') + '→' + p.b.join(','), to: q.cls + ' →' + q.b.join(',') }); }
    }
    out.metrics.path = { moves: mv.length, stops, turns }; }
  const bigJumps = jumps.filter(j => j.kind !== 'too fast to follow');
  if (tourOn) check('jumps', bigJumps.length ? 'FAIL' : jumps.length ? 'WARN' : 'PASS', bigJumps.length || jumps.length ? jumps.slice(0, 6).map(j => `${j.cls} ${j.kind}${j.px != null ? ' ' + j.px + ' px' : ''}${j.dt != null ? ' in ' + j.dt + ' ms' : ''}${j.ratio != null ? ' ×' + j.ratio : ''}${j.around != null ? ' (around it ' + j.around + ' px/frame)' : ''} at ${j.phase}`).join('; ') + (jumps.length > 6 ? ` … ${jumps.length} in all` : '') : `none; coin→piece hand-offs ${handoffs.map(h => h.px + ' px ×' + h.size).join(', ') || 'none'}`);
  // ── one carrier: a second copy of the card flying with the coin (the list's own leave animation), a "+1" of its own ──
  if (tourOn) {
    const extra = [];
    for (const [id, tr] of tracks) {
      const cls = clsOf(id); if (!/mGhost|mPlus/.test(cls)) continue;
      const inTour = tr.filter(p => p.t >= tStart - 300 && p.t <= tEnd && p.op > .2); if (!inTour.length) continue;
      const far = Math.hypot(inTour[inTour.length - 1].x - inTour[0].x, inTour[inTour.length - 1].y - inTour[0].y);
      if (cls === 'mPlus') extra.push(`"${(info[id] || {}).text}" pops up at ${R(inTour[0].x)},${R(inTour[0].y)} (${phaseAt(inTour[0].t)})`);
      else if (far > 100) extra.push(`a copy of the card flies ${R(far)} px from ${R(inTour[0].x)},${R(inTour[0].y)} to ${R(inTour[inTour.length - 1].x)},${R(inTour[inTour.length - 1].y)} (${phaseAt(inTour[0].t)}–${phaseAt(inTour[inTour.length - 1].t)})`);
    }
    // the pressed card itself, still in the list and still seen, sliding away while the coin is in the air
    const seenCard = f => f.cd && f.cd[4] > .2 && f.cd[1] - f.cd[3] / 2 < res.vp.height - 30 && f.cd[1] + f.cd[3] / 2 > 30;   // (its box still inside the window)
    const cdF = F.filter(f => f.t >= tStart - 200 && f.t <= tEnd && seenCard(f));
    if (cdF.length > 1) {
      let far = 0, a = cdF[0], b = cdF[0];
      for (const f of cdF) { const d = Math.hypot(f.cd[0] - cdF[0].cd[0], f.cd[1] - cdF[0].cd[1]); if (d > far) { far = d; b = f; } }
      if (far > 120) extra.push(`the card itself slides ${R(far)} px down the list, in sight, while the coin is in the air (${R(a.cd[0])},${R(a.cd[1])} → ${R(b.cd[0])},${R(b.cd[1])}, ${phaseAt(a.t)}–${phaseAt(b.t)}), under the tour's lifted copy of it`);
    }
    out.metrics.copies = extra;
    check('copies', extra.length ? 'FAIL' : 'PASS', extra.length ? 'besides the coin: ' + extra.join('; ') : 'the coin is the only thing flying');
  }
  // ── covered ──
  const cov = [], offs = [];
  let seenFrames = 0;
  for (const f of F) { if (!f.hit) continue; for (const [id, who] of f.hit) { seenFrames++; if (who) (who.startsWith('nothing') ? offs : cov).push({ t: f.t, id, cls: clsOf(id), who }); } }
  const byWho = {}; for (const c of cov.concat(offs)) byWho[c.who] = (byWho[c.who] || 0) + 1;
  const coveredMs = (() => { const ts = [...new Set(cov.concat(offs).map(c => c.t))].sort((a, b) => a - b); let ms = 0; for (let i = 0; i < ts.length; i++) { const j = F.findIndex(f => f.t === ts[i]); if (j > 0) ms += Math.min(50, F[j].t - F[j - 1].t); } return R(ms); })();
  out.metrics.covered = { frames: cov.length + offs.length, of: seenFrames, ms: coveredMs, byWho, first: cov[0] ? phaseAt(cov[0].t) : null };
  // (a window over the tour is the pop-up's hand-off; anything else, a caption, a note or the window's edge, is the flight's)
  const byWin = cov.filter(c => /dialog#/.test(c.who)), rest = cov.filter(c => !/dialog#/.test(c.who)).concat(offs);
  const say = list => { const w = {}; for (const c of list) w[c.who] = (w[c.who] || 0) + 1; return Object.entries(w).sort((a, b) => b[1] - a[1]).slice(0, 4).map(([k, n]) => `${k} ×${n}`).join('; '); };
  if (tourOn) check('window', byWin.length > 2 ? 'FAIL' : byWin.length ? 'WARN' : 'PASS', byWin.length ? `the coin or a piece is behind an open window in ${byWin.length} of ${seenFrames} sightings (${Math.round(byWin.length / Math.max(1, seenFrames) * 100)}%): ${say(byWin)}; first at ${phaseAt(byWin[0].t)}` : 'no window over the tour');
  if (tourOn) check('covered', rest.length > 2 ? 'FAIL' : rest.length ? 'WARN' : 'PASS', rest.length ? `the coin or a piece is hidden at its centre, or off screen, in ${rest.length} of ${seenFrames} sightings: ${say(rest)}; first at ${phaseAt(rest[0].t)}` : `always seen when no window is over it (${seenFrames} sightings)`);
  // ── captions ──
  const skipAct2 = !!(sc.act && /esc|skip/.test(sc.act.kind) && res.actAt != null);
  const caps = [...tracks].filter(([id]) => /tourCap|tourPlus|nfCap|mNote|mPlus|toast/.test(clsOf(id)));
  const capList = caps.map(([id, tr]) => {
    const vis = tr.filter(p => p.op >= .6); let ms = 0; for (let i = 1; i < tr.length; i++) if (tr[i].op >= .6 && tr[i - 1].op >= .6) ms += Math.min(60, tr[i].t - tr[i - 1].t);
    const off = tr.some(p => p.op > .5 && (p.x - p.w / 2 < -1 || p.x + p.w / 2 > res.vp.width + 1 || p.y - p.h / 2 < -1 || p.y + p.h / 2 > res.vp.height + 1));
    // (one still showing when the recording stops, a note home that stays several seconds, is not cut short)
    const still = tr[tr.length - 1].t >= F[F.length - 1].t - 120 && tr[tr.length - 1].op >= .6;
    return { id, cls: clsOf(id), text: (info[id] || {}).text, from: vis[0] ? R(vis[0].t) : null, readableMs: R(ms), still, off, phase: vis[0] ? phaseAt(vis[0].t) : '' };
  }).filter(c => c.from != null || c.readableMs > 0);
  const overlaps = [];
  for (const f of F) {
    const vis = f.n.filter(r => /tourCap|tourPlus|nfCap|mNote|mPlus|toast/.test(clsOf(r[0])) && r[5] > .5), coin = f.n.find(r => /tourCoin|tourPiece/.test(clsOf(r[0])) && r[5] > .5);
    const box = r => [r[1] - r[3] / 2, r[2] - r[4] / 2, r[1] + r[3] / 2, r[2] + r[4] / 2];
    const inter = (a, b) => { const A = box(a), B = box(b), w = Math.min(A[2], B[2]) - Math.max(A[0], B[0]), h = Math.min(A[3], B[3]) - Math.max(A[1], B[1]); return w > 0 && h > 0 ? w * h / Math.min(a[3] * a[4], b[3] * b[4]) : 0; };
    for (let i = 0; i < vis.length; i++) for (let j = i + 1; j < vis.length; j++) { const o = inter(vis[i], vis[j]); if (o > .08) overlaps.push({ t: f.t, a: clsOf(vis[i][0]) + ' "' + (info[vis[i][0]].text || '').slice(0, 40) + '"', b: clsOf(vis[j][0]) + ' "' + (info[vis[j][0]].text || '').slice(0, 40) + '"', o: +o.toFixed(2) }); }
    if (coin) for (const c of vis) { const o = inter(c, coin); if (o > .15 && !/mNote/.test(clsOf(c[0]))) overlaps.push({ t: f.t, a: clsOf(c[0]) + ' "' + (info[c[0]].text || '').slice(0, 40) + '"', b: 'the ' + clsOf(coin[0]).replace('tour', '').toLowerCase(), o: +o.toFixed(2) }); }
  }
  const ovPairs = {}; for (const o of overlaps) { const k = o.a + ' over ' + o.b; ovPairs[k] = ovPairs[k] || { n: 0, first: o.t, max: 0 }; ovPairs[k].n++; ovPairs[k].max = Math.max(ovPairs[k].max, o.o); }
  const cutBySkip = c => skipAct2 && (tracks.get(c.id) || []).some(p => p.t >= res.actAt - 60 && p.op >= .6);
  const brief = capList.filter(c => c.readableMs < 1200 && !c.still && !cutBySkip(c) && !/mPlus/.test(c.cls));
  out.metrics.captions = { list: capList, overlaps: ovPairs, brief };
  if (tourOn || capList.length) check('captions', Object.keys(ovPairs).length || brief.length ? 'FAIL' : capList.some(c => c.off) ? 'WARN' : 'PASS',
    [Object.keys(ovPairs).length ? 'overlapping: ' + Object.entries(ovPairs).slice(0, 4).map(([k, v]) => `${k} (${v.n} frames, up to ${R(v.max * 100)}%, at ${phaseAt(v.first)})`).join('; ') : '',
      brief.length ? 'too brief to read: ' + brief.slice(0, 5).map(c => `"${(c.text || '').slice(0, 48)}" ${c.readableMs} ms`).join('; ') : '',
      capList.some(c => c.off) ? 'partly off screen: ' + capList.filter(c => c.off).map(c => `"${(c.text || '').slice(0, 40)}"`).join('; ') : ''].filter(Boolean).join(' · ') || `${capList.length} captions, each readable ≥ 1.2 s: ` + capList.map(c => `"${(c.text || '').slice(0, 30)}" ${c.readableMs} ms`).join('; '));
  // ── tab switches ──
  const tabs = [], actT = res.actAt != null ? res.actAt : 1e9, skipAct = sc.act && /esc|skip/.test(sc.act.kind);
  for (let i = 1; i < F.length; i++) {
    if (sc.act && sc.act.kind === 'resize' && F[i].t >= actT - 20 && F[i].t <= actT + 400) continue;
    const a = F[i - 1], b = F[i];
    for (const [k, name] of [[0, 'Review'], [1, 'Nest']]) { const up = b.v[k] - a.v[k]; if (Math.abs(up) > .45) tabs.push({ t: b.t, kind: up > 0 ? `the ${name} tab pops in` : `the ${name} tab vanishes`, from: a.v[k], to: b.v[k], phase: phaseAt(b.t) }); }
    if (a.bx && b.bx && Math.abs(a.bx[0] - b.bx[0]) + Math.abs(a.bx[1] - b.bx[1]) > 8) tabs.push({ t: b.t, kind: `the page's layout moves (view box ${a.bx.join(',')} → ${b.bx.join(',')})`, phase: phaseAt(b.t) });
    if (b.pend && b.v[1] > .15) tabs.push({ t: b.t, kind: `the Nest tab is shown at ${R(b.v[1] * 100)}% with ${b.pend} sheet${b.pend > 1 ? 's' : ''} not drawn yet`, phase: phaseAt(b.t) });
  }
  // a crossfade keeps one view at least half there; an empty (or half-empty) screen between them is a cut, not a crossfade
  const dips = []; let dip = null;
  for (let i = 1; i < F.length; i++) {
    const vis = Math.max(F[i].v[0], F[i].v[1]);
    if (vis < .5 && !F[i].dl && !(skipAct && F[i].t >= actT)) { if (!dip) dip = { t: F[i - 1].t, ms: 0, empty: 0, low: 1 }; dip.ms += F[i].t - F[i - 1].t; if (vis < .08) dip.empty += F[i].t - F[i - 1].t; dip.low = Math.min(dip.low, vis); }
    else if (dip) { dips.push(dip); dip = null; }
  }
  if (dip) dips.push(dip);
  out.metrics.tabDips = dips.map(d => ({ at: phaseAt(d.t), ms: R(d.ms), empty: R(d.empty), low: +d.low.toFixed(2) }));
  for (const d of dips) if (d.empty > 100 || d.ms > 220) tabs.push({ t: d.t, kind: d.empty > 100 ? `both tabs hidden for ${R(d.empty)} ms (an empty screen; under half there for ${R(d.ms)} ms)` : `both tabs under half there for ${R(d.ms)} ms (at ${R(d.low * 100)}% at most)`, phase: phaseAt(d.t) });
  out.metrics.tabs = tabs;
  const tabKinds = {}; for (const x of tabs) { const k = x.kind.replace(/\d+%/, 'n%').replace(/ \d+ sheets?/, ' n sheets').replace(/\(view box.*\)/, '').replace(/^(both tabs .*?) for \d+ ms.*/, '$1'); tabKinds[k] = tabKinds[k] || { n: 0, at: x.phase, ms: '' }; tabKinds[k].n++; const m = /for (\d+ ms.*)$/.exec(x.kind); if (m) tabKinds[k].ms += (tabKinds[k].ms ? '; ' : '') + m[1]; }
  const hard = tabs.filter(x => /pops|vanishes|layout|empty|under half/.test(x.kind) && !(skipAct && x.t != null && x.t >= actT));
  if (tourOn) check('tabs', hard.length ? 'FAIL' : tabs.length ? 'WARN' : 'PASS', tabs.length ? Object.entries(tabKinds).map(([k, v]) => `${k} (${v.n}×, first at ${v.at}${v.ms ? ', ' + v.ms : ''})`).join('; ') : 'crossfades only, the Nest tab drawn before it shows');
  // ── home ──
  const b0 = res.before, b1 = res.after, diffs = [];
  const expect = Object.assign({}, b0);
  if (sc.from === 'designs') expect.designs = false, expect.dialogs = '';   // (sent: its designs window goes back into its card)
  for (const k of ['mode', 'chip', 'cseg', 'designs', 'dialogs']) if (String(expect[k]) !== String(b1[k])) diffs.push(`${k}: ${JSON.stringify(expect[k])} → ${JSON.stringify(b1[k])}`);
  // (after a resize the list is laid out anew: its scroll is not comparable; the card having left, the list may be too
  // short for the same scroll: the view slides by that much, a warning)
  const resized = sc.act && sc.act.kind === 'resize', clamped = b1.maxScroll >= 0 && b0.scroll > b1.maxScroll + 2 && Math.abs(b1.scroll - b1.maxScroll) <= 2;
  const soft = [];
  if (Math.abs(b0.scroll - b1.scroll) > 2) (resized ? soft : clamped ? soft : diffs).push(`Review scroll: ${b0.scroll} → ${b1.scroll}` + (resized ? ' (after the resize)' : clamped ? ` (the list is ${b0.maxScroll - b1.maxScroll} px shorter without the card, so the view slides by ${b0.scroll - b1.scroll} px: keep the cards above it where they were)` : ''));
  if (JSON.stringify(expect.orderWin) !== JSON.stringify(b1.orderWin)) diffs.push(`order window: ${JSON.stringify(expect.orderWin)} → ${JSON.stringify(b1.orderWin)}`);
  const pagesMoved = b0.nestPages !== b1.nestPages;
  out.metrics.home = { before: b0, after: b1, diffs, pagesMoved };
  if (soft.length && !diffs.length) { check('home', 'WARN', soft.join('; ')); } else
  check('home', diffs.length ? 'FAIL' : pagesMoved ? 'WARN' : 'PASS', diffs.length ? diffs.concat(soft).join('; ') : pagesMoved ? `home as left, but the Nest cards show other sheets than before (${b0.nestPages} → ${b1.nestPages})` : `home as left (${b1.mode}${b1.chip ? ' · ' + b1.chip : ''}, scroll ${b1.scroll}${b1.orderWin ? ', order window on ' + b1.orderWin.view : ''})`);
  // ── origin: the coin rises from the card that was pressed, where it stood, with the list still under the user ──
  const c0 = rec.card0;
  if (tourOn && !sc.reduced && c0 && sc.from !== 'orderwin') {
    const firstOf = re => [...tracks].filter(([id]) => re.test(clsOf(id))).map(([, tr]) => tr.find(p => p.op > .35)).filter(Boolean).sort((a, b) => a.t - b.t)[0];
    const coin = firstOf(/^tourCoin$/), snapT = firstOf(/^tourCard$/);
    const [bl, bt, bw, bh] = c0.box, cy0 = bt + bh / 2;
    const outside = p => R(Math.hypot(Math.max(bl - p.x, 0, p.x - (bl + bw)), Math.max(bt - p.y, 0, p.y - (bt + bh))));
    // (the second before the coin rises: with a sheet nested first, the harness itself holds the tour, which is not the page's doing)
    const tCoin = coin ? coin.t : tStart, pre = F.filter(f => f.t <= tCoin && f.t >= tCoin - 1500);
    // the list under the user before the coin rises: its scroll, and the pressed card's own place
    const scShift = c0.scroll >= 0 ? Math.max(0, ...pre.map(f => f.sc[0] >= 0 ? Math.abs(f.sc[0] - c0.scroll) : 0)) : 0;
    let cardShift = 0, cardShiftAt = null; for (const f of pre) if (f.cd && Math.abs(f.cd[1] - cy0) > cardShift) { cardShift = R(Math.abs(f.cd[1] - cy0)); cardShiftAt = f.t; }
    // the card gone (neither it nor the tour's lifted copy of it seen) before the coin is there
    let seenAt = null; for (const f of pre) if ((f.cd && f.cd[4] > .35) || f.n.some(r => clsOf(r[0]) === 'tourCard' && r[5] > .35)) seenAt = f.t;
    const blank = coin && seenAt != null ? R(coin.t - seenAt) : null;
    const o = { coinFrom: coin ? [R(coin.x), R(coin.y)] : null, card: c0.box, coinOutside: coin ? outside(coin) : null, snapOutside: snapT ? outside({ x: snapT.x, y: snapT.y }) : null, listScroll: R(scShift), cardShift, cardShiftAt: cardShiftAt != null ? phaseAt(cardShiftAt) : null, blankMs: blank, firstMotionMs: coin ? R(coin.t) : null };
    out.metrics.origin = o;
    const bad = [], soft = [];
    if (!coin) bad.push('no coin seen');
    else if (o.coinOutside > 40) bad.push(`the coin first shows at ${o.coinFrom.join(',')}, ${o.coinOutside} px outside the card that was pressed (${R(bl)},${R(bt)} ${R(bw)}×${R(bh)})${coin.y > res.vp.height - 20 || coin.y < 20 ? ', at the edge of the window' : ''}`);
    const moved = Math.max(cardShift, scShift) > 40 ? `before the lift the list moves under the user: the card ${cardShift} px${cardShiftAt != null ? ' (at ' + phaseAt(cardShiftAt) + ')' : ''}, the list's scroll ${R(scShift)} px` : '';
    if (moved) (Math.max(cardShift, scShift) > 120 ? bad : soft).push(moved);
    if (blank != null && blank > 150) bad.push(`the card is gone ${blank} ms before the coin shows (nothing where the order was)`);
    if (coin && coin.t > 700 && !sc.placed) soft.push(`nothing moves for ${R(coin.t)} ms after the press (the button's busy words only)`);
    check('origin', bad.length ? 'FAIL' : soft.length ? 'WARN' : 'PASS', bad.concat(soft).join('; ') || `the coin rises from the pressed card (${o.coinOutside} px outside it), ${R(coin.t)} ms after the press, the list still (${cardShift} px)`);
  }
  // ── leftovers ──
  const L = res.left, lo = [];
  if (L.layer) lo.push(`${L.layer} tour node(s) left`); if (L.lit) lo.push(`${L.lit} spotlight piece(s) left`); if (L.dim.length) lo.push('the Nest tab still dimmed (' + L.dim.join(',') + ')'); if (L.nfOpen) lo.push('NestFocus still open');
  if (L.views) lo.push(`${L.views} view animation(s) running`); if (L.inert.length) lo.push('a view left faded: ' + L.inert.join(',')); if (res.sheets1.hidden.length) lo.push('pieces still held back on ' + res.sheets1.hidden.join(','));
  check('leftovers', lo.length ? 'FAIL' : 'PASS', lo.length ? lo.join('; ') : 'nothing left over');
  // ── pacing ──
  // (reduced motion: nothing flies by design; its gentle version is judged under reduced, below)
  if (tourOn && !sc.reduced) {
    const legs = (res.sheets1.mine || []).reduce((s, m) => { m.where.forEach(w => s.add(w)); return s; }, new Set()).size || 1;
    const shortMoves = out.metrics.moves.filter(m => m.ms < 280 && m.px > 60);
    const want = [3000 + (legs - 1) * 1200, 9000 + (legs - 1) * 2500];
    const bad = [];
    if (ph.total < want[0] && !(sc.act && /esc|skip/.test(sc.act.kind))) bad.push(`total ${ph.total} ms is under ${want[0]} ms for ${legs} sheet${legs > 1 ? 's' : ''}: rushed`);
    if (ph.total > want[1]) bad.push(`total ${ph.total} ms is over ${want[1]} ms`);
    // (the eye follows a moving thing smoothly up to about 30-40 degrees a second: at a desk, 1,500-2,000 px/s)
    if (top > 2200) bad.push(`the coin reaches ${R(top)} px/s (at ${out.metrics.speed.phase}): too fast for the eye to follow`);
    const pth = out.metrics.path;
    // (every piece on a sheet is seen landing unless the user skipped: fewer landings is a tour cut short, its watchdog)
    const onSheets = (res.sheets1.mine || []).filter(m => m.where.length).length;
    if (!sc.act && lands.length < onSheets) bad.push(`only ${lands.length} of ${onSheets} pieces seen landing: the tour was cut short (its watchdog fires after 6000 + legs × 3200 ms)`);
    // (one up-then-down arc through the Nest tab is the tour's shape: allowed once; any other turn back is not)
    const arcs = pth.turns.filter(x => x.apex), backs = pth.turns.filter(x => !x.apex).concat(arcs.slice(1));
    if (backs.length) bad.push(`the path turns back on itself ${backs.length}× (${backs.slice(0, 3).map(x => `${x.deg}° at ${x.phase}`).join(', ')})`);
    if (pth.stops.length > 2 + legs) bad.push(`${pth.moves} separate moves with ${pth.stops.length} stops between them: a chain of hops, not one flight`);
    if (shortMoves.length) bad.push(`${shortMoves.length} move(s) under 280 ms: ` + shortMoves.slice(0, 3).map(m => `${m.cls} ${m.px} px in ${m.ms} ms at ${m.phase}`).join(', '));
    if (ph.liftToNest != null && ph.liftToNest < 1000) bad.push(`from the lift to the Nest tab in ${ph.liftToNest} ms: where it started is barely seen`);
    check('pacing', bad.length ? 'FAIL' : 'PASS', `press→start ${ph.pressToStart} ms, lift→Nest ${ph.liftToNest ?? '-'} ms, Nest→first landing ${ph.nestToFirstLanding ?? '-'} ms${ph.landingGaps ? ', landings ' + ph.landingGaps.join('/') + ' ms apart' : ''}, last landing→home ${ph.lastLandingToHome ?? '-'} ms, home→end ${ph.homeSwitchToEnd ?? '-'} ms, total ${ph.total} ms; top speed ${R(top)} px/s; ${out.metrics.moves.length} moves, ${out.metrics.path.stops.length} stops (${out.metrics.path.stops.map(x => x.ms).join('/')} ms), ${backs.length} turn-backs${arcs.length ? ' (and the one arc up into the Nest tab and down onto the sheet)' : ''}` + (bad.length ? ' — ' + bad.join('; ') : ''));
  } else if (!sc.reduced) check('pacing', 'FAIL', 'no tour played' + (res.notes.length ? ' (' + res.notes.join('; ') + ')' : ''));
  // ── landing: each piece where its sheet has it ──
  const landed = [];
  for (const [id, tr] of tracks) {
    if (clsOf(id) !== 'tourPiece') continue;
    // (a flight a skip cut short is finished at once, not flown: only those that ended before the skip are judged)
    if (sc.act && /esc|skip/.test(sc.act.kind) && res.actAt != null && tr[tr.length - 1].t >= res.actAt - 20) continue;
    const solid = tr.filter(p => p.op > .9); const last = solid[solid.length - 1] || tr[tr.length - 1];
    const spots = (last.f.sp || []).filter(s => s.x != null);
    if (!spots.length) { landed.push({ id, at: last.t, err: 'no spot of the order on screen', kinds: (last.f.sp || []).map(s => s.kind).join(',') }); continue; }
    const best = spots.map(s => ({ s, d: Math.hypot(s.x - last.x, s.y - last.y) })).sort((a, b) => a.d - b.d)[0];
    const size = Math.sqrt(last.w * last.h) / Math.max(1, Math.sqrt(best.s.w * best.s.h));
    const off = last.x < 0 || last.y < 0 || last.x > res.vp.width || last.y > res.vp.height;
    landed.push({ id, at: R(last.t), px: +best.d.toFixed(1), kind: best.s.kind, metal: best.s.metal, page: best.s.page, size: +size.toFixed(2), off, where: [R(last.x), R(last.y)], phase: phaseAt(last.t) });
  }
  // (pieces sent but never seen landing: the order's pieces on the sheets against the pieces flown)
  const nPieces = (res.sheets1.mine || []).length;
  out.metrics.landing = { landed, nPieces };
  if (tourOn && !sc.reduced) {   // (reduced motion: nothing flies, so nothing lands; judged under reduced)
    const far = landed.filter(l => l.err || l.off || l.px > 6 || (l.kind === 'placed' && (l.size > 1.6 || l.size < .6)));
    const skipped = sc.act && /esc|skip/.test(sc.act.kind);
    check('landing', far.length ? 'FAIL' : !skipped && res.sheets0 && landed.length < nPieces && (res.sheets1.rows || []).some(r => r.state === 'pooled') ? 'WARN' : 'PASS',
      `${landed.length} piece flight(s) for ${nPieces} piece(s) sent: ` + landed.map(l => l.err ? l.err : `${l.px} px from its ${l.kind === 'placed' ? 'place on ' + l.metal + ' sheet ' + l.page : l.kind === 'queue' ? 'thumbnail in the ' + l.metal + ' queue' : l.kind}${l.kind === 'placed' ? ', size ×' + l.size : ''}${l.off ? ` — OFF SCREEN at ${l.where.join(',')} in a ${res.vp.width}×${res.vp.height} window` : ''}`).join('; '));
  }
  // ── integrity ──
  const S1 = res.sheets1, bad = [];
  if (S1.dup.length) bad.push('a piece on the sheets twice: ' + S1.dup.join(', '));
  if (S1.placeDup.length) bad.push('a placement twice: ' + S1.placeDup.join(', '));
  if (S1.mine.some(m => m.n > 1)) bad.push('this order duplicated: ' + S1.mine.filter(m => m.n > 1).map(m => m.id + ' on ' + m.where.join('+')).join(', '));
  const r0 = JSON.stringify(res.sheets0.rose), r1_ = JSON.stringify(S1.rose);
  if (r0 !== r1_) bad.push(`Rose Gold lines changed: ${r0} → ${r1_}`);
  if (res.rose.length) bad.push('Rose Gold cloud calls during the send and tour: ' + res.rose.join(', '));
  const expectPieces = sc.designs.reduce((a, dd2) => a + (dd2.qty || 1), 0);
  if (S1.mine.length && S1.mine.length !== expectPieces) bad.push(`${S1.mine.length} pieces of the order on the sheets, ${expectPieces} sent`);
  if (sc.dbl && rec.marks.filter(m => m[0] === 'start').length > 1) bad.push(`${rec.marks.filter(m => m[0] === 'start').length} tours played for one double-click`);
  check('integrity', bad.length ? 'FAIL' : 'PASS', bad.length ? bad.join('; ') : `each of the ${S1.mine.length} piece(s) once (${[...new Set(S1.mine.flatMap(m => m.where))].join(', ') || 'none on a sheet'}); rows ${S1.rows.map(r => r.state).join(',')}; no Rose Gold line or cut`);
  // ── motion: only transform, opacity, clip-path ──
  const props = [...new Set(rec.anims.filter(a => tStart == null || a.t >= tStart - 5).flatMap(a => a.props))], layout = props.filter(p => LAYOUT.test(p)), other = props.filter(p => !['transform', 'opacity', 'clipPath', 'offsetDistance'].includes(p) && !LAYOUT.test(p));
  check('motion', layout.length ? 'FAIL' : other.length ? 'WARN' : 'PASS', `animated: ${props.join(', ') || 'nothing'}` + (layout.length ? ` (layout: ${layout.join(', ')}, by ${[...new Set(rec.anims.filter(a => a.props.some(p => LAYOUT.test(p))).map(a => a.cls))].join(', ')})` : '') + (other.length ? ` (paint: ${other.join(', ')}, by ${[...new Set(rec.anims.filter(a => a.props.some(p => other.includes(p))).map(a => a.cls))].slice(0, 4).join(', ')})` : ''));
  // ── reduced motion: the gentle version (Paul, 29 Sep 01:30: the same story, calmer): nothing flies or glides; the
  // same captions, over short fades, name each sheet and what arrived; the tabs dissolve (judged under tabs); home, a
  // note says where the order went; and it takes about as long as the full tour's captions need to be read ──
  if (sc.reduced) {
    const notes = capList.filter(c => /mNote|toast|mPlus/.test(c.cls)), said = capList.filter(c => c.cls === 'tourCap');
    const flown = Object.keys(info).filter(id => /^tour(Coin|Piece|Card)$|nfPiece/.test(info[id].cls));
    const named = said.filter(c => /(Placed|Queued) on Sheet \d|· waiting/.test(c.text || ''));
    const bad = [];
    if (!tourOn) bad.push('no tour: its gentle version did not play');
    if (flown.length || out.metrics.moves.length) bad.push(`${flown.length} thing(s) flying (${[...new Set(flown.map(id => info[id].cls))].join(', ')}), ${out.metrics.moves.length} move(s)`);
    if (tourOn && sw[0] == null) bad.push('the Nest tab never shown');
    if (!named.length) bad.push('no caption names the sheet');
    if (!notes.length) bad.push('home, nothing says where the order went');
    if (tourOn && (ph.total < 3000 || ph.total > 9000)) bad.push(`${ph.total} ms: ${ph.total < 3000 ? 'too short to read' : 'too long'}`);
    check('reduced', bad.length ? 'FAIL' : 'PASS', (bad.length ? bad.join('; ') + ' · ' : '') + `${tourOn ? 'the gentle version' : 'no tour'}: ${said.map(c => '"' + (c.text || '').slice(0, 34) + '" ' + c.readableMs + ' ms').join('; ') || 'no captions'}; nothing flying (${flown.length}); ${tourOn ? ph.total + ' ms; ' : ''}note: ${notes.map(n => '"' + (n.text || '').slice(0, 60) + '"').join('; ') || 'none'}`);
  }
  // ── skip / Esc: home at once, everything shown ──
  if (sc.act && /esc|skip/.test(sc.act.kind)) {
    const after = tEnd != null && res.actAt != null ? tEnd - res.actAt : null;
    check('skip', after != null && after < 1300 && !res.sheets1.hidden.length ? 'PASS' : 'FAIL', after == null ? 'the ' + sc.act.kind + ' did not end the tour' + (res.notes.length ? ' (' + res.notes.join('; ') + ')' : '') : `home ${after} ms after the ${sc.act.kind} (at ${phaseAt(res.actAt)})` + (res.notes.length ? '; ' + res.notes.join('; ') : ''));
  }
  const errs = res.errors.concat(rec.errs || []);
  check('errors', errs.length ? 'FAIL' : 'PASS', errs.length ? errs.slice(0, 3).join(' | ') : 'no page errors');
  if (res.how !== 'button') out.checks.push({ id: 'press', status: 'WARN', text: 'pressed by ' + res.how });
  return out;
}

/* ── the strip: frames of the screencast, and each frame's change from the one before ── */
async function stripOf(browser, res, an) {
  if (!res.shots || !res.shots.length) return null;
  const page = await browser.newPage({ viewport: { width: 1500, height: 600 } });
  try {
    await page.setContent('<body style="margin:0"></body>');
    // each frame against the one before it: the mean change of a small grey copy (0-1), and the gap in time
    const diffs = await page.evaluate(async shots => {
      // (the share of the screen that changes clearly in one frame: a pixel whose grey moves by more than 0.1; a
      // crossfade moves each pixel a little per frame, a cut moves many a lot)
      const W = 180, H = 120, cv = document.createElement('canvas'); cv.width = W; cv.height = H; const ctx = cv.getContext('2d', { willReadFrequently: true }); let prev = null; const out = [];
      for (const s of shots) {
        const im = new Image(); im.src = 'data:image/jpeg;base64,' + s.data; await im.decode().catch(() => {});
        ctx.drawImage(im, 0, 0, W, H); const d = ctx.getImageData(0, 0, W, H).data, g = new Float32Array(W * H);
        for (let i = 0; i < g.length; i++) g[i] = (d[i * 4] * .3 + d[i * 4 + 1] * .59 + d[i * 4 + 2] * .11) / 255;
        let m = 0; if (prev) { for (let i = 0; i < g.length; i++) if (Math.abs(g[i] - prev[i]) > .1) m++; m /= g.length; }
        out.push(+m.toFixed(4)); prev = g;
      }
      return out;
    }, res.shots.map(s => ({ data: s.data })));
    res.shots.forEach((s, i) => { s.diff = diffs[i]; s.gap = i ? s.t - res.shots[i - 1].t : 0; });
    const marks = an.metrics.marks || [];
    const phaseAt = t => { const ms = marks.filter(m => m[1] <= t); return ms.length ? ms[ms.length - 1][0] + ' +' + R(t - ms[ms.length - 1][1]) : 'press +' + R(t); };
    const cuts = res.shots.filter((s, i) => i && s.t >= 0 && s.diff > .06).map(s => ({ t: s.t, diff: s.diff, gap: R(s.gap), phase: phaseAt(s.t) })).sort((a, b) => b.diff - a.diff);
    an.metrics.visualCuts = cuts.slice(0, 8);
    an.metrics.screencast = { frames: res.shots.length, topDiffs: res.shots.map(s => [s.t, s.diff]).sort((a, b) => b[1] - a[1]).slice(0, 5), decoded: diffs.filter(x => x > 0).length };
    // the frames shown: every ~ (tour / 14) ms, the marks, and the worst moments (jumps, cuts, the coin hidden)
    const end = (marks.find(m => m[0] === 'end') || [0, res.shots[res.shots.length - 1].t])[1] + 900;
    const want = new Map();
    const add = (t, label) => { if (t == null) return; const s = res.shots.reduce((b, x) => x.t <= t + 8 && (!b || x.t > b.t) ? x : b, null) || res.shots[0]; if (!want.has(s)) want.set(s, label); else if (label && !/^\d/.test(label)) want.set(s, (want.get(s) + ' · ' + label).replace(/^ · /, '')); };
    const n = 16; for (let i = 0; i <= n; i++) add(R(end * i / n), '');
    for (const m of marks) add(m[1] + 40, m[0]);
    for (const j of (an.metrics.jumps || []).slice(0, 3)) add(j.at, 'JUMP: ' + j.cls + ' ' + j.kind);
    for (const c of cuts.slice(0, 3)) add(c.t, `CUT ${R(c.diff * 100)}%`);
    const cov = an.checks.find(c => c.id === 'covered'); if (cov && cov.status !== 'PASS') { const f = res.rec.frames.find(ff => ff.hit && ff.hit.some(h => h[1])); if (f) add(f.t, 'COVERED'); }
    const pick = [...want].sort((a, b) => a[0].t - b[0].t).slice(0, 28);
    const esc = s => String(s).replace(/[&<>"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' })[c]);
    const status = an.checks.filter(c => c.status === 'FAIL').map(c => c.id).join(', ') || 'no failures';
    const html = `<body style="margin:0;background:#1b1916;color:#eee;font:12px system-ui"><div style="padding:8px 12px;font:600 15px system-ui">${esc(res.name)} · ${esc(res.title)} · ${esc(an.metrics.phases.pressToHome != null ? an.metrics.phases.pressToHome + ' ms press → home' : 'no tour')} · FAIL: ${esc(status)}</div><div style="display:grid;grid-template-columns:repeat(4, 360px);gap:6px;padding:0 8px 8px">` +
      pick.map(([s, label]) => `<figure style="margin:0"><img style="width:360px;display:block;border:${/JUMP|CUT|COVERED/.test(label) ? '2px solid #e0584a' : '2px solid transparent'}" src="data:image/jpeg;base64,${s.data}"><figcaption style="padding:2px 0 4px">${s.t} ms · ${esc(phaseAt(s.t))}${label ? ' · <b>' + esc(label) + '</b>' : ''}</figcaption></figure>`).join('') + '</div></body>';
    await page.setContent(html); await page.waitForTimeout(150);
    const file = path.join(OUT, `${res.name}.png`);
    await page.screenshot({ path: file, fullPage: true });
    return file;
  } finally { await page.close(); }
}

/* ── the findings: every failed check grouped across starting points, ranked, in plain words, with a fix and an owner ── */
const KB = {
  copies: { w: 10, owner: () => 'pacing', title: 'Two things fly at once: a copy of the card goes to the Nest tab beside the coin',
    sees: 'As the coin lifts off, a second, faded copy of the whole card also slides away towards the Nest tab (the Review list\'s own "card left" animation), and a "+1" pops at the tab. Elsewhere the card itself slides down and out of the list under the tour\'s own lifted copy of it at the same time. Either way two things move at once and the eye does not know which one to follow.',
    fix: 'Two separate causes, one rule: while the tour carries an order, nothing else about that order moves. (a) The tour\'s snap() marks the card `_mLeaving` so Motion.reconcile makes no ghost of its own, but CustomSheet.send redraws the list (`say()` → `redraw()`, the busy spinner) before the line leaves, so the node that leaves is a new one without the mark: carry the mark by the card\'s mkey rather than on the node (a set of mkeys the tour owns, checked in reconcile\'s `before` loop), or make Review\'s `leave()` return `null` for a line the tour is carrying. (b) The card itself is still in the list and slides 450–540 px down it as the rows below close up, under the tour\'s lifted copy: hold the list still until the coin has gone (keep the row in place, or its height, until `switch:nest`), then let it close up while the Review tab is out of sight.' },
  window: { w: 12, owner: () => 'pop-up', title: 'The whole tour plays behind the order window',
    sees: 'Pressed from the order window, nothing of the tour is seen: the window stays open over the page while the coin flies, the tabs switch and the piece lands behind it. It "looks broken". A double-click on a card\'s Send to Sheet does the same: the second click lands on the card (the button has turned into a spinner) and opens the order window over the tour, which is still open at the end.',
    fix: 'The order window is a modal `<dialog>` in the browser\'s top layer, above `#tourLayer` whatever its z-index. Before the lift, close it into its card with its own close animation (OrderWin.shut, back into what it grew from), wait for that, then lift the coin from the card. When home, reopen it on the same order and tab, with its typed text. For the double-click, ignore clicks on a card while its send is busy or its tour plays (the card\'s onclick in wireAct/reviewRow). Alternatively, move the tour layer into the top layer (a `popover="manual"`, shown after the dialog so it sits above it), but the tab switches would still happen behind the window: closing it is the clear hand-off.' },
  origin: { w: 9, owner: r => r.every(x => /^(designs-window)/.test(x.sc)) ? 'pop-up' : 'pacing', title: 'The coin does not rise from the card that was pressed',
    sees: 'After the press, the list moves under the user (the card is redrawn busy, or leaves and the cards below close up), and the coin then rises from somewhere else on the screen, sometimes at its very edge, rather than out of the card the user just pressed. Where the order started is lost. (In the two runs that nest a sheet before the tour — placed, resize-mid — the long wait before the lift is the harness\'s own doing; what counts there is that the card has already left the list by the time the coin rises.)',
    fix: 'Take the lift-off point once, at the press, from the card as the user saw it: call SendTour.snap(card) in CustomSheet.send before its first `say()` redraw ("Reading…"), not after the reading, saving and `changed()`. Keep that lifted copy on the tour layer where the card was, lift the coin from it, and let the list close the gap only after the coin has left, with its scroll held so the cards above stay put. The lift can start at once (the reading and placing finish while the coin rises), so the press is answered straight away.' },
  covered: { w: 8, owner: () => 'pacing', title: 'The coin is hidden, covered by a caption or off screen while it flies',
    sees: 'For part of the flight the design is covered by the tour\'s own caption, or it comes down outside the window (a sheet card lower on the page, its queue below the fold), so the user cannot see where it went.',
    fix: 'Keep captions off the coin\'s path: place the "Order N" caption after the coin has risen, on the side away from where it will fly, or attach it to the coin with an offset that never overlaps it. Bring the landing spot into view before the piece comes down: NestFocus.open glides the card so its sheet preview shows, but a queued piece lands on its thumbnail in the card\'s queue, which can be below the window (the RG and 10K cards in the second row). Glide so that the queue (or the spot) is inside the window, and land there only once it is. `#tourLayer` (z-index 150) is also below `.mNote` (305), `.toasts` (300) and `.cuTip` (160), so a note or toast can cover the coin.' },
  home: { w: 8, owner: r => r.some(x => /order window|orderWin|dialogs/.test(x.text)) ? 'pop-up' : 'pacing', title: 'It does not come back to where the user began',
    sees: 'After the tour, the screen is not what the user left: a different tab or filter, another scroll position, or the order window (and what was typed in it) gone.',
    fix: 'Record the whole view before the lift: tab, Review filter, Open or Completed, the list scroll, whether the order window is open (with its order, tab and typed text) and the Nest cards\' sheets. Put all of it back as the last beat, with the order window growing back out of its card. `goHome` restores only the Review scroll today.' },
  integrity: { w: 12, owner: () => 'other', title: 'The send or the tour changed the sheets wrongly',
    sees: 'A piece is on the sheets twice, a Rose Gold green line or cut appears, or the number of pieces on the sheets is not the number sent.',
    fix: 'The placement is CustomSheet.send / Review.repool. The tour only shows it, so check that repool is not run twice (for example on a double-click, or on a line that is already pooled). A Rose Gold line should come only from Cut Sheet. These are the "duplicate charm" and "RG green line" fixes now under way; this flags when the tour path triggers them.' },
  jumps: { w: 9, owner: r => r.some(x => /^(designs-window|orderwin)/.test(x.sc)) ? 'pacing (the window hand-offs: pop-up)' : 'pacing', title: 'The motion jumps',
    sees: 'Something the eye is following leaps in a single frame instead of gliding: the coin, a piece at the hand-off from the coin, a caption, or the sheet card being spotlit.',
    fix: 'Keep one carrier on screen from the lift to the landing: the piece should leave from the coin\'s exact centre and size, with no new node appearing elsewhere. Do not re-aim a flight that is already running. Only start a move once its target has stopped (NestFocus.open glides the card for 620 ms while the coin flies to where the card was). When a flight has to change direction, blend from the current position with a composite animation, never jump to a recomputed start.' },
  landing: { w: 7, owner: r => r.some(x => /resize/.test(x.sc)) ? 'other' : 'pacing', title: 'A piece does not come down where the sheet has it',
    sees: 'The flying piece settles a little off its place (or off its thumbnail in the queue), and then the real piece pops in somewhere else.',
    fix: 'Read each spot as late as possible: just before its own flight starts, not when the sheet opens. Recompute the spot after a resize or a scroll (listen for resize while the tour plays). A placed piece should land at the rect from NestFocus.spotOf; a queued piece at its queue thumbnail.' },
  tabs: { w: 10, owner: () => 'pacing', title: 'The switch between the Review and Nest tabs pops or flashes',
    sees: 'Going to the Nest tab, the Review list fades out completely and the screen stands empty (plain cream, only the coin and the top bar) for about a quarter of a second before the sheets fade in; coming home, the Nest tab vanishes or the Review list pops in at full strength in one frame. It reads as a cut between two scenes, not one continuous move.',
    fix: 'Draw the Nest cards (drawPreviewNow for each active page) and settle the layout before the Nest view becomes visible: setMode("nest") draws them one animation frame later, and it toggles `#app.nestMode`, which re-lays out the rail. Then crossfade both views at once (the old fading out while the new fades in, 300–400 ms), instead of fading out, cutting, then fading in.' },
  captions: { w: 8, owner: () => 'pacing', title: 'Captions overlap or go by too fast to read',
    sees: 'Two labels stand over each other, a label sits over the coin, or a label vanishes before it can be read.',
    fix: 'Show one caption at a time, in one place, for at least 1.2 s (about 1.6–2 s for a sentence). Let NestFocus\'s own `.nfCap` and the tour\'s "+N on …" note take turns rather than show together. Keep captions clear of the coin\'s path, and hold the "Order N" caption until the coin has left the card.' },
  pacing: { w: 9, owner: () => 'pacing', title: 'Rushed: too fast, or too many short moves',
    sees: 'The tour hurries: the coin darts, several short hops follow each other, and there is too little time to see where the order started and where it went.',
    fix: 'Use fewer, longer moves: a lift of about 0.6 s with a 0.4 s hold, one arc of 0.9–1.1 s to the sheet (not three hops: to the tab, to the card, over the sheet), a 0.5 s beat on the sheet before the piece comes down, and a 0.6 s rest after the landing. Cap the speed at about 1,500 px/s. Recompute the watchdog (6000 + legs × 3200 ms today) from the new phase lengths, or a slower tour will be cut short.' },
  frames: { w: 5, owner: () => 'other', title: 'Dropped frames',
    sees: 'The motion stutters: a frame takes longer than 50 ms (three frames\' worth), or a long task freezes the animation for a moment — most often at the press, at the tab switch, and as the sheet opens.',
    fix: 'Do nothing heavy while something moves: finish the list re-render, the Nest previews and NestFocus\'s full picture (fullOf) before the first move, or in idle gaps between beats. Tab switches re-lay out the whole page (the rail): keep them in a still moment.' },
  leftovers: { w: 7, owner: () => 'pacing', title: 'Something is left behind after the tour',
    sees: 'After the tour ends, a spotlight, a dimmed Nest tab, a hidden piece or a stray node stays on screen.',
    fix: 'In the finally block of play(), wait for NestFocus.close() to finish and remove every node. Reveal everything held back (`_tourHide`), and cancel both views\' animations.' },
  skip: { w: 6, owner: () => 'pacing', title: 'Skip or Esc does not bring the user home at once',
    sees: 'Pressing Esc or clicking during the tour does not end it straight away, or leaves pieces hidden.',
    fix: 'forward() should finish every animation, close NestFocus with everything shown, and go home within about 300 ms.' },
  reduced: { w: 6, owner: () => 'other', title: 'Reduced motion still animates, or says nothing',
    sees: 'With reduced motion on, something still flies or glides, or the gentle version does not show and say where the order went.',
    fix: 'Under prefers-reduced-motion, play the gentle version: nothing flies or glides; the same captions over short fades ("Order N", then "Placed on Sheet 1 · GF"), the Nest tab shown with the sheet in the light, then home with the note on the card (with Show). About 3–9 s in all.' },
  motion: { w: 3, owner: () => 'pacing', title: 'A property other than transform, opacity or clip-path is animated',
    sees: 'Nothing directly, but these animations run on the main thread and are the first to stutter.', fix: 'Animate only transform, opacity and clip-path.' },
  errors: { w: 9, owner: () => 'other', title: 'Page errors during the tour', sees: 'Something failed during the tour.', fix: 'See the error text.' },
  ran: { w: 9, owner: () => 'other', title: 'A starting point could not be played', sees: 'The harness could not play the tour from here.', fix: 'See the error text.' },
  press: { w: 4, owner: () => 'pop-up', title: 'The order window has no Send to Sheet of its own', sees: 'From the order window there is no Send to Sheet button; the harness pressed it by calling CustomSheet.send() while the window was open, as a button there would.', fix: 'Add a Send to Sheet button to the order window (its custom bar, `#owCustom`, beside Print QR label and Complete Order) that closes the window into its card and plays the tour.' },
  consistency: { w: 6, owner: () => 'pacing', title: 'The same send takes a different time depending on where it starts', sees: 'Sending one GF piece takes noticeably longer or shorter from one place than from another, so the tour does not feel like the same thing each time.', fix: 'Make the tour\'s own phases fixed. Any hand-off beforehand (closing the designs window or the order window, 480 ms today) should be one fixed beat before `tour:start`, never overlapping the lift.' }
};
function findings(all, sameGroups, load) {
  const groups = new Map();
  for (const r of all) for (const c of r.an.checks) {
    if (c.status === 'PASS') continue;
    const g = groups.get(c.id) || { id: c.id, rows: [] }; g.rows.push({ sc: r.name, title: r.title, status: c.status, text: c.text, strip: r.strip, data: c.data }); groups.set(c.id, g);
  }
  for (const g of sameGroups) if (g.status !== 'PASS') groups.set('consistency', { id: 'consistency', rows: [{ sc: g.names.join(', '), title: 'one GF piece, from each place', status: g.status, text: g.text, strip: null }] });
  const list = [...groups.values()].map(g => {
    const kb = KB[g.id] || { w: 3, owner: () => 'other', title: g.id, sees: '', fix: '' };
    const fails = g.rows.filter(r => r.status === 'FAIL').length, warns = g.rows.length - fails;
    return Object.assign(g, { kb, owner: kb.owner(g.rows), score: (fails ? kb.w * 10 : kb.w * 4) + Math.min(fails, 10) + .3 * warns });   // (how bad first, how widespread second)
  }).sort((a, b) => b.score - a.score);
  const L = [];
  L.push('# Send to Sheet tour: adversarial findings', '');
  L.push(`Generated by \`tests/charm-nest/tour-adversarial.cjs\` at ${new Date().toISOString()} (load per core ${load.toFixed(2)}${STRIP ? ', screencast on' : ''}), ${all.length} starting points. Ranked worst first. Owners: **pacing** (the flight's middle, path, captions, timings), **pop-up** (the hand-off in and out of the order window), **other**. Each starting point's frame strip is \`<name>.png\` in this folder (red frames: a jump, a cut or the coin covered), and its last frame is \`<name>-end.png\`.`, '');
  L.push('Rerun after the fixes: `NODE_PATH=$(npm root -g) PW_DIR=$(npm root -g)/playwright/node_modules CHROMIUM=$(ls -d /opt/pw-browsers/chromium-*/chrome-linux/chrome|head -1) SHOTS=<dir> node tests/charm-nest/tour-adversarial.cjs` (ONLY=name,name for a few; STRIP=0 without the screencast; RAW=1 to keep every frame\'s data; REPORT=1 to write this file again from results.json without playing). Scratch runs: point SHOTS outside /mnt/project-files.', '');
  L.push('## Summary', '', '| # | finding | owner | where (FAIL / WARN) |', '|---|---|---|---|');
  list.forEach((g, i) => L.push(`| ${i + 1} | ${g.kb.title} | ${g.owner} | ${g.rows.filter(r => r.status === 'FAIL').map(r => r.sc).join(', ') || '-'} / ${g.rows.filter(r => r.status !== 'FAIL').map(r => r.sc).join(', ') || '-'} |`));
  L.push('');
  list.forEach((g, i) => {
    L.push(`## ${i + 1}. ${g.kb.title} (owner: ${g.owner})`, '');
    L.push(`**What the user sees:** ${g.kb.sees}`, '');
    L.push('**Where, and the numbers:**', '');
    for (const r of g.rows) L.push(`- ${r.status} · **${r.sc}** (${r.title}): ${r.text}${r.strip ? ` · strip: \`${path.basename(r.strip)}\`` : ''}`);
    L.push('', `**Suggested fix:** ${g.kb.fix}`, '');
  });
  // one tour, beat by beat: the first starting point that sends one GF piece from its card
  const one = all.find(r => r.name === 'review-custom' && r.an.metrics.phases && r.an.metrics.phases.total != null) || all.find(r => r.an.metrics.phases && r.an.metrics.phases.total != null);
  if (one) {
    const m = one.an.metrics;
    L.push(`## Anatomy of one tour (${one.name}: ${one.title})`, '');
    L.push('What moves, when, how far and how long (ms from the press). A stop is the coin standing still between two moves; a turn-back is a move that goes back the way it came.', '');
    L.push('| from | what | px | ms | at |', '|---|---|---|---|---|');
    for (const mv of m.moves) L.push(`| ${mv.from} | ${mv.cls === 'tourCoin' ? 'the coin' : 'a piece'} ${mv.a.join(',')} → ${mv.b.join(',')} | ${mv.px} | ${mv.ms} | ${mv.phase} |`);
    L.push('', `Stops: ${m.path.stops.map(x => `${x.ms} ms at ${x.phase}`).join(', ') || 'none'}. Turn-backs: ${m.path.turns.map(x => `${x.deg}° at ${x.phase}`).join(', ') || 'none'}. Top speed ${m.speed.topPxS} px/s at ${m.speed.phase}.`, '');
    L.push('Captions (readable = at 60% opacity or more):', '');
    for (const c of m.captions.list) L.push(`- ${c.cls} "${c.text}": from ${c.from} ms (${c.phase}), readable ${c.readableMs} ms${c.still ? ' (still showing when the recording stopped)' : ''}`);
    L.push('', `Marks: ${m.marks.map(x => `${x[0]} ${x[1]}`).join(' · ')}.`, '');
  }
  L.push('## Every starting point', '', '| starting point | press→home | tour | lift→Nest | Nest→landing | landing→home | worst frame | FAIL |', '|---|---|---|---|---|---|---|---|');
  for (const r of all) { const p = r.an.metrics.phases || {}, f = r.an.metrics.frames || {}; L.push(`| ${r.name} | ${p.pressToHome ?? '-'} | ${p.total ?? '-'} | ${p.liftToNest ?? '-'} | ${p.nestToFirstLanding ?? '-'} | ${p.lastLandingToHome ?? '-'} | ${f.worst ?? '-'} | ${r.an.checks.filter(c => c.status === 'FAIL').map(c => c.id).join(', ') || '-'} |`); }
  L.push('');
  return L.join('\n');
}

if (process.env.REPORT === '1') {
  // (the findings written again from the last results.json, without playing anything)
  const j = JSON.parse(fs.readFileSync(path.join(OUT, 'results.json'), 'utf8'));
  const all = j.runs.map(r => ({ name: r.name, title: r.title, sc: SCENARIOS.find(x => x.name === r.name) || {}, an: { checks: r.checks, metrics: r.metrics }, strip: r.strip }));
  fs.writeFileSync(path.join(OUT, 'findings.md'), findings(all, j.consistency || [], j.load || 0));
  console.log('findings written again: ' + path.join(OUT, 'findings.md'));
  process.exit(0);
}
(async () => {
  const load = os.loadavg()[0] / os.cpus().length;
  fs.mkdirSync(OUT, { recursive: true });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const all = [];
  const list = SCENARIOS.filter(s => !ONLY.length || ONLY.includes(s.name));
  console.log(`tour-adversarial: ${list.length} starting points, load per core ${load.toFixed(2)}${load > 1.2 ? ' (busy: frame times are pessimistic)' : ''}${STRIP ? ', screencast on' : ''}`);
  try {
    for (const sc of list) {
      const t = Date.now();
      const res = await run(browser, sc);
      res.an = analyse(res);
      try { res.strip = await stripOf(browser, res, res.an); } catch (e) { res.notes.push('strip: ' + e.message); }
      const cuts = res.an.metrics.visualCuts || [];
      if (cuts.length) { const big = cuts.filter(c => c.diff > .12 && c.gap < 70); res.an.checks.push({ id: 'jumps', status: big.length ? 'FAIL' : 'WARN', text: `screencast: the screen changes by ${cuts.slice(0, 4).map(c => `${R(c.diff * 100)}% in one frame (${c.gap} ms) at ${c.phase}`).join('; ')}` }); }
      all.push(res);
      if (process.env.RAW === '1' && res.rec) fs.writeFileSync(path.join(OUT, `raw-${sc.name}.json`), JSON.stringify({ rec: res.rec, before: res.before, after: res.after, sheets0: res.sheets0, sheets1: res.sheets1 }));
      console.log(`\n${sc.name} · ${sc.title} (${Math.round((Date.now() - t) / 1000)} s${res.nested ? ', nested in ' + res.nested.map(n => n.ms + ' ms ' + n.pages.join(' ')).join('; ') : ''})`);
      for (const c of res.an.checks) console.log(`  ${c.status.padEnd(4)} ${sc.name} · ${c.id}: ${c.text}`);
      for (const n of res.notes) console.log(`  note ${n}`);
    }
  } finally { await browser.close(); }
  // the same order sent from each place: the same tour
  const sameGroups = [];
  for (const key of [...new Set(all.map(r => r.sc.same).filter(Boolean))]) {
    const rs = all.filter(r => r.sc.same === key && r.an.metrics.phases && r.an.metrics.phases.total != null);
    if (rs.length < 2) continue;
    const spread = k => { const v = rs.map(r => r.an.metrics.phases[k]).filter(x => x != null); return v.length > 1 ? Math.max(...v) - Math.min(...v) : 0; };
    const parts = ['pressToHome', 'total', 'pressToStart', 'liftToNest', 'nestToFirstLanding', 'lastLandingToHome'].map(k => [k, spread(k)]);
    const bad = parts.filter(([k, v]) => v > (k === 'pressToHome' || k === 'pressToStart' ? 700 : 400));
    const text = parts.map(([k, v]) => `${k} spread ${v} ms`).join(', ') + ' — ' + rs.map(r => `${r.name} ${r.an.metrics.phases.pressToHome}/${r.an.metrics.phases.total} ms`).join(', ');
    sameGroups.push({ key, names: rs.map(r => r.name), status: bad.length ? 'FAIL' : 'PASS', text });
    console.log(`\n  ${(bad.length ? 'FAIL' : 'PASS').padEnd(4)} consistency (${key}): ${text}`);
  }
  // one line per check, over every starting point
  const ids = [...new Set(all.flatMap(r => r.an.checks.map(c => c.id)))];
  console.log('\nsummary');
  for (const id of ids) {
    const rows = all.map(r => [r.name, r.an.checks.filter(c => c.id === id)]).filter(([, cs]) => cs.length);
    const fail = rows.filter(([, cs]) => cs.some(c => c.status === 'FAIL')).map(([n]) => n), warn = rows.filter(([, cs]) => !cs.some(c => c.status === 'FAIL') && cs.some(c => c.status === 'WARN')).map(([n]) => n);
    console.log(`  ${(fail.length ? 'FAIL' : warn.length ? 'WARN' : 'PASS').padEnd(4)} ${id}: ${rows.length - fail.length - warn.length}/${rows.length} pass${fail.length ? ' · FAIL ' + fail.join(', ') : ''}${warn.length ? ' · WARN ' + warn.join(', ') : ''}`);
  }
  const slim = all.map(r => ({ name: r.name, title: r.title, how: r.how, notes: r.notes, checks: r.an.checks, metrics: r.an.metrics, before: r.before, after: r.after, strip: r.strip, nested: r.nested }));
  fs.writeFileSync(path.join(OUT, 'results.json'), JSON.stringify({ at: new Date().toISOString(), load, consistency: sameGroups, runs: slim }, null, 1));
  if (!ONLY.length || process.env.FINDINGS === '1') fs.writeFileSync(path.join(OUT, 'findings.md'), findings(all, sameGroups, load));
  else fs.writeFileSync(path.join(OUT, 'findings-partial.md'), findings(all, sameGroups, load));
  const failed = all.some(r => r.an.checks.some(c => c.status === 'FAIL')) || sameGroups.some(g => g.status === 'FAIL');
  console.log(`\n${failed ? 'FAILED' : 'all passed'} · findings: ${path.join(OUT, ONLY.length && process.env.FINDINGS !== '1' ? 'findings-partial.md' : 'findings.md')}`);
  process.exit(failed ? 1 : 0);
})().catch(e => { console.error(e); process.exit(2); });
