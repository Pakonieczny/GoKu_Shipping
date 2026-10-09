/* Charm Nest · piece dots (window.PieceDots): the one dot component for every list that shows an order's pieces as small circles.
   Paul, 5 Oct 2026, round 15: "The colours of the hollow circles representing pieces that have not yet been placed on a sheet should be
   showing the same colour as the sheet they belong to. This should happen to all sheets, and the users should be able to hover over each
   one of those dots filled in and hollow, and have a small pop-up show up to show the thumbnail of the vector design for that particular
   piece. This needs to be instantaneous with no delay and very fast and ready to load on hover state." · "Also clicking on any of the dots
   both solid and hollow will automatically open the detailed order view with that particular piece pre-selected."

   What a dot is. One per piece of an order. Filled: the piece sits on a sheet, in that sheet's colour. Hollow ring: it is not placed on a
   sheet yet (or something else still holds it), and the ring takes the colour of the sheet it belongs to. The colour is always one of the
   sheet tokens the cards and tags use (--m-gold, --m-silver, --m-rose, --m-gold10k, --m-gold14k): a piece of a known metal has its
   metal's colour; a piece of no known metal (an unknown SKU, no station read) takes the colour of the sheet whose list the order is shown
   under (html's sheetMetal); in a list that is about no one sheet it keeps a calm neutral ring. A ring is never the issue colour.

   html(dots, o) -> the markup of one group of dots:
       dots: [{ ring, metal, pool, line, n, none, state, kind }]  (ring: not placed · metal: a metal key or its sheet code · pool: the piece's
              pool id · line: its line's key · n: its number in the order · none: known to have no vector design · state: a few plain words
              for the label · kind: why it is a ring)
       o:    { order, sheetMetal, total }
   warm(scope, { clip })    prefetches the thumbnails of the dots drawn in `scope`, the visible ones first (idle slices), into a bounded LRU
   hide() / shown() / current() / stats()

   The hover card. ONE floating element for every dot (made once, moved into the open dialog's layer when the dot is inside one, so it shows
   over the issues panel and the shared-orders modal), pointer-events none: it takes no press. It is shown synchronously in the pointer's own
   event (no timer, no hover delay, no fade-in: only a 3 px settle in 80 ms that never delays the first paint), above the dot (below it at the
   top edge), inside the screen and under the top bar. Moving from dot to dot swaps the picture in the same card with no gap. It hides at once
   on leave, blur, Esc, a scroll and a press; on touch a press shows it and letting go hides it (the tap itself opens the order).
   What it shows is the VECTOR DESIGN of that piece: the very picture the order window shows as "Vector design" (ListMedia.vectorThumb, handed to this page's
   scripts as window.PieceMedia.vectorThumb / vectorKey: the same
   renderer and master-preview cache as vectorInto), never a second renderer. A piece with no design says one calm line at once.

   Ready before the hover. warm() resolves every dot to its design (pieces of one design share one thumbnail, ListMedia.vectorKey), and renders
   the missing ones in requestIdleCallback slices (two at a time), the visible dots first and the rest after, into an LRU of about 300 small
   ready-to-paint pictures (a decoded <img> kept with its data address: painted by moving the node, no decode on hover). A list that has gone from
   the page stops its queue. A dot hovered before its picture is ready jumps the queue, shows a small labelled spinner ("Loading design") and swaps
   to the picture the moment it is there; it never waits longer than its own render. Nothing is fetched here that ListMedia would not fetch for the
   order window, nothing is written, nothing is recorded, and Etsy is never asked.

   A press (a click, Enter or Space on a focused dot, a tap) opens the detailed order window with THAT piece selected in the piece switcher:
   the dot says exactly which piece it is (its pool id, its line's key, its number). The host may take the press first: it hears the cancelable
   event "piecedot:open" ({ order, pool, line, n, ring, dot }) on the dot, and a host that opens windows over a pop-up (the issues panel) hands
   over as it does for a row. A press nobody takes opens the window itself (openOrderFrom, else OrderWin.openOrder) with { poolId, row, pick }.

   Delegation: the page has ONE set of listeners on the document (hover, focus, press, keys) and none on a dot. A redraw of the list that
   replaces the hovered dot finds its successor by key and keeps the card (the pointer's place is read with elementFromPoint), so a hovered dot
   keeps its card through the Library's 3 s redraws. The arrow keys move between the dots of a group (they are focusable but not Tab stops: a
   list of sixty orders is not three hundred stops; a host decides how the keyboard gets into a row). Everything is feature-detected: without
   ListMedia, OrderPieces, Orders or OrderWin the dots still draw and the card says its calm line. */
(function (root) {
  'use strict';
  const doc = root.document;
  if (!doc || root.PieceDots) return;

  const CAP = 300, CONC = 2, GAP = 9, Z = 2147483200, SETTLE_MS = 80;
  const METALS = ['gold', 'silver', 'rose', 'gold10k', 'gold14k'];
  const BY_CODE = { GF: 'gold', SS: 'silver', RG: 'rose', '10K': 'gold10k', '14K': 'gold14k' };
  const NO_DESIGN = 'No vector design for this piece yet', FAILED = 'Design could not be loaded';
  const clamp = (n, a, b) => Math.max(a, Math.min(b, n));
  const esc = t => String(t == null ? '' : t).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const reduced = () => { try { return root.Motion && root.Motion.reduced ? !!root.Motion.reduced() : !!(root.matchMedia && root.matchMedia('(prefers-reduced-motion: reduce)').matches); } catch (_) { return false; } };
  const warn = (what, e) => { try { console.warn('Piece dots: ' + what, e); } catch (_) {} };
  /** A metal key, or a sheet's code (GF, RG ...) as a metal key; '' when it is neither. */
  const metalKey = m => { const s = String(m || ''); return METALS.includes(s) ? s : BY_CODE[s.toUpperCase()] || ''; };
  const lineOf = pool => String(pool || '').replace(/_\d+$/, '');

  /* ═══ the dots (markup) ═══ */
  function css() {
    if (doc.getElementById('pdCss')) return;
    const s = doc.createElement('style'); s.id = 'pdCss';
    s.textContent = `.pdots{display:inline-flex;gap:3px;flex:none;vertical-align:middle}
.pdot{position:relative;flex:none;display:block;box-sizing:border-box;width:10px;height:10px;border-radius:50%;background:var(--mc,var(--ink25,#c4bdb0));cursor:pointer;outline:none}
.pdot::before{content:"";position:absolute;inset:-5px -1.5px}
.pdot.ring{background:transparent;border:2px solid var(--mc,var(--ink45,#938c80))}
.pdot.ring::before{inset:-7px -3.5px}
.pdots [data-m=gold]{--mc:var(--m-gold,#c8a24e)}.pdots [data-m=silver]{--mc:var(--m-silver,#8d95a0)}.pdots [data-m=rose]{--mc:var(--m-rose,#c08578)}.pdots [data-m=gold10k]{--mc:var(--m-gold10k,#b08d2a)}.pdots [data-m=gold14k]{--mc:var(--m-gold14k,#d9b545)}
.pdot:focus-visible{outline:2px solid var(--gold,#a9823f);outline-offset:2px}
.pdTip{position:fixed;z-index:${Z};left:0;top:0;box-sizing:border-box;width:max-content;max-width:min(236px,calc(100vw - 16px));padding:6px;border-radius:11px;background:var(--card,#fffefb);color:var(--ink70,#5b554c);border:1px solid var(--line,#e4ddd0);box-shadow:0 10px 26px rgba(30,26,20,.13),0 1px 3px rgba(30,26,20,.07);font:11px/1.4 var(--sans,system-ui,sans-serif);text-align:left;pointer-events:none;visibility:hidden;--ax:50%}
.pdTip[data-on]{visibility:visible}
.pdTip *{pointer-events:none}
.pdTip.text{padding:8px 11px 9px}
.pdBox{box-sizing:border-box;width:112px;height:112px;border-radius:7px;background:#fff;border:1px solid var(--line2,#efe9dd);display:grid;place-items:center;overflow:hidden}
.pdBox img{display:block;max-width:100%;max-height:100%;object-fit:contain}
.pdWait{display:grid;justify-items:center;gap:8px;font:10px/1.2 var(--sans,system-ui,sans-serif);color:var(--ink45,#938c80)}
.pdSpin{width:14px;height:14px;box-sizing:border-box;border:2px solid var(--line,#e4ddd0);border-top-color:var(--ink70,#5b554c);border-radius:50%;animation:pdSpin .7s linear infinite}
@keyframes pdSpin{to{transform:rotate(360deg)}}
.pdLine{display:block;overflow-wrap:anywhere}
.pdTip::after{content:"";position:absolute;left:var(--ax);bottom:-3.5px;width:7px;height:7px;margin-left:-3.5px;background:var(--card,#fffefb);border:1px solid var(--line,#e4ddd0);border-top:0;border-left:0;border-radius:0 0 2px 0;transform:rotate(45deg)}
.pdTip.below::after{bottom:auto;top:-3.5px;transform:rotate(225deg)}
@media (prefers-reduced-motion:reduce){.pdSpin{animation-duration:1.6s}}`;
    (doc.head || doc.documentElement).appendChild(s);
  }
  const waitingKinds = new Set(['pooled', 'noSku', 'unmatched', 'noDesign']);
  function dotHtml(d, o, i, order) {
    const m = metalKey(d && d.metal) || metalKey(o.sheetMetal), pool = String((d && d.pool) || ''), line = String((d && d.line) || lineOf(pool)), n = +(d && d.n) || 0;
    const key = `${order}|${pool || '#' + i}`, ring = !!(d && d.ring);
    // (an earring pair's piece says which ear it is: the card then shows that ear's own picture, turned over for the Right)
    const side = d && (d.side === 'L' || d.side === 'R') ? d.side : '', sideWord = side === 'L' ? 'Left' : side === 'R' ? 'Right' : '';
    const label = `Piece ${n || i + 1}${sideWord ? ', ' + sideWord.toLowerCase() : ''}${d && d.state ? ', ' + String(d.state).replace(/^./, c => c.toLowerCase()) : ''}`;
    return `<i class="pdot${ring ? ' ring' : ''}"${m ? ` data-m="${m}"` : ''} data-pk="${esc(key)}" data-pd-o="${esc(order)}" data-pd-p="${esc(pool)}" data-pd-l="${esc(line)}" data-pd-n="${n}"${side ? ` data-pd-s="${side}"${d.mirror ? ' data-pd-m="1"' : ''}` : ''}${d && d.none ? ' data-pd-none="1"' : ''} tabindex="-1" role="button" aria-label="${esc(label)}"></i>`;
  }
  function html(dots, o) {
    o = o || {}; css();
    const list = Array.isArray(dots) ? dots : [], order = String(o.order || ''), total = Math.max(list.length, +o.total || 0), hollow = list.filter(d => d && d.ring);
    const allUnplaced = hollow.every(d => !d.kind || waitingKinds.has(d.kind));
    const label = `${total} ${total === 1 ? 'piece' : 'pieces'}${hollow.length ? `, ${hollow.length} ${allUnplaced ? 'not on a sheet yet' : 'waiting'}` : ''}`;
    return `<span class="pdots" role="group" aria-label="${esc(label)}" data-pd-order="${esc(order)}">${list.map((d, i) => dotHtml(d, o, i, order)).join('')}</span>`;
  }

  /* ═══ what a dot is a picture of (cheap, synchronous: no read, no network) ═══ */
  let ri = { arr: null, n: -1, key: null, pool: null };
  function rowIndex() {
    let arr = null; try { arr = root.Orders && typeof root.Orders.rows === 'function' ? root.Orders.rows() : null; } catch (_) {}
    if (!Array.isArray(arr)) return null;
    if (ri.arr !== arr || ri.n !== arr.length) {
      const key = new Map(), pool = new Map();
      for (const r of arr) { if (!r) continue; if (r.key != null) key.set(String(r.key), r); for (const id of r.poolIds || []) pool.set(String(id), r); }
      ri = { arr, n: arr.length, key, pool };
    }
    return ri;
  }
  // the piece's order line: the pull's row; for an order outside the pull the piece as OrderPieces tells it (its SKU, and its size from the pool row)
  function rowFor(order, line, pool) {
    const ix = rowIndex(), r = ix && ((line && ix.key.get(line)) || (pool && ix.pool.get(pool))) || null;
    if (r) return r;
    const OP = root.OrderPieces; if (!OP || typeof OP.of !== 'function') return null;
    let ps = []; try { ps = OP.of(order) || []; } catch (_) {}
    const p = ps.find(x => x.key === pool) || ps.find(x => x.lineKey === line); if (!p || p.hand || p.noDesign) return null;
    let pr = null; try { pr = root.B && root.B.pool && root.B.pool.rows && root.B.pool.rows.get(p.key); } catch (_) {}
    return { key: p.lineKey, order: { receiptId: order }, line: { sku: p.sku, listingId: p.listingId }, spec: { designSku: p.sku, size: (pr && pr.size) || null, noDesign: false }, poolIds: [p.key] };
  }
  /** { none, key, row } of a dot: none when it is known to have no design; key = the design's identity (pieces of one design share a picture). */
  function resolve(dot) {
    if (dot._pd) return dot._pd;
    if (dot.getAttribute('data-pd-none') === '1') return { none: true, key: '', row: null };
    const o = dot.getAttribute('data-pd-o') || '', p = dot.getAttribute('data-pd-p') || '', l = dot.getAttribute('data-pd-l') || '';
    const row = rowFor(o, l, p), LM = root.PieceMedia || root.ListMedia;
    const sd = dot.getAttribute('data-pd-s'), opts = sd === 'L' || sd === 'R' ? { highlight: sd, side: sd, mirror: dot.getAttribute('data-pd-m') === '1' } : null;   // (an earring's own ear: no opts for any other piece, so nothing else changes)
    let key = '';
    if (row) { try { key = LM && typeof LM.vectorKey === 'function' ? (opts ? LM.vectorKey(row, opts) : LM.vectorKey(row)) || '' : (row.spec && row.spec.noDesign ? '' : 'sku:' + String((row.spec && row.spec.designSku) || (row.line && row.line.sku) || '').toUpperCase()); } catch (e) { warn('design key', e); } }
    const out = key && key !== 'sku:' ? { none: false, key, row, opts } : { none: true, key: '', row };
    if (!out.none) dot._pd = out;   // (a dot whose row is not there yet is looked at again next time)
    return out;
  }

  /* ═══ the cache: design key -> a small ready-to-paint picture, bounded (LRU) ═══ */
  const cache = new Map();   // key -> { state: 'busy' | 'ready' | 'none' | 'error', img, url, at }
  const counts = { started: 0, made: 0, none: 0, error: 0, hits: 0 };
  const put = (k, e) => { cache.delete(k); cache.set(k, e); while (cache.size > CAP) cache.delete(cache.keys().next().value); };
  const usable = e => e && (e.state !== 'error' || Date.now() - e.at > 30000);   // (a failed design is tried again after 30 s, never in a loop)
  let running = 0, queue = [], pumping = false;
  function load(key, row, opts) {
    const had = cache.get(key);
    if (had && !(had.state === 'error' && Date.now() - had.at > 30000)) { if (had.state !== 'busy') { cache.delete(key); cache.set(key, had); } return had; }
    const e = { state: 'busy', img: null, url: '', at: Date.now() };
    put(key, e); running++; counts.started++;
    const LM = root.PieceMedia || root.ListMedia;
    const task = Promise.resolve().then(() => {
      if (!LM || typeof LM.vectorThumb !== 'function') return null;
      return opts ? LM.vectorThumb(row, opts) : LM.vectorThumb(row);
    }).then(url => {
      if (!url) { e.state = 'none'; counts.none++; return; }
      const img = new root.Image(); img.alt = ''; img.decoding = 'async'; img.draggable = false; img.src = url;
      const done = typeof img.decode === 'function' ? img.decode().catch(() => { if (!(img.complete && img.naturalWidth)) throw new Error('picture unreadable'); }) : new Promise((res, rej) => { img.onload = res; img.onerror = () => rej(new Error('picture unreadable')); });
      return done.then(() => { e.img = img; e.url = url; e.state = 'ready'; counts.made++; });
    }).catch(err => { e.state = 'error'; e.at = Date.now(); counts.error++; warn('design', err); });
    task.then(() => { running--; settled(key); pump(); });
    return e;
  }
  const schedule = fn => { if (typeof root.requestIdleCallback === 'function') root.requestIdleCallback(fn, { timeout: 500 }); else setTimeout(() => fn(null), 16); };
  function pump() {
    if (pumping || !queue.length || running >= CONC) return;
    pumping = true;
    schedule(dl => {
      pumping = false;
      while (queue.length && running < CONC && (!dl || dl.didTimeout || dl.timeRemaining() > 2)) {
        const j = queue.shift();
        if (!j.scope.isConnected) continue;   // (its list has gone from the page: nobody will hover it)
        const had = cache.get(j.key); if (had && usable(had) && had.state !== 'error') continue;
        load(j.key, j.row, j.opts);
      }
      if (queue.length) pump();
    });
  }
  /** Prefetch the thumbnails of the dots drawn in `scope`: the visible dots first, the rest after, in idle slices. Returns how many designs were queued. */
  function warm(scope, o) {
    if (!scope || typeof scope.querySelectorAll !== 'function' || !scope.isConnected) return 0;
    const dots = scope.querySelectorAll('.pdot'); if (!dots.length) return 0;
    const seen = new Set(), vis = [], rest = [];
    let clip = null, vh = root.innerHeight || 800;
    for (const d of dots) {
      const x = resolve(d);
      if (x.none || !x.key || seen.has(x.key)) continue;
      const had = cache.get(x.key); if (had && !(had.state === 'error' && Date.now() - had.at > 30000)) continue;
      seen.add(x.key);
      if (!clip) { const c = (o && o.clip) || scope; clip = c.getBoundingClientRect ? c.getBoundingClientRect() : { top: 0, bottom: vh }; }
      const r = d.getBoundingClientRect();
      (r.height && r.bottom > Math.max(0, clip.top) && r.top < Math.min(vh, clip.bottom) ? vis : rest).push({ key: x.key, row: x.row, opts: x.opts, scope });
    }
    if (!vis.length && !rest.length) return 0;
    const keys = new Set(vis.concat(rest).map(j => j.key));
    queue = vis.concat(queue.filter(j => !keys.has(j.key) && j.scope.isConnected), rest);
    pump();
    return keys.size;
  }

  /* ═══ the one card ═══ */
  const st = { tip: null, box: null, wait: null, line: null, dot: null, key: '', how: '', x: 0, y: 0, on: false, mo: null, scope: null, anim: null };
  function make() {
    if (st.tip) return st.tip;
    css();
    const t = st.tip = doc.createElement('div'); t.className = 'pdTip'; t.setAttribute('aria-hidden', 'true');   // (the dot's own label says what it is)
    st.box = doc.createElement('span'); st.box.className = 'pdBox';
    st.wait = doc.createElement('span'); st.wait.className = 'pdWait';
    const sp = doc.createElement('i'); sp.className = 'pdSpin'; sp.setAttribute('role', 'status'); sp.setAttribute('aria-label', 'Loading design');
    const tx = doc.createElement('span'); tx.textContent = 'Loading design'; st.wait.append(sp, tx);
    st.line = doc.createElement('span'); st.line.className = 'pdLine';
    return t;
  }
  const dotOf = n => (n && n.nodeType === 1 && n.classList && n.classList.contains('pdot')) ? n : null;
  /** What the card holds for a dot now: the picture (ready), the spinner (being made), or one calm line. */
  function paint(info, e) {
    const t = st.tip;
    if (!info.none && e && e.state === 'ready') { st.box.replaceChildren(e.img); t.classList.remove('text'); t.replaceChildren(st.box); return; }
    if (!info.none && (!e || e.state === 'busy')) { st.box.replaceChildren(st.wait); t.classList.remove('text'); t.replaceChildren(st.box); return; }
    st.line.textContent = !info.none && e && e.state === 'error' ? FAILED : NO_DESIGN;
    t.classList.add('text'); t.replaceChildren(st.line);
  }
  function place(dot) {
    const t = st.tip, b = dot.getBoundingClientRect();
    const vw = root.innerWidth || 1200, vh = root.innerHeight || 800, bar = doc.querySelector('.topbar'), q = bar && bar.getClientRects().length ? bar.getBoundingClientRect() : null;
    const lo = Math.max(8, q && q.bottom < vh / 2 ? q.bottom + 8 : 0), hi = vh - 8;
    t.style.left = '0px'; t.style.top = '0px';
    const w = t.offsetWidth, h = t.offsetHeight, cx = (b.left + b.right) / 2;
    const above = b.top - GAP - lo, below = hi - (b.bottom + GAP), up = above >= h || (below < h && above >= below);
    const y = clamp(up ? b.top - GAP - h : b.bottom + GAP, lo, Math.max(lo, hi - h)), x = clamp(cx - w / 2, 8, Math.max(8, vw - w - 8));
    t.style.left = Math.round(x) + 'px'; t.style.top = Math.round(y) + 'px';
    t.style.setProperty('--ax', clamp(cx - x, 14, Math.max(14, w - 14)) + 'px'); t.classList.toggle('below', !up);
  }
  // the card goes into the open dialog the dot is in (a modal dialog is the top layer: a card on the body would be hidden behind it), else onto the body
  const layerOf = dot => dot.closest('dialog[open]') || doc.body || doc.documentElement;
  function show(dot, how, ev) {
    if (!dot || !dot.isConnected) return;
    const t = make(), info = resolve(dot);
    let e = null;
    if (!info.none) { e = cache.get(info.key) || null; if (e && e.state === 'error' && Date.now() - e.at > 30000) e = null; if (!e) e = load(info.key, info.row, info.opts); else if (e.state !== 'busy') { cache.delete(info.key); cache.set(info.key, e); counts.hits++; } }
    const layer = layerOf(dot); if (t.parentNode !== layer) layer.appendChild(t);
    const was = st.on;
    st.dot = dot; st.key = dot.getAttribute('data-pk') || ''; st.how = how;
    if (ev && ev.clientX != null) { st.x = ev.clientX; st.y = ev.clientY; }
    paint(info, e); place(dot);
    st.on = true; t.setAttribute('data-on', '');
    if (!was) {
      if (st.anim) { try { st.anim.cancel(); } catch (_) {} st.anim = null; }
      if (!reduced() && t.animate) { try { st.anim = t.animate([{ transform: 'translateY(3px)' }, { transform: 'none' }], { duration: SETTLE_MS, easing: 'ease-out' }); } catch (_) {} }   // (transform only: the first frame is already the whole card)
      listen(true);
    }
    watch(dot);
  }
  function hide() {
    if (!st.tip) return;
    st.on = false; st.dot = null; st.key = '';
    unwatch(); listen(false);
    st.tip.removeAttribute('data-on');
    if (st.anim) { try { st.anim.cancel(); } catch (_) {} st.anim = null; }
  }
  // a picture that arrives while its dot is shown replaces the spinner in the same card
  function settled(key) {
    if (!st.on || !st.dot || !st.dot.isConnected) return;
    const info = resolve(st.dot); if (info.key !== key) return;
    paint(info, cache.get(key) || null); place(st.dot);
  }
  /* while the card shows: a redraw that replaced the dot hands the card to its successor (found by key); rows that moved move the card */
  function findByKey(scope, key) { return scope && key ? scope.querySelector(`.pdot[data-pk="${String(key).replace(/["\\]/g, '')}"]`) : null; }
  function onMutate() {
    if (!st.on) return;
    const d = st.dot;
    if (d && d.isConnected) { place(d); return; }
    const n = st.scope && st.scope.isConnected ? findByKey(st.scope, st.key) : null;
    if (!n) { hide(); return; }
    if (st.how === 'focus') { try { n.focus({ preventScroll: true }); } catch (_) {} show(n, 'focus'); return; }
    let at = null; try { at = doc.elementFromPoint(st.x, st.y); } catch (_) {}
    const under = dotOf(at);
    if (under) show(under, 'pointer'); else hide();   // (the pointer is where it was: if a dot is still under it, the card stays)
  }
  function watch(dot) {
    const scope = dot.closest('[data-pd-scope]') || dot.parentElement;
    if (st.scope === scope && st.mo) return;
    unwatch(); st.scope = scope;
    if (root.MutationObserver && scope) { st.mo = new root.MutationObserver(onMutate); st.mo.observe(scope, { childList: true, subtree: true }); }
  }
  function unwatch() { if (st.mo) { try { st.mo.disconnect(); } catch (_) {} st.mo = null; } st.scope = null; }
  const onMove = ev => { if (st.on && st.how === 'pointer') { st.x = ev.clientX; st.y = ev.clientY; } };
  const onScroll = () => { if (st.on) hide(); };
  const onBlur = () => { if (st.on) hide(); };
  let listening = false;
  function listen(on) {
    if (on === listening) return; listening = on;
    const f = on ? 'addEventListener' : 'removeEventListener';
    doc[f]('pointermove', onMove, { capture: true, passive: true }); doc[f]('scroll', onScroll, { capture: true, passive: true }); root[f]('blur', onBlur);
  }

  /* ═══ the page's one set of listeners ═══ */
  doc.addEventListener('pointerover', ev => {
    if (ev.pointerType === 'touch') return;   // (touch has no hover: a press shows the card)
    const d = dotOf(ev.target); if (!d) return;
    if (d === st.dot && st.on) return;
    show(d, 'pointer', ev);
  }, true);
  doc.addEventListener('pointerout', ev => {
    if (!st.on || ev.pointerType === 'touch') return;
    const d = dotOf(ev.target); if (!d || d !== st.dot) return;
    if (dotOf(ev.relatedTarget)) return;   // dot to dot: the next dot's pointerover swaps the picture, no gap
    hide();
  }, true);
  doc.addEventListener('pointerdown', ev => {
    if (ev.pointerType !== 'touch') return;
    const d = dotOf(ev.target); if (d) show(d, 'touch', ev); else if (st.on) hide();
  }, true);
  for (const type of ['pointerup', 'pointercancel']) doc.addEventListener(type, ev => { if (st.on && st.how === 'touch' && ev.pointerType === 'touch') hide(); }, true);
  doc.addEventListener('focusin', ev => { const d = dotOf(ev.target); if (d) show(d, 'focus'); }, true);
  doc.addEventListener('focusout', ev => { if (st.on && st.how === 'focus' && dotOf(ev.target) === st.dot && st.dot.isConnected) hide(); }, true);   // (a dot taken out by a redraw is not a blur: its successor takes the card)
  doc.addEventListener('keydown', ev => {
    if (ev.key === 'Escape') { if (st.on) hide(); return; }   // (Esc is also the panel's: the card goes first, the key goes on)
    const d = dotOf(ev.target); if (!d || ev.altKey || ev.ctrlKey || ev.metaKey) return;
    if (ev.key === 'Enter' || ev.key === ' ' || ev.key === 'Spacebar') { ev.preventDefault(); ev.stopPropagation(); activate(d, ev); return; }
    if (ev.key === 'ArrowLeft' || ev.key === 'ArrowRight') {
      const all = [...d.parentNode.querySelectorAll('.pdot')], to = all[clamp(all.indexOf(d) + (ev.key === 'ArrowRight' ? 1 : -1), 0, all.length - 1)];
      if (to && to !== d) { ev.preventDefault(); ev.stopPropagation(); to.focus({ preventScroll: true }); }   // (at the end of a group the key is the host's: the panel takes the first dot's Left back to its row)
    }
  }, true);
  doc.addEventListener('click', ev => {
    const d = dotOf(ev.target); if (!d) return;
    ev.preventDefault(); ev.stopPropagation();   // (a press on a dot opens its piece; the row around it does not also open the order)
    activate(d, ev);
  }, true);

  /* ═══ a press: the order window on that piece ═══ */
  /** The options that open the order window on one piece: its pool id (the exact copy), its line's key, and `pick` (the piece switcher opens on it). */
  function openOpts(x) {
    const pool = String((x && x.pool) || ''), line = String((x && x.line) || lineOf(pool));
    if (!pool && !line) return {};   // (a dot that cannot say which piece it is opens the order as the row does, never a guessed piece)
    return Object.assign({ pick: true }, pool ? { poolId: pool } : {}, line ? { row: { key: line } } : {});
  }
  function openOrder(el, x) {
    const rid = String(x.order || '').replace(/\D/g, ''); if (!rid) return false;
    const o = openOpts(x);
    try { if (typeof root.openOrderFrom === 'function') { const r = root.openOrderFrom(el, rid, o); if (r !== false) return true; } } catch (e) { warn('open order', e); }
    try { if (root.OrderWin && typeof root.OrderWin.openOrder === 'function') { root.OrderWin.openOrder(rid, Object.assign({ from: el }, o)); return true; } } catch (e) { warn('open order', e); }
    return false;
  }
  function activate(d, ev) {
    hide();
    const pool = d.getAttribute('data-pd-p') || '', detail = { order: d.getAttribute('data-pd-o') || '', pool, line: d.getAttribute('data-pd-l') || lineOf(pool), n: +d.getAttribute('data-pd-n') || 0, ring: d.classList.contains('ring'), dot: d, via: ev && ev.type || 'click' };
    let ce; try { ce = new root.CustomEvent('piecedot:open', { bubbles: true, cancelable: true, detail }); } catch (_) { ce = null; }
    if (ce) { d.dispatchEvent(ce); if (ce.defaultPrevented) return true; }
    return openOrder(d, detail);
  }

  root.PieceDots = {
    html, warm, hide, openOpts, activate,
    shown: () => st.on, current: () => st.dot, tip: () => st.tip,
    // for the checks: what the cache holds and has done
    stats: () => ({ size: cache.size, cap: CAP, running, queued: queue.length, ready: [...cache.values()].filter(e => e.state === 'ready').length, ...counts }),
    has: key => { const e = cache.get(key); return e ? e.state : ''; },
    keyOf: dot => resolve(dot).key,
    CAP, NO_DESIGN, FAILED
  };
})(typeof self !== 'undefined' ? self : this);
