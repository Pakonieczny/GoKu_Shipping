/* Charm Nest · Library issues panel (window.LibraryIssues).
   Paul, 5 Oct 2026 (points 4, 8, 10): "instead of a drop-down the way you have it, I would rather the user click on the '!'
   in the progress timeline and have that expand a menu to see the specific issues with shortcut links to those issues
   holding the sheet back from moving onto the laser process" · "easy to understand at a glance, beautiful tastefully
   designed with minimal text and maximum visuals" · "I only wanna see the issues ... do not show completed progress in this
   list ... completely remove all UI pertaining to the back engraving status".

   What is seen. Each sheet card's small step rail carries a '!' button (charm-nest-bridge.js, Library-set-card's rail:
   <button class="flowDot flowBang" data-issues-open data-issues-id="<sheetId>" data-issues-step="<step>">). A press opens ONE
   small panel that grows out of that '!' (the app's own curve, cubic-bezier(.2,.8,.2,1)), on a fixed layer, never under the top
   bar and never past the bottom edge (it flips above the '!' or scrolls inside itself). It lists ONLY what holds that sheet
   back, as compact visual rows: the order's picture (its Etsy listing photo from the page's own cache; a quiet piece icon until
   it is there or when it has none), the customer and the order number on one short line, ONE short reason chip ("1 piece not on
   a sheet yet", "1 piece has no SKU", "Waits on SS Sheet 1"), and a chevron: a press opens the order (or the sheet) with the
   page's own hand-off, and when that window closes the panel is back, showing what is still left. More than six issues fold
   into groups by reason (a chip, a count, a small stack of pictures) with "Show N more". The sheet's own blocker (the
   current step) is ONE small chip at the top: for Engraving only the single link "Open engraving approvals". Never a completed
   step, never an engraving count or row, never a QR / back-file tick, never a wall of words.

   What it does not do. It reads, it never writes: no order, sheet, seal or record is changed here, no network call is made
   (the picture comes from ListMedia's own cache and cached lookup), and it keeps no poller: LaserReview's paint (which the
   Library's live read already drives, about every 3 s) calls LibraryIssues.refresh(), and an open panel updates its rows in
   place (a row whose reason changed re-words itself, a row that is no longer an issue folds away, a new one slides in; the
   scroll position and the focused row stay).

   Closes on Esc (focus goes back to the '!'), a press outside, the '!' pressed again, the sheet leaving the screen, or
   when nothing is left to show. Keyboard: Tab / Shift+Tab stay inside, Up/Down/Home/End move between rows, Enter opens.
   Reduced motion: a short fade only. Touch: rows are 52 px tall, the panel is at most the screen width less 16 px.

   Data: LaserReview.issuesOf(sheetId, anchor) -> { id, label, code, metal, ready, done, step, issues:[...], explain } where issues
   is CharmNestReadiness.issues(...) (round 2 interface B); until that exists a thin adapter reads today's explain() items.
   Everything is feature-detected (window.X?.): without LaserReview, OrderWin or SheetWin nothing here throws. */
(function (root) {
  "use strict";
  const doc = root.document;
  if (!doc || root.LibraryIssues) return;

  /* ═══ words (pieces, never "lines") ═══ */
  const clamp = (v, lo, hi) => Math.max(lo, Math.min(hi, v));
  const plural = (n, one, many) => `${n} ${n === 1 ? one : many}`;
  const sheetsWord = labels => labels.length === 1 ? labels[0] : labels.length ? plural(labels.length, 'sheet', 'sheets') : 'another sheet';
  const REASON = {
    pooled: { tone: 'gold', group: () => 'Not on a sheet yet', chip: n => `${plural(n, 'piece', 'pieces')} not on a sheet yet` },
    noSku: { tone: 'clay', group: () => 'No SKU', chip: n => n === 1 ? '1 piece has no SKU' : `${n} pieces have no SKU` },
    unmatched: { tone: 'clay', group: () => 'Unknown SKU', chip: n => n === 1 ? '1 piece has an unknown SKU' : `${n} pieces have unknown SKUs` },
    noDesign: { tone: 'clay', group: () => 'No design', chip: n => n === 1 ? '1 piece has no design' : `${n} pieces have no design` },
    held: { tone: 'clay', group: () => 'On hold', chip: n => n === 1 ? '1 piece is on hold' : `${n} pieces are on hold` },
    otherSheetNotReady: { tone: 'slate', group: s => `Waits on ${s}`, chip: (n, s) => `Waits on ${s}` },
    unverified: { tone: 'gold', group: () => 'Not checked yet', chip: () => 'Not checked yet' }
  };
  const MATE = { waitsOnSheet: 'Not ready yet', missingSheet: 'Not found', setMissing: 'Set not loaded' };
  // a sentence the readiness gave for a reason this panel has no wording for: at most six words, "line" said as "piece"
  const tidy = t => {
    const w = String(t || '').replace(/\s+/g, ' ').replace(/\b(other )?lines\b/gi, '$1pieces').replace(/\bline\b/gi, 'piece').replace(/[.\s]+$/, '').split(' ').filter(Boolean);
    const s = w.slice(0, 6).join(' ') + (w.length > 6 ? '…' : '');
    return s ? s[0].toUpperCase() + s.slice(1) : '';
  };
  const OWN = {
    engraving: { label: 'Open engraving approvals', icon: 'pen', go: 'engraving' },
    backFiles: { label: 'Back files not saved yet', icon: 'file' },
    qr: { label: 'QR label missing', icon: 'qr' },
    nesting: { label: 'Layout not ready', icon: 'grid' }
  };
  const METAL_OF = { GF: 'gold', SS: 'silver', RG: 'rose', '10K': 'gold10k', '14K': 'gold14k' };
  const STEP = { nesting: 'Nesting', engraving: 'Engraving', backFiles: 'Back files', qr: 'QR label', orders: 'Order check', laser: 'Laser cutting' };   // the rail's own words
  const HARD = new Set(['noSku', 'unmatched', 'noDesign', 'held']);   // a person has to fix these (the count chip turns clay); the others only wait
  const SIX = 6, SHOWN = 3;   // more than SIX issues fold into groups; a group shows SHOWN rows before "Show N more"

  /* ═══ the adapter: today's explain() items as issues (used only until CharmNestReadiness.issues exists) ═══ */
  function adapt(e, feed) {
    const out = [];
    if (!e || !Array.isArray(e.steps) || e.ready || e.done) return out;
    let own = false;
    for (const s of e.steps) {
      if (s.state === 'done' || !s.items) continue;
      if (s.key === 'orders') {
        for (const it of s.items) {
          if (it.kind === 'order') {
            const m = /^Order (\d+)(?: \((.*)\))?$/.exec(it.label || '') || [];
            const why = String(it.why || ''), other = /on ([A-Z0-9]{2,3} Sheet \d+)/.exec(why);
            const key = /SKU not in a master|no SKU/i.test(why) ? 'unmatched' : /no design/i.test(why) ? 'noDesign' : /hold|need review/i.test(why) ? 'held' : other ? 'otherSheetNotReady'
              : /not (on|every)|saved sheet|pooled/i.test(why) ? 'pooled' : 'otherSheetNotReady';
            out.push({ step: 'orders', key, orderId: String(it.id), orderLabel: 'Order ' + it.id, customer: m[2] || '', pieces: [{ index: 0, label: '', sheetLabel: other ? other[1] : null, why }], open: { type: 'order', id: String(it.id) } });
          }
        }
      } else if (s.key === 'laser') {
        for (const it of s.items) if (it.kind === 'sheet') out.push({ step: 'laser', key: 'waitsOnSheet', label: it.label, open: { type: 'sheet', id: it.id } });
      } else if (!own && OWN[s.key]) {
        own = true;
        out.unshift({ step: s.key, key: s.key, label: OWN[s.key].label, open: { type: 'sheet', id: feed.id } });
      }
    }
    return out;
  }

  /* ═══ the model: what the panel shows, from the issues (pure) ═══ */
  function reasonOf(it) {
    const pieces = Array.isArray(it.pieces) ? it.pieces : [], n = Math.max(1, pieces.length || +it.pieceCount || 1);
    const labels = [...new Set(pieces.map(p => p && p.sheetLabel).filter(Boolean))];
    const r = REASON[it.key];
    if (!r) { const text = tidy((pieces[0] && pieces[0].why) || it.why || it.label || 'Not ready') || 'Not ready'; return { id: 'x:' + text, tone: 'gold', chip: text, group: text }; }
    const s = it.key === 'otherSheetNotReady' ? sheetsWord(labels) : '';
    let chip = r.chip(n, s);
    if (it.key === 'held') {   // a held piece says why it is held ("A piece of this order is held: Check customer changes" -> "On hold: check customer changes")
      // (the held piece's own words, else the order's: "A piece of this order is held: Check customer changes")
      const hp = pieces.find(p => p && (p.kind === 'held' || /held/i.test(p.why || ''))) || pieces[0], own = String((hp && hp.why) || '').replace(/^.*\bheld:\s*/i, ''), whole = /\bheld:\s*(.+)$/i.exec(String(it.why || ''));
      const useful = t => t && !/^(a )?piece\b/i.test(t) && !/^(held|on hold)\.?$/i.test(t.trim());
      const tail = tidy(useful(own) ? own : whole && useful(whole[1]) ? whole[1] : '').replace(/…$/, '');
      if (tail) { const w = tail.split(' ').slice(0, 4).join(' '); chip = 'On hold: ' + (/^[A-Z][a-z]/.test(w) ? w[0].toLowerCase() + w.slice(1) : w); }
    }
    return { id: it.key + (s ? ':' + s : ''), tone: r.tone, chip, group: r.group(s) };
  }
  function model(feed, opts) {
    opts = opts || {};
    const all = Array.isArray(feed && feed.issues) ? feed.issues.filter(Boolean) : [];
    // one panel per step of the rail: the '!' on Order check lists the orders, on Engraving the one link, on Laser cutting the sheets;
    // a '!' whose own step has nothing falls back to whatever the sheet is held by (never an empty panel)
    const mine = opts.step ? all.filter(it => (it.step || 'orders') === opts.step) : [];
    const list = mine.length ? mine : all, step = mine.length ? opts.step : (list[0] && (list[0].step || 'orders')) || opts.step || '';
    const label = String((feed && feed.label) || ''), parts = /^(\S+)\s+(.*)$/.exec(label) || [];
    const code = (feed && feed.code) || (METAL_OF[parts[1]] ? parts[1] : ''), name = code && parts[2] ? parts[2] : label;
    const m = { id: feed && feed.id, label, code, metal: (feed && feed.metal) || METAL_OF[code] || '', name, step, title: STEP[step] || 'Needs attention', own: null, notes: [], sheets: [], orders: [], groups: null, count: 0, hard: false, checking: !!(feed && feed.checking) };
    const seen = new Set();
    for (const it of list) {
      if (it.step === 'laser') {
        const id = (it.open && it.open.id) || '';
        if (seen.has('s:' + (id || it.label))) continue;
        seen.add('s:' + (id || it.label));
        const lc = (/^([A-Z0-9]{2,3}) Sheet/.exec(it.label || '') || [])[1] || '';
        m.sheets.push({ k: 's:' + (id || it.label), id: String(id), label: String(it.label || 'Sheet'), code: lc, metal: METAL_OF[lc] || '', key: it.key || 'waitsOnSheet', chip: MATE[it.key] || MATE.waitsOnSheet });
      } else if (it.orderId && (!it.step || it.step === 'orders')) {
        const id = String(it.orderId);
        if (seen.has('o:' + id)) continue;   // an order is listed once
        seen.add('o:' + id);
        const r = reasonOf(it), pool = it.open && it.open.poolId, type = (it.open && it.open.type) || 'order', pieces = Array.isArray(it.pieces) ? it.pieces : [];
        const bad = Math.max(1, pieces.length), total = clamp(Math.max(+it.pieceCount || 0, bad + 1), 2, 6);
        const dots = [];
        for (let i = 0; i < Math.min(bad, total - 1); i++) { const p = pieces[i] || {}, lc = (/^([A-Z0-9]{2,3}) Sheet/.exec(p.sheetLabel || '') || [])[1] || ''; dots.push({ ring: true, metal: METAL_OF[lc] || '', tone: r.tone }); }
        while (dots.length < total) dots.unshift({ ring: false, metal: (feed && feed.metal) || '', tone: '' });
        const cust = String(it.customer || '').trim();
        m.orders.push({ k: 'o:' + id, orderId: id, key: it.key || '', customer: cust, listingId: it.listingId ? String(it.listingId) : '', thumb: typeof it.thumb === 'string' ? it.thumb : (it.thumb && it.thumb.url) || '', reason: r, dots, go: type === 'piece' ? 'piece' : type === 'sheet' ? 'sheet' : 'order', target: String((it.open && it.open.id) || id), pool: pool ? String(pool) : '' });
        if (HARD.has(it.key)) m.hard = true;
      } else if (it.key === 'unverified' && !it.orderId) {   // the sheet's orders could not be read yet: ONE quiet chip for the whole sheet, worded from the readiness' own label
        if (seen.has('n:unverified')) continue;
        seen.add('n:unverified');
        m.notes.push({ k: 'n:unverified', key: 'unverified', step: 'orders', label: tidy(it.label) || 'Orders not checked yet', orders: (Array.isArray(it.orderIds) ? it.orderIds : []).map(String), tone: 'gold' });
      } else if (!it.orderId && !m.own && OWN[it.step]) {
        const o = OWN[it.step], go = o.go || (it.open && it.open.type === 'sheet' ? 'sheet' : '');
        m.own = { k: 'own:' + it.step, step: it.step, key: it.key || it.step, label: o.go ? o.label : String(it.label || o.label), icon: o.icon, go, target: String((it.open && it.open.id) || (feed && feed.id) || '') };
      }
    }
    m.count = m.orders.length + m.sheets.length + m.notes.length;
    if (m.orders.length > SIX) {
      const by = new Map();
      for (const o of m.orders) { const g = by.get(o.reason.id) || { k: 'g:' + o.reason.id, tone: o.reason.tone, text: o.reason.group, orders: [] }; g.orders.push(o); by.set(o.reason.id, g); }
      m.groups = [...by.values()];
    }
    return m;
  }

  /* ═══ drawing ═══ */
  const esc = t => String(t == null ? '' : t).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const ICON = {
    piece: '<svg viewBox="0 0 24 24" aria-hidden="true"><circle cx="12" cy="4.6" r="2"/><path d="M12 6.6v2"/><path d="M12 8.6c-3.5 0-5.9 2.4-5.9 5.6 0 3.2 2.5 5.6 5.9 5.6s5.9-2.4 5.9-5.6c0-3.2-2.4-5.6-5.9-5.6z"/></svg>',
    pen: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4 20h4L19 9a2.1 2.1 0 0 0-3-3L5 17z"/><path d="M14 8l3 3"/></svg>',
    file: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M7 3.5h7l4 4V20.5H7z"/><path d="M14 3.5v4h4"/></svg>',
    qr: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4 4h6v6H4zM14 4h6v6h-6zM4 14h6v6H4zM14 14h2.5v2.5H14zM18 18h2v2h-2z"/></svg>',
    grid: '<svg viewBox="0 0 24 24" aria-hidden="true"><rect x="4" y="4" width="16" height="16" rx="3"/><path d="M4 12h16M12 4v16"/></svg>',
    bang: '<svg viewBox="0 0 24 24" aria-hidden="true"><circle cx="12" cy="12" r="8"/><path d="M12 8v4.6M12 15.6v.2"/></svg>',
    clock: '<svg viewBox="0 0 24 24" aria-hidden="true"><circle cx="12" cy="12" r="8"/><path d="M12 7.6V12l3 1.8"/></svg>',
    chev: '<svg class="lisCh" viewBox="0 0 16 16" aria-hidden="true"><path d="M6 3.5 10.5 8 6 12.5"/></svg>'
  };
  const photos = new Map();   // listing id -> { url | null, busy, at }
  const thumbHtml = o => `<span class="lisTh" data-lid="${esc(o.listingId)}" data-url="${esc(o.thumb)}">${ICON.piece}</span>`;
  const dotsHtml = o => `<span class="lisDots" aria-hidden="true">${o.dots.map(d => `<i class="lisDot${d.ring ? ' ring' : ''}"${d.metal ? ` data-m="${esc(d.metal)}"` : ''}${d.tone ? ` data-t="${d.tone}"` : ''}></i>`).join('')}</span>`;
  const rowHtml = (o, chip) => `<button type="button" class="lisRow lisGo" data-go="${o.go}" data-id="${esc(o.target)}" data-pool="${esc(o.pool)}" data-issue-step="orders" data-issue-key="${esc(o.key)}" data-issue-order="${esc(o.orderId)}" aria-label="${esc(`Open order ${o.orderId}${o.customer ? ', ' + o.customer : ''}${chip ? ', ' + o.reason.chip : ''}`)}">${thumbHtml(o)}<span class="lisTx"><span class="lisWho"><b>${esc(o.orderId)}</b>${o.customer ? `<i>${esc(o.customer)}</i>` : ''}</span><span class="lisSub">${dotsHtml(o)}${chip ? `<span class="lisChip ${o.reason.tone}">${esc(o.reason.chip)}</span>` : ''}</span></span>${ICON.chev}</button>`;
  const sheetRowHtml = s => { const inner = `<span class="lisTh lisTile" ${s.metal ? `data-m="${esc(s.metal)}"` : ''}>${esc(s.code || '')}</span><span class="lisTx"><span class="lisWho"><b class="sans">${esc(s.label)}</b></span><span class="lisSub"><span class="lisChip slate">${esc(s.chip)}</span></span></span>`, hook = ` data-issue-step="laser" data-issue-key="${esc(s.key)}" data-issue-sheet="${esc(s.id)}"`;
    return s.id ? `<button type="button" class="lisRow lisGo" data-go="sheet" data-id="${esc(s.id)}"${hook} aria-label="${esc('Open ' + s.label)}">${inner}${ICON.chev}</button>` : `<div class="lisRow lisStatic"${hook}>${inner}<span></span></div>`; };
  // the groups of a long list start folded (a chip, a count, a stack of pictures) unless there is only one; a press unfolds one
  const openGroup = (m, g, ui) => ui.fold.has(g.k) ? ui.fold.get(g.k) : m.groups.length === 1;
  function blocks(m, ui) {
    const out = [];
    if (m.own) {
      const o = m.own, inner = `<span class="lisOwnI">${ICON[o.icon] || ICON.bang}</span><span>${esc(o.label)}</span>${o.go ? ICON.chev : ''}`;
      const hook = ` data-issue-step="${esc(o.step)}" data-issue-key="${esc(o.key)}"`;
      out.push({ k: o.k, html: o.go ? `<button type="button" class="lisOwn lisGo" data-go="${o.go}" data-id="${esc(o.target)}"${hook}>${inner}</button>` : `<div class="lisOwn"${hook}>${inner}</div>` });
    }
    for (const n of m.notes) out.push({ k: n.k, html: `<div class="lisOwn lisNote ${n.tone}" data-issue-step="${esc(n.step)}" data-issue-key="${esc(n.key)}" data-issue-orders="${esc(n.orders.join(','))}"><span class="lisOwnI">${ICON.clock}</span><span>${esc(n.label)}</span></div>` });
    for (const s of m.sheets) out.push({ k: s.k, html: sheetRowHtml(s) });
    if (m.groups) {
      for (const g of m.groups) {
        const open = openGroup(m, g, ui), full = ui.more.has(g.k), list = open ? (full ? g.orders : g.orders.slice(0, SHOWN)) : [];
        const stack = open ? '' : `<span class="lisStack" aria-hidden="true">${g.orders.slice(0, 4).map(o => `<span class="lisMini" data-lid="${esc(o.listingId)}" data-url="${esc(o.thumb)}"></span>`).join('')}</span>`;
        out.push({ k: g.k + ':h', html: `<button type="button" class="lisGroup" data-fold="${esc(g.k)}" data-issue-group="${esc(g.orders[0].key)}" data-issue-orders="${esc(g.orders.map(o => o.orderId).join(','))}" aria-expanded="${open}"><span class="lisChip big ${g.tone}">${esc(g.text)}</span><span class="lisN">${g.orders.length}</span>${stack}${ICON.chev}</button>` });
        for (const o of list) out.push({ k: o.k, html: rowHtml(o, false) });
        if (open && g.orders.length > SHOWN) out.push({ k: g.k + ':m', html: `<button type="button" class="lisMore" data-more="${esc(g.k)}" aria-expanded="${full}">${full ? 'Show less' : `Show ${g.orders.length - SHOWN} more`}</button>` });
      }
    } else for (const o of m.orders) out.push({ k: o.k, html: rowHtml(o, true) });
    for (const b of out) b.sig = b.html;
    return out;
  }
  const sigOf = (m, ui) => JSON.stringify([m.title, m.hard, m.checking, m.count, m.own && [m.own.k, m.own.label], blocks(m, ui).map(b => b.html)]);

  /* ═══ state ═══ */
  const st = { open: false, id: '', step: '', anchor: null, panel: null, body: null, head: null, handed: false, back: null, more: new Set(), fold: new Map(), sig: '', io: null, anim: null, seq: 0, shown: false };
  const reduced = () => { try { return root.Motion && root.Motion.reduced ? !!root.Motion.reduced() : !!(root.matchMedia && root.matchMedia('(prefers-reduced-motion: reduce)').matches); } catch (_) { return false; } };
  const EASE = 'cubic-bezier(.2,.8,.2,1)';
  const anim = (el, frames, o) => { try { return el.animate ? el.animate(frames, o) : null; } catch (_) { return null; } };
  const warn = (what, e) => { try { console.warn('Library issues: ' + what, e); } catch (_) {} };

  function css() {
    if (doc.getElementById('lisCss')) return;
    const s = doc.createElement('style'); s.id = 'lisCss';
    s.textContent = `.lisPanel{position:fixed;z-index:2147483000;left:0;top:0;width:336px;max-width:calc(100vw - 16px);display:flex;flex-direction:column;background:var(--card,#fffefb);color:var(--ink,#1c1a17);border:1px solid var(--line,#e4ddd0);border-radius:12px;box-shadow:0 1px 0 rgba(255,255,255,.6) inset,0 14px 34px rgba(30,26,20,.16),0 2px 6px rgba(30,26,20,.06);font:12px/1.4 var(--sans,system-ui,sans-serif);transform-origin:var(--ox,34px) var(--oy,-6px);opacity:0;-webkit-font-smoothing:antialiased}
.lisPanel *{box-sizing:border-box}
.lisPanel[hidden]{display:none}
.lisPanel.handed{opacity:0!important;pointer-events:none;visibility:hidden}
.lisPanel::before{content:"";position:absolute;left:var(--ax,34px);top:-6px;width:10px;height:10px;margin-left:-5px;background:var(--card,#fffefb);border-left:1px solid var(--line,#e4ddd0);border-top:1px solid var(--line,#e4ddd0);transform:rotate(45deg)}
.lisPanel.up::before{top:auto;bottom:-6px;transform:rotate(225deg)}
.lisHead{flex:0 0 auto;display:flex;align-items:center;gap:10px;padding:11px 14px 7px;min-width:0}
.lisHead b{min-width:0;font:500 15px/1.2 var(--serif,Georgia,serif);color:var(--ink,#1c1a17);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.lisCount{margin-left:auto;flex:none;font:700 8.5px/1 var(--mono,monospace);letter-spacing:.1em;text-transform:uppercase;color:#7a5a1d;background:var(--goldSoft,#f0e6cd);border-radius:999px;padding:3px 7px}
.lisCount.clay{color:#8a3a26;background:var(--claySoft,#f4e3dc)}
.lisBusy{flex:none;display:inline-flex;align-items:center;gap:5px;font:11px/1 var(--sans,system-ui,sans-serif);color:var(--ink45,#938c80)}
.lisBusy{margin-left:auto}.lisBusy+.lisCount{margin-left:0}
.lisBusy .lisSpin{position:static}
.lisBody{flex:1 1 auto;min-height:0;overflow:auto;overscroll-behavior:contain;padding:2px 6px 8px;display:flex;flex-direction:column;gap:2px}
.lisBlk{min-width:0;flex:none}
.lisRow{all:unset;box-sizing:border-box;display:grid;grid-template-columns:36px minmax(0,1fr) 14px;align-items:center;gap:11px;width:100%;min-height:48px;padding:5px 8px;border:1px solid transparent;border-radius:10px;cursor:pointer;transition:background .15s,border-color .15s,transform .15s}
.lisRow:hover,.lisRow:focus-visible{border-color:var(--goldLine,#e3d3a6);background:var(--card2,#faf7f1);transform:translateY(-1px)}
.lisRow:focus-visible,.lisOwn:focus-visible,.lisMore:focus-visible{outline:2px solid var(--gold,#a9823f);outline-offset:1px}
.lisTh{position:relative;width:36px;height:36px;border-radius:8px;background:var(--card2,#faf7f1);border:1px solid var(--line,#e4ddd0);overflow:hidden;display:grid;place-items:center;color:var(--ink25,#c4bdb0)}
.lisTh svg{width:20px;height:20px;fill:none;stroke:currentColor;stroke-width:1.5;stroke-linecap:round;stroke-linejoin:round}
.lisTh img,.lisMini img{position:absolute;inset:0;width:100%;height:100%;object-fit:cover;opacity:0;transition:opacity .25s}
.lisTh img.on,.lisMini img.on{opacity:1}
.lisTh.has svg{opacity:0}
.lisTile{font:700 11px/1 var(--mono,monospace);letter-spacing:.04em;color:#fff;background:var(--c,var(--ink45,#938c80));border-color:transparent;text-shadow:0 1px 0 rgba(0,0,0,.18)}
.lisPanel [data-m=gold]{--mc:var(--m-gold,#c8a24e)}.lisPanel [data-m=silver]{--mc:var(--m-silver,#8d95a0)}.lisPanel [data-m=rose]{--mc:var(--m-rose,#c08578)}.lisPanel [data-m=gold10k]{--mc:var(--m-gold10k,#b08d2a)}.lisPanel [data-m=gold14k]{--mc:var(--m-gold14k,#d9b545)}
.lisTile[data-m]{--c:var(--mc)}
.lisSpin{position:absolute;right:3px;bottom:3px;width:10px;height:10px;border:2px solid var(--line,#e4ddd0);border-top-color:var(--ink70,#5b554c);border-radius:50%;animation:lisSpin .7s linear infinite}
@keyframes lisSpin{to{transform:rotate(360deg)}}
.lisTx{min-width:0;display:grid;gap:5px;align-content:center}
.lisWho{display:flex;align-items:baseline;gap:7px;min-width:0}
.lisWho b{flex:none;font:700 12px/1.2 var(--mono,monospace);color:var(--ink,#1c1a17);letter-spacing:0}
.lisWho b.sans{font:600 13px/1.25 var(--sans,system-ui,sans-serif)}
.lisWho i{min-width:0;font:11px/1.2 var(--sans,system-ui,sans-serif);font-style:normal;color:var(--ink45,#938c80);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.lisSub{display:flex;align-items:center;gap:7px;min-width:0}
.lisDots{display:inline-flex;gap:3px;flex:none}
.lisDot{width:8px;height:8px;border-radius:50%;background:var(--ink25,#c4bdb0)}
.lisDot[data-m]{background:var(--mc)}
.lisDot.ring{background:transparent;border:2px solid var(--clay,#b0563f)}
.lisDot.ring[data-t=gold]{border-color:var(--gold2,#caa861)}.lisDot.ring[data-t=slate]{border-color:var(--slate,#4a6b78)}.lisDot.ring[data-m]{border-color:var(--mc)}
.lisChip{display:inline-flex;align-items:center;gap:5px;min-width:0;max-width:100%;font:650 10.5px/1 var(--sans,system-ui,sans-serif);padding:4px 8px 4px 7px;border-radius:999px;background:var(--goldSoft,#f0e6cd);color:#7a5a1d;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.lisChip::before{content:"";flex:none;width:5px;height:5px;border-radius:50%;background:currentColor;opacity:.7}
.lisChip.clay{background:var(--claySoft,#f4e3dc);color:#8a3a26}
.lisChip.slate{background:var(--slateSoft,#e1ebee);color:#33525d}
.lisChip.big{font-size:11px;padding:5px 10px 5px 8px}
.lisCh{width:14px;height:14px;fill:none;stroke:var(--ink45,#938c80);stroke-width:1.7;stroke-linecap:round;stroke-linejoin:round;opacity:0;transition:opacity .15s,transform .15s}
.lisGo:hover .lisCh,.lisGo:focus-visible .lisCh{opacity:1;transform:translateX(2px)}
.lisOwn{all:unset;box-sizing:border-box;display:flex;align-items:center;gap:10px;width:100%;padding:9px 11px;border:1px solid var(--line,#e4ddd0);border-radius:10px;background:var(--card2,#faf7f1);font:600 12.5px/1.25 var(--sans,system-ui,sans-serif);color:var(--ink,#1c1a17);transition:border-color .15s,transform .15s}
.lisOwn>span:nth-child(2){flex:1 1 auto;min-width:0}
button.lisOwn{cursor:pointer}button.lisOwn:hover,button.lisOwn:focus-visible{border-color:var(--goldLine,#e3d3a6);transform:translateY(-1px)}
.lisOwnI{flex:none;width:28px;height:28px;border-radius:50%;display:grid;place-items:center;background:var(--goldSoft,#f0e6cd);color:#7a5a1d}
.lisOwnI svg{width:15px;height:15px;fill:none;stroke:currentColor;stroke-width:1.6;stroke-linecap:round;stroke-linejoin:round}
.lisOwn .lisCh{opacity:1}
.lisGroup{all:unset;box-sizing:border-box;display:flex;align-items:center;gap:8px;width:100%;min-height:44px;padding:6px 8px;border:1px solid transparent;border-radius:10px;cursor:pointer;transition:background .15s,border-color .15s}
.lisGroup:hover,.lisGroup:focus-visible{border-color:var(--goldLine,#e3d3a6);background:var(--card2,#faf7f1)}
.lisGroup:focus-visible{outline:2px solid var(--gold,#a9823f);outline-offset:1px}
.lisGroup .lisCh{opacity:1;transition:transform .22s cubic-bezier(.2,.8,.2,1)}
.lisGroup[aria-expanded=true] .lisCh{transform:rotate(90deg)}
.lisN{font:700 10px/1 var(--mono,monospace);color:var(--ink45,#938c80)}
.lisStack{margin-left:auto;display:flex;padding-left:8px}
.lisGroup>.lisCh{margin-left:auto}.lisStack+.lisCh{margin-left:2px}
.lisMini{position:relative;width:22px;height:22px;margin-left:-8px;border-radius:50%;border:2px solid var(--card,#fffefb);background:var(--paper2,#ebe5d9);overflow:hidden;box-shadow:0 0 0 1px var(--line2,#efe9dd)}
.lisMore{all:unset;box-sizing:border-box;cursor:pointer;display:block;margin:1px 0 3px 56px;padding:4px 8px;font:600 11.5px/1.2 var(--sans,system-ui,sans-serif);color:var(--slate,#4a6b78);border-radius:8px}
.lisMore:hover{text-decoration:underline}
.lisStatic{cursor:default}
button.flowDot[data-issues-open]{position:relative;cursor:pointer}
button.flowDot[data-issues-open]::before{content:"";position:absolute;inset:-9px;border-radius:50%}
button.flowDot[data-issues-open]:focus-visible{outline:2px solid var(--gold,#a9823f);outline-offset:2px}
button.flowDot[data-issues-open][aria-expanded="true"]{box-shadow:0 0 0 4px rgba(176,86,63,.2)!important}
@media (hover:none){.lisCh{opacity:1}}
@media (max-width:480px){.lisRow{min-height:56px}}
@media (prefers-reduced-motion:reduce){.lisCh,.lisRow,.lisOwn,.lisTh img,.lisMini img{transition:none}.lisRow:hover,.lisRow:focus-visible,button.lisOwn:hover{transform:none}.lisSpin{animation-duration:1.6s}}`;
    doc.head.appendChild(s);
  }

  /* pictures: the listing photo from the page's own cache (LaserReview.photo -> ListMedia), asked for once (again a few seconds later
     while a row still has none: the page's own preparation may have found it), laid in when it is there. A wait shows a small spinner. */
  const spin = (host, on) => {
    if (!host.classList.contains('lisTh') || host.classList.contains('lisTile')) return;
    const sp = host.querySelector('.lisSpin');
    if (on && !sp) { const i = doc.createElement('i'); i.className = 'lisSpin'; i.setAttribute('role', 'status'); i.setAttribute('aria-label', 'Loading picture'); host.appendChild(i); } else if (!on && sp) sp.remove();
  };
  function lay(host, url) {
    if (host.querySelector('img')) return;
    const img = doc.createElement('img'); img.alt = ''; img.decoding = 'async'; img.referrerPolicy = 'no-referrer';
    img.onload = () => { img.classList.add('on'); host.classList.add('has'); spin(host, false); };
    img.onerror = () => { img.remove(); spin(host, false); };
    img.src = root.cors ? root.cors(url) : url; host.appendChild(img);
  }
  function ask(lid) {
    const rec = { url: null, busy: true, at: Date.now() }; photos.set(lid, rec);
    while (photos.size > 400) photos.delete(photos.keys().next().value);
    Promise.resolve().then(() => root.LaserReview.photo(lid)).then(u => { rec.url = u || null; }, () => {}).then(() => { rec.busy = false; rec.at = Date.now(); if (st.open && st.panel) pictures(st.panel); });
  }
  function pictures(scope) {
    const L = root.LaserReview, now = Date.now();
    for (const host of scope.querySelectorAll('[data-lid],[data-url]')) {
      if (host.querySelector('img')) continue;
      const lid = host.dataset.lid, url = host.dataset.url;
      if (url) { lay(host, url); continue; }
      if (!lid || !L || typeof L.photo !== 'function') continue;
      const rec = photos.get(lid);
      if (rec && rec.url) { lay(host, rec.url); continue; }
      if (!rec || (!rec.busy && now - rec.at > 4000)) ask(lid);
      spin(host, !!photos.get(lid).busy);
    }
  }

  /* the feed: LaserReview's readiness for one sheet; issues from CharmNestReadiness.issues, or today's explain() items */
  function feedOf(id, anchor) {
    const L = root.LaserReview;
    if (!L || typeof L.issuesOf !== 'function') return null;
    let f = null;
    try { f = L.issuesOf(id, anchor); } catch (e) { warn('read', e); }
    if (!f) return null;
    if (!Array.isArray(f.issues)) f = Object.assign({}, f, { issues: adapt(f.explain, f) });
    return f;
  }
  const buttons = () => doc.querySelectorAll('[data-issues-open]');
  const idOf = b => b.dataset.issuesId || b.dataset.sheetId || '';
  const stepOf = b => b.dataset.issuesStep || b.dataset.step || '';
  function findAnchor() {
    if (st.anchor && st.anchor.isConnected && idOf(st.anchor) === st.id && st.anchor.getClientRects().length) return st.anchor;
    let any = null;
    for (const b of buttons()) if (idOf(b) === st.id && b.getClientRects().length) { if (stepOf(b) === st.step) return b; any = any || b; }
    return any;
  }
  const mark = on => { for (const b of buttons()) if (idOf(b) === st.id) b.setAttribute('aria-expanded', on && b === st.anchor ? 'true' : 'false'); };

  /* position: below the '!' (above when there is no room), under no bar, inside the screen */
  function place() {
    const p = st.panel, a = st.anchor;
    if (!p || !a || !a.isConnected) return false;
    const ar = a.getBoundingClientRect(), whole = (a.closest && a.closest('.flowStep') || a).getBoundingClientRect();   // (below the step's label, never over it)
    if (!ar.width && !ar.height) return false;
    const vw = root.innerWidth || 1200, vh = root.innerHeight || 800, bar = doc.querySelector('.topbar');
    const lo = Math.max(8, (bar ? bar.getBoundingClientRect().bottom : 0) + 8), hi = vh - 8, w = Math.min(336, vw - 16), gap = 11;
    p.style.width = w + 'px'; p.style.maxHeight = 'none';
    const h0 = p.offsetHeight, below = hi - (whole.bottom + gap), above = whole.top - gap - lo, down = h0 <= below || below >= above;
    const room = Math.max(140, down ? below : above);
    p.style.maxHeight = room + 'px';
    const h = Math.min(h0, room), x = clamp(ar.left + ar.width / 2 - 34, 8, vw - w - 8), y = down ? whole.bottom + gap : whole.top - gap - h, ax = clamp(ar.left + ar.width / 2 - x, 20, w - 20);
    p.style.left = Math.round(x) + 'px'; p.style.top = Math.round(Math.max(lo, y)) + 'px';
    p.style.setProperty('--ax', ax + 'px'); p.style.setProperty('--ox', ax + 'px'); p.style.setProperty('--oy', (down ? -6 : h + 6) + 'px');
    p.classList.toggle('up', !down);
    return true;
  }

  /* drawing the panel from a model: the first time all at once, afterwards by row, so what is still true does not flicker */
  function paintHead(m) {
    const n = m.count;
    st.head.innerHTML = `<b>${esc(m.title)}</b>${m.checking ? '<span class="lisBusy" role="status"><i class="lisSpin" aria-hidden="true"></i>Checking…</span>' : ''}${n ? `<span class="lisCount${m.hard ? ' clay' : ''}">${plural(n, 'issue', 'issues')}</span>` : ''}`;
  }
  function draw(m, first) {
    const bs = blocks(m, st), body = st.body, old = new Map([...body.children].map(n => [n.dataset.k, n]));
    const focus = body.contains(doc.activeElement) ? (doc.activeElement.closest('.lisBlk') || {}).dataset?.k : '';
    let prev = null, i = 0;
    paintHead(m);
    for (const b of bs) {
      let n = old.get(b.k); old.delete(b.k);
      if (!n) {
        n = doc.createElement('div'); n.className = 'lisBlk'; n.dataset.k = b.k; n.innerHTML = b.html; n._sig = b.sig;
        if (!reduced() && !first) anim(n, [{ opacity: 0, transform: 'translateY(5px)' }, { opacity: 1, transform: 'none' }], { duration: 220, easing: EASE });
        else if (first && !reduced() && i < 8) anim(n, [{ opacity: 0, transform: 'translateY(5px)' }, { opacity: 1, transform: 'none' }], { duration: 260, delay: 70 + i * 32, easing: EASE, fill: 'backwards' });
      } else if (n._sig !== b.sig) {
        const keep = n.contains(doc.activeElement), had = n.firstElementChild, tmp = doc.createElement('div');
        tmp.innerHTML = b.html; n._sig = b.sig;
        const now = tmp.firstElementChild;
        if (had && now && had.classList.contains('lisGroup') && now.classList.contains('lisGroup')) {   // (a group header changes in place: its chevron turns, it does not blink)
          had.setAttribute('aria-expanded', now.getAttribute('aria-expanded'));
          const os = had.querySelector('.lisStack'), ns = now.querySelector('.lisStack');
          if (os && !ns) os.remove(); else if (!os && ns) had.insertBefore(ns, had.querySelector('.lisCh'));
          had.querySelector('.lisN').textContent = now.querySelector('.lisN').textContent;
          const oc = had.querySelector('.lisChip'), nc = now.querySelector('.lisChip'); if (oc.outerHTML !== nc.outerHTML) oc.replaceWith(nc);
        } else { n.replaceChildren(...tmp.childNodes); if (keep) n.querySelector('.lisGo,.lisMore')?.focus({ preventScroll: true }); }
      }
      if (prev ? prev.nextSibling !== n : body.firstChild !== n) body.insertBefore(n, prev ? prev.nextSibling : body.firstChild);
      prev = n; i++;
    }
    for (const n of old.values()) {
      if (n.classList.contains('gone')) continue;
      n.classList.add('gone');
      const a = !reduced() && anim(n, [{ opacity: 1, maxHeight: n.offsetHeight + 'px' }, { opacity: 0, maxHeight: '0px' }], { duration: 180, easing: 'ease-in', fill: 'forwards' });
      if (a && a.finished) a.finished.then(() => n.remove(), () => n.remove()); else n.remove();
    }
    if (focus && !body.contains(doc.activeElement)) (body.querySelector(`.lisBlk[data-k="${focus.replace(/"/g, '')}"] .lisGo`) || body.querySelector('.lisGo'))?.focus({ preventScroll: true });
    pictures(st.panel);
  }

  /* ═══ open, refresh, close ═══ */
  function open(btn) {
    css();
    const id = idOf(btn); if (!id) return false;
    if (st.open) closeNow();
    const feed = feedOf(id, btn); if (!feed) return false;
    const m = model(feed, { step: stepOf(btn) });
    if (!m.own && !m.count) return false;   // (a '!' drawn a moment before the sheet became ready: nothing to say)
    Object.assign(st, { open: true, id, step: stepOf(btn), anchor: btn, handed: false, back: null, more: new Set(), fold: new Map(), sig: '', seq: st.seq + 1, shown: false });
    const p = st.panel = doc.createElement('div');
    p.className = 'lisPanel'; p.id = 'libIssuesPanel'; p.setAttribute('data-issues-for', 'sheet:' + id); p.setAttribute('role', 'dialog'); p.setAttribute('aria-modal', 'false'); p.setAttribute('aria-label', `What holds ${m.label || 'this sheet'} back`); p.tabIndex = -1;
    st.head = doc.createElement('div'); st.head.className = 'lisHead';
    st.body = doc.createElement('div'); st.body.className = 'lisBody';
    p.append(st.head, st.body);
    (btn.closest('dialog[open]') || doc.body).appendChild(p);
    btn.setAttribute('aria-controls', p.id);
    draw(m, true); st.sig = sigOf(m, st);
    mark(true);
    p.style.opacity = '0';
    place();
    const down = !p.classList.contains('up');
    p.style.opacity = '1';
    anim(p, reduced() ? [{ opacity: 0 }, { opacity: 1 }] : [{ opacity: 0, transform: `scale(.88) translateY(${down ? -7 : 7}px)` }, { opacity: 1, transform: 'none' }], { duration: reduced() ? 90 : 250, easing: EASE });
    watch();
    const first = p.querySelector('.lisGo'); (first || p).focus({ preventScroll: true });
    return true;
  }
  function watch() {
    unwatch();
    const a = st.anchor;
    if (root.IntersectionObserver && a) {
      st.io = new root.IntersectionObserver(es => {
        const e = es[es.length - 1]; if (!e || e.isIntersecting || !st.open || st.handed || st.anchor !== a) return;
        if (a.isConnected) { close(); return; }   // (it scrolled out of its list, or its tab was hidden)
        // (the card was drawn anew: the next refresh finds the '!' again; if nothing does, the panel goes)
        setTimeout(() => { if (st.open && st.anchor === a && !a.isConnected) { const f = findAnchor(); if (f) rebind(f); else close(); } }, 400);
      }, { threshold: 0 });
      st.io.observe(a);
    }
  }
  function unwatch() { if (st.io) { try { st.io.disconnect(); } catch (_) {} st.io = null; } }
  function rebind(a) { st.anchor = a; st.step = stepOf(a) || st.step; unwatch(); watch(); }
  function refresh() {
    if (!st.open || !st.panel) return false;
    const a = findAnchor();
    if (!a) { close(); return false; }
    if (a !== st.anchor) rebind(a);
    if (st.handed) return true;   // (shown again, from fresh data, when the window handed over to closes)
    const feed = feedOf(st.id, a), m = feed && model(feed, { step: st.step });
    if (!m || (!m.own && !m.count)) { close(); return false; }
    mark(true);
    const sig = sigOf(m, st);
    if (sig !== st.sig) { st.sig = sig; draw(m, false); } else pictures(st.panel);
    place();
    return true;
  }
  function closeNow() {
    unwatch();
    if (st.panel) { try { st.panel.remove(); } catch (_) {} }
    const was = st.id; st.open = false; st.panel = st.body = st.head = null; st.handed = false; st.back = null;
    for (const b of buttons()) if (idOf(b) === was) { b.setAttribute('aria-expanded', 'false'); }
    st.anchor = null;
  }
  function close(opts) {
    if (!st.open) return false;
    opts = opts || {};
    const p = st.panel, a = findAnchor() || st.anchor;
    unwatch();
    st.open = false;
    for (const b of buttons()) if (idOf(b) === st.id) b.setAttribute('aria-expanded', 'false');
    const fade = !reduced() && !st.handed && anim(p, [{ opacity: 1, transform: 'none' }, { opacity: 0, transform: 'scale(.97)' }], { duration: 140, easing: 'ease-in', fill: 'forwards' });
    p.setAttribute('inert', ''); p.style.pointerEvents = 'none';
    const done = () => { try { p.remove(); } catch (_) {} };
    if (fade && fade.finished) fade.finished.then(done, done); else done();
    if (opts.focus && a && a.isConnected) { try { a.focus({ preventScroll: true }); } catch (_) {} }
    st.panel = st.body = st.head = null; st.handed = false; st.back = null; st.anchor = null;
    return true;
  }
  function toggle(btn) {
    if (st.open && st.panel && idOf(btn) === st.id && !st.handed) { close({ focus: true }); return; }
    open(btn);
  }

  /* ═══ the hand-off (the page's own: an order or a sheet opens in its window, and this panel comes back when it closes) ═══ */
  function openOrder(el, rid, o) {
    rid = String(rid || '').replace(/\D/g, ''); if (!rid) return false;
    try { if (typeof root.openOrderFrom === 'function') { const r = root.openOrderFrom(el, rid, o || {}); if (r !== false) return true; } } catch (e) { warn('open order', e); }
    try { if (root.OrderWin && typeof root.OrderWin.openOrder === 'function') { root.OrderWin.openOrder(rid, Object.assign({ from: el }, o || {})); return true; } } catch (e) { warn('open order', e); }
    return false;
  }
  function openSheet(el, id) {
    id = String(id || ''); if (!id) return false;
    try { if (typeof root.openLibrarySheet === 'function') { root.openLibrarySheet(id); return true; } } catch (e) { warn('open sheet', e); }
    try { if (root.CN && typeof root.CN.openLibrarySheet === 'function') { root.CN.openLibrarySheet(id); return true; } } catch (e) { warn('open sheet', e); }
    try { if (root.SheetWin && typeof root.SheetWin.open === 'function') { const r = el.getBoundingClientRect(); root.SheetWin.open(id, r && r.width ? { fromRect: r } : {}); return true; } } catch (e) { warn('open sheet', e); }
    return false;
  }
  function engravingTab() {
    close();
    try { if (typeof root.setMode === 'function') root.setMode('engrave'); else if (root.CN && root.CN.setMode) root.CN.setMode('engrave'); if (root.Engrave && root.Engrave.render) root.Engrave.render(); return true; } catch (e) { warn('engraving tab', e); return false; }
  }
  let comeBackTimer = 0, onClose = null;
  function handOver(el, run) {
    const p = st.panel; if (!p) return;
    st.handed = true; st.back = { k: el.closest('.lisBlk') && el.closest('.lisBlk').dataset.k, id: st.id };
    p.classList.add('handed'); p.setAttribute('inert', '');
    let ok = false;
    try { ok = run() !== false; } catch (e) { warn('hand-off', e); }
    if (!ok) { comeBack(); return; }
    const settle = () => { clearTimeout(comeBackTimer); comeBackTimer = setTimeout(() => { if (st.open && st.handed && !doc.querySelector('dialog[open]')) comeBack(); }, 60); };
    if (onClose) doc.removeEventListener('close', onClose, true);
    onClose = () => settle();
    doc.addEventListener('close', onClose, true);
    clearTimeout(comeBackTimer);
    comeBackTimer = setTimeout(() => { if (st.open && st.handed && !doc.querySelector('dialog[open]')) comeBack(); }, 2500);   // (nothing opened: the hand-off failed quietly)
  }
  function comeBack() {
    clearTimeout(comeBackTimer); comeBackTimer = 0;
    if (onClose) { doc.removeEventListener('close', onClose, true); onClose = null; }
    if (!st.open || !st.handed || !st.panel) return;
    const back = st.back; st.handed = false; st.back = null;
    const p = st.panel;
    p.classList.remove('handed'); p.removeAttribute('inert');
    const a = findAnchor();
    if (!a) { close(); return; }
    if (a !== st.anchor) rebind(a);
    const feed = feedOf(st.id, a), m = feed && model(feed, { step: st.step });
    if (!m || (!m.own && !m.count)) { close(); return; }
    mark(true);
    st.sig = sigOf(m, st); draw(m, false); place();
    anim(p, reduced() ? [{ opacity: 0 }, { opacity: 1 }] : [{ opacity: 0, transform: 'scale(.97)' }, { opacity: 1, transform: 'none' }], { duration: reduced() ? 90 : 420, easing: EASE });
    // focus returns to the row that was used (or the one now in its place)
    const row = back && st.body.querySelector(`.lisBlk[data-k="${String(back.k || '').replace(/"/g, '')}"] .lisGo`);
    (row || st.body.querySelector('.lisGo') || p).focus({ preventScroll: true });
  }
  function go(el) {
    const kind = el.dataset.go, id = el.dataset.id, pool = el.dataset.pool;
    if (kind === 'engraving') { engravingTab(); return; }
    if (kind === 'order') handOver(el, () => openOrder(el, id));
    else if (kind === 'piece') handOver(el, () => openOrder(el, id, pool ? { poolId: pool } : {}));
    else if (kind === 'sheet') handOver(el, () => openSheet(el, id));
  }

  /* ═══ events: one set for the page, on the capture phase so a card that stops a click never hides the '!' from here ═══ */
  doc.addEventListener('click', ev => {
    const t = ev.target && ev.target.closest ? ev.target : null; if (!t) return;
    const b = t.closest('[data-issues-open]');
    if (b) { ev.preventDefault(); ev.stopPropagation(); toggle(b); return; }
    if (!st.open || !st.panel || !st.panel.contains(t)) return;
    const fold = t.closest('[data-fold]');
    if (fold) { ev.preventDefault(); st.fold.set(fold.dataset.fold, fold.getAttribute('aria-expanded') !== 'true'); st.sig = ''; refresh(); return; }
    const more = t.closest('[data-more]');
    if (more) { ev.preventDefault(); const k = more.dataset.more; if (st.more.has(k)) st.more.delete(k); else st.more.add(k); st.sig = ''; refresh(); return; }
    const g = t.closest('.lisGo');
    if (g) { ev.preventDefault(); go(g); }
  }, true);
  doc.addEventListener('pointerdown', ev => {
    const t = ev.target && ev.target.closest ? ev.target : null; if (!t) return;
    if (t.closest('[data-issues-open]')) { ev.stopPropagation(); return; }   // (the '!' never starts a drag of its card)
    if (st.open && st.panel && !st.handed && !st.panel.contains(t)) close();
  }, true);
  doc.addEventListener('keydown', ev => {
    if (!st.open || !st.panel || st.handed) return;
    if (doc.querySelector('dialog[open]')) return;
    if (ev.key === 'Escape') { ev.preventDefault(); ev.stopPropagation(); close({ focus: true }); return; }
    const inside = st.panel.contains(doc.activeElement);
    const items = [...st.panel.querySelectorAll('.lisGo,.lisMore,.lisGroup')];
    if (!items.length) return;
    const i = items.indexOf(doc.activeElement);
    if (ev.key === 'Tab' && inside) {
      ev.preventDefault(); items[(i + (ev.shiftKey ? items.length - 1 : 1)) % items.length].focus();
    } else if (inside && (ev.key === 'ArrowDown' || ev.key === 'ArrowUp' || ev.key === 'Home' || ev.key === 'End')) {
      ev.preventDefault();
      const to = ev.key === 'Home' ? 0 : ev.key === 'End' ? items.length - 1 : (i + (ev.key === 'ArrowDown' ? 1 : -1) + items.length) % items.length;
      items[to].focus(); items[to].scrollIntoView && items[to].scrollIntoView({ block: 'nearest' });
    }
  }, true);
  let frame = 0;
  const again = () => { if (!st.open || frame) return; frame = root.requestAnimationFrame ? root.requestAnimationFrame(() => { frame = 0; if (st.open && !st.handed) place(); }) : 0; };
  root.addEventListener('resize', again);
  doc.addEventListener('scroll', again, true);

  /** what the open panel lists, folded groups included (a stable read for tests): [{ step, key, orderId?, sheetId? }] */
  function listed() {
    if (!st.open || !st.panel) return [];
    const feed = feedOf(st.id, st.anchor), m = feed && model(feed, { step: st.step }), out = [];
    if (!m) return out;
    if (m.own) out.push({ step: m.own.step, key: m.own.key });
    for (const o of m.orders) out.push({ step: 'orders', key: o.key, orderId: o.orderId });
    for (const x of m.sheets) out.push({ step: 'laser', key: x.key, sheetId: x.id });
    return out;
  }
  root.LibraryIssues = { open, close, refresh, listed, isOpen: () => st.open, model, adapt, current: () => st.open ? { id: st.id, step: st.step, handed: st.handed } : null };
})(typeof self !== 'undefined' ? self : this);
