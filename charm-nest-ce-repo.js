/* charm-nest-ce-repo.js — THE CHARM EDITOR'S "UPDATE REPOSITORY CHARM" (REPOEDIT; Paul, 17 Nov 2026).
 *
 *   Paul: "inside the Charm Editor we need to give users an option to update the Charm repository for a given charm. I need an elegant and practical solution ... this needs to be a separate
 *   option from the blue save button ... users should also be able to open the charm from the charm repository inside the charm editor then make an update to that charm and save those updates
 *   to the charm repository outside of any existing orders." · "Please don't put it beside the blue save button. This is separate. It needs to be more around the button that opens the
 *   repository" · "It's already crowded in the panel, so maybe you can be creative ... make sure that the charm that is opened the SKU of that charm wherever it came from either from within the
 *   actual application or opened inside the charm editor from the repository it has to be matched to the SKU for that charm so then when it gets updated, it's always the correct one that
 *   gets updated" · "You must test thoroughly that cloud gets updated. The charm repository gets updated all other computers get updated."
 *
 * WHERE: one small button in the editor's head, right beside the Library button (the button that opens the repository): "Repository". It is not in the Library panel and not beside Save. It
 * carries a dot once the drawing differs from the one opened. Pressed, it opens a small card under itself.
 *
 * WHICH CHARM (the rule: no exact match = no match): the editor is told, by whoever opened it, the ONE repository entry its drawing came from (CharmEditor.open opts.repo):
 *     { kind: "order" | "repository", sku, size, charmHash (what the repository draws for that SKU and size as it was opened), piece?, order?, why? }
 *   · a piece of an order (charm-nest-bridge.js openCharmEditorOnce): the SKU and size its pool row was made from, only when the piece holds a hash the repository's drawing goes by NOW;
 *   · a charm opened from the charm library (charm-nest-ce-masters.js masterStart): that entry and size.
 *   Anything else (a custom design, a pair, a piece cut from an older drawing, a saved design opened again, a new design) has `why` or no repo at all: the button says why and updates nothing.
 *   The SKU is never typed and never picked here: it is the one the charm came with. The server writes that one entry and refuses when it no longer draws what was opened (masterEditorPut).
 *
 * WHAT IS SAVED: the FRONT as it stands in the editor now, every part except the order's own engraving words (source "engrave") and the back's guide: the charm, not the order. It is turned
 * into the charm's own points (an order's piece is shown turned upright: its face says how, FaceDoc.toCharm), written and read back as any design file is (CharmNestCustomPool.pdfOf / readOf: it
 * must read as ONE closed charm), and handed to Master.replaceDrawing, which builds the per-SKU file, reads that back too, uploads and commits.
 *
 * AFTER: every computer's library takes the new drawing; the open pieces cut from the old one are made again in place by the computer that carries the run (the choice on the card; a cut sheet
 * never changes). An order's piece open here is one of them, so the editor offers to close: its work is in the repository now and its piece is being made again from it.
 *
 * UMD: window.CharmNestCeRepo in the page; module.exports in node (the pure parts: identity words, the mapping of a face to the charm's points). */
(function (root, factory) {
  const api = factory(root);
  if (typeof module === 'object' && module.exports) module.exports = api;
  if (root && root.document) root.CharmNestCeRepo = api;
})(typeof self !== 'undefined' ? self : typeof globalThis !== 'undefined' ? globalThis : this, function (root) {
  'use strict';
  const doc = root && root.document ? root.document : null;
  const MM = 25.4 / 72;
  const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const up = s => String(s == null ? '' : s).trim().toUpperCase();
  const mm = pt => { const v = (+pt || 0) * MM; return (Math.round(v * 10) / 10).toFixed(1); };
  /* TRUESIZE (Paul, 22 Nov 2026: "I had to set the size to 8.1 mm in order for the system to recognize it as 9 mm"): the size a person reads is the charm as drawn, the very W and H of the editor
     and of Illustrator (drawnPt); widthPt / heightPt are the reader's frame (the shape plus half its line and 1 pt on every side). An entry saved before drawnPt was kept is read without that 1 pt. */
  const drawnOf = g => { if (!g) return null; const d = Array.isArray(g.drawnPt) && g.drawnPt.length === 2 ? g.drawnPt : null; if (d) return d; const w = +g.widthPt || 0, h = +g.heightPt || 0; return w > 0 && h > 0 ? [w > 2 ? w - 2 : w, h > 2 ? h - 2 : h] : null; };
  const sizeWords = g => { const d = drawnOf(g); return d ? ` · ${mm(d[0])} × ${mm(d[1])} mm` : ''; };
  const ap = (m, p) => [m[0] * p[0] + m[2] * p[1] + m[4], m[1] * p[0] + m[3] * p[1] + m[5]];

  /* ── the pure parts ── */
  /** What the editor was told about the charm's repository entry, made plain. → { ok, kind, sku, size, charmHash, piece, order, why } */
  function identity(repo) {
    const r = repo && typeof repo === 'object' ? repo : null;
    if (!r) return { ok: false, kind: '', sku: '', size: '', charmHash: '', why: 'This drawing did not come from a charm in the repository.' };
    const sku = up(r.sku), size = r.size ? up(r.size) : '', hash = String(r.charmHash || '');
    const why = r.why ? String(r.why) : !sku ? 'This drawing did not come from a charm in the repository.' : !hash ? `${sku}: the repository's drawing could not be told.` : '';
    return { ok: !why, kind: r.kind === 'order' ? 'order' : 'repository', sku, size, charmHash: hash, piece: String(r.piece || ''), order: String(r.order || ''), why };
  }
  /** The parts of a front that are the CHARM: not the order's engraving words, not a guide, not hidden engraving. */
  const charmParts = items => (items || []).filter(it => it && !it.guide && it.source !== 'engrave');
  /** Leaves (a face's parts as drawn, points, y down) put into the charm's own points by the face's toCharm, and handed back y down again (the file writer turns y up itself). */
  function leavesToCharm(leaves, toCharm) {
    if (!Array.isArray(toCharm) || toCharm.length !== 6) return leaves;
    const f = p => { const q = ap(toCharm, p); return [q[0], -q[1]]; };
    return leaves.map(l => Object.assign({}, l, { subpaths: (l.subpaths || []).map(sp => sp.map(o => (o[0] === 'h' ? ['h'] : [o[0]].concat(o.slice(1).map(f))))) }));
  }
  const WORDS = {
    title: 'Charm repository',
    order: 'Update this charm in the repository with the drawing as it is here. The order\'s engraving is not part of it.',
    repository: 'Save this drawing as the repository\'s charm. Save (the blue button) keeps a separate design and never changes the repository.',
    remake: 'Every open order of this charm is made again with it, each piece in its own place on its own sheet.',
    cut: 'A piece on a sheet marked laser cut never changes.',
    go: 'Update repository charm'
  };

  if (!doc) return { identity, charmParts, leavesToCharm, WORDS, version: 1 };

  /* ── the page ── */
  const CSS = `
.ce-repo{ position:relative; flex:none; display:inline-flex; align-items:center; }
.ce-repo__b{ display:inline-flex; align-items:center; gap:5px; height:24px; padding:0 8px 0 6px; border:1px solid var(--ai-input-border,#6b6b6b); border-radius:12px; background:transparent; color:var(--ai-text,#efefef); cursor:pointer; font:600 11px/1 var(--ai-font,system-ui); white-space:nowrap; }
.ce-repo__b:hover,.ce-repo__b[aria-expanded="true"]{ background:var(--ai-hover,#5e5e5e); }
.ce-repo__b svg{ width:15px; height:15px; flex:none; }
.ce-repo__b[data-off="1"]{ opacity:.55; border-style:dashed; }
.ce-repo__dot{ width:7px; height:7px; border-radius:50%; background:#e0a52b; display:none; }
.ce-repo[data-dirty="1"] .ce-repo__dot{ display:inline-block; }
.ce-repo[data-done="1"] .ce-repo__dot{ display:inline-block; background:#3fb873; }
.ce-repo__card{ position:fixed; top:34px; left:8px; z-index:60; width:336px; max-width:calc(100vw - 16px); background:var(--ai-menu,#3b3b3b); color:var(--ai-text,#efefef); border:1px solid var(--ai-menu-border,#222); border-radius:6px; box-shadow:0 10px 30px rgba(0,0,0,.45); padding:12px 14px 14px; font:400 12px/1.4 var(--ai-font,system-ui); white-space:normal; }
.ce-repo__k{ font-size:10px; letter-spacing:.08em; text-transform:uppercase; color:var(--ai-text-dim,#c4c4c4); }
.ce-repo__sku{ font-size:14px; font-weight:700; margin:2px 0 0; overflow-wrap:anywhere; }
.ce-repo__file{ font-size:11px; color:var(--ai-text-dim,#c4c4c4); overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
.ce-repo__cmp{ display:grid; grid-template-columns:1fr 18px 1fr; align-items:center; gap:6px; margin:10px 0 8px; }
.ce-repo__pic{ background:#fff; border-radius:4px; height:104px; display:flex; align-items:center; justify-content:center; overflow:hidden; color:#666; font-size:11px; text-align:center; padding:4px; }
.ce-repo__pic img{ max-width:100%; max-height:96px; }
.ce-repo__cap{ font-size:10.5px; color:var(--ai-text-dim,#c4c4c4); text-align:center; margin-top:3px; }
.ce-repo__arrow{ text-align:center; color:var(--ai-text-dim,#c4c4c4); font-size:15px; }
.ce-repo__say{ margin:6px 0; }
.ce-repo__note{ font-size:11px; color:var(--ai-text-dim,#c4c4c4); margin:6px 0 0; }
.ce-repo__why{ margin:8px 0 0; padding:8px 9px; border-radius:4px; background:rgba(224,165,43,.14); border:1px solid rgba(224,165,43,.5); }
.ce-repo__bad{ margin:8px 0 0; padding:8px 9px; border-radius:4px; background:rgba(222,84,84,.16); border:1px solid rgba(222,84,84,.6); }
.ce-repo__ok{ margin:8px 0 0; padding:8px 9px; border-radius:4px; background:rgba(63,184,115,.16); border:1px solid rgba(63,184,115,.6); }
.ce-repo__go{ margin-top:10px; width:100%; height:28px; border-radius:14px; border:1px solid var(--ai-accent,#378ef0); background:var(--ai-accent,#378ef0); color:var(--ai-accent-text,#fff); font:600 12px/1 var(--ai-font,system-ui); cursor:pointer; }
.ce-repo__go[disabled]{ opacity:.5; cursor:default; }
.ce-repo__go--plain{ background:transparent; color:var(--ai-text,#efefef); border-color:var(--ai-input-border,#6b6b6b); }
.ce-repo__steps{ margin:8px 0 0; padding:0; list-style:none; font-size:11.5px; }
.ce-repo__steps li{ padding:1px 0; color:var(--ai-text-dim,#c4c4c4); }
.ce-repo__steps li:last-child{ color:var(--ai-text,#efefef); font-weight:600; }
.ce-repo__bar{ height:4px; border-radius:2px; background:rgba(255,255,255,.15); overflow:hidden; margin-top:8px; }
.ce-repo__bar i{ display:block; height:100%; width:0; background:var(--ai-accent,#378ef0); transition:width .35s ease; }
`;
  const ICON = '<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.3" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><ellipse cx="8" cy="3.6" rx="5" ry="1.9"/><path d="M3 3.6v4.2c0 1 2.2 1.9 5 1.9M13 3.6v2.6"/><path d="M3 7.8V12c0 1 2.2 1.9 5 1.9"/><path d="M12 14.2V9.4M9.9 11.3 12 9.2l2.1 2.1"/></svg>';
  let cssIn = false;
  const css = () => { if (cssIn) return; cssIn = true; const s = doc.createElement('style'); s.textContent = CSS; doc.head.appendChild(s); };
  const log = (kind, data) => { try { if (root.CNDesignLog && typeof root.CNDesignLog.event === 'function') root.CNDesignLog.event(kind, data); } catch (_) { /* the log only */ } };
  const whoNow = () => { try { const E = root.CNEmployee; return (E && typeof E.name === 'function' && E.name()) || ''; } catch (_) { return ''; } };

  /** The entry the page holds for the identity, and the size's own geometry. */
  function entryOf(id) {
    const Ms = root.Master, Pl = root.Pool; if (!Ms || !id.sku) return { entry: null, geom: null };
    const entry = Ms.entryFor(id.sku) || null; if (!entry || up(entry.sku) !== id.sku) return { entry: null, geom: null };
    let geom = null; try { geom = Pl && Pl.sizeEntry ? Pl.sizeEntry(entry, id.size || null) : entry; } catch (_) { geom = null; }
    if (id.size) geom = (entry.sizes && entry.sizes[id.size]) || null;
    return { entry, geom: geom || entry };
  }
  /* SAVETHENREPO (Paul, 24 Nov 2026, the Charm Editor test: Save, then Update repository, was refused: "nothing has changed since this drawing was opened"): a piece saved with its own front drawing differs from the repository's */
  function ownEdit(id) { try { const pid = id && id.piece ? String(id.piece) : ''; if (!pid) return false; const r = root.B && root.B.pool && root.B.pool.rows && root.B.pool.rows.get(pid); return !!((r && r.artEdit && r.artEdit.front) || (root.CharmNestSavedCharms && root.CharmNestSavedCharms.has(pid))); } catch (_) { return false; } }
  /** How many live pieces of this SKU (and size) the run holds on this page. */
  function piecesOf(id) {
    let n = 0;
    try { const B = root.B, rows = B && B.pool && B.pool.rows; if (!rows) return 0;
      for (const p of rows.values()) { if (!p || p.custom || !p.charmHash || up(p.sku) !== id.sku) continue; if (['superseded', 'gone', 'removed', 'cancelled', 'shipped'].includes(String(p.state))) continue; if (id.size && up(p.size) !== id.size) continue; n++; } } catch (_) { n = 0; }
    return n;
  }
  /** The editor's front as it stands, as a charm read from its own file: → { charm, parsed, thumb, widthPt, heightPt }. Throws with words a person can read. */
  async function drawingNow(cur, id) {
    const CM = root.CharmNestCeMasters, CP = root.CharmNestCustomPool, D = root.CharmNestDesigns, P = root.CharmNestPDF;
    if (!CM || !CP || !D || !P || typeof D.leaves !== 'function') throw new Error('the page has not finished loading: reload it');
    const res = cur.result(), front = res && res.faces && res.faces.front;
    if (!front || !Array.isArray(front.items)) throw new Error('the front of the charm is not open here');
    const items = charmParts(front.items);
    if (!items.length) throw new Error('nothing of the charm is left on its front');
    const face = Object.assign({}, front, { items });
    const design = CM.model.designOf({ faces: { front: face } }, { name: id.sku, faces: { front: face }, source: { kind: 'master', sku: id.sku } }, whoNow());
    let d2 = { name: id.sku, faces: { front: design.faces.front } };
    if (Array.isArray(front.toCharm)) {
      const lv = leavesToCharm(D.leaves({ front: design.faces.front }).front || [], front.toCharm);
      d2 = { name: id.sku, faces: { front: { face: 'front', unitPt: true, frame: design.faces.front.frame, items: lv.map((l, i) => ({ id: String(l.id == null ? i : l.id), kind: 'path', role: l.role, subpaths: l.subpaths, stroke: l.stroke, fill: l.fill, lwPt: l.lwPt == null ? undefined : l.lwPt, evenOdd: !!l.even, tx: 0, ty: 0, sx: 1, sy: 1, rot: 0 })) } } };
    }
    const L = CP.leavesOf(d2);
    if (L.skipped && L.skipped.length) throw new Error(`${L.skipped.length} part${L.skipped.length === 1 ? '' : 's'} of the drawing cannot be written (words with no outlines)`);
    const bytes = await CP.pdfOf(L, id.sku), read = await CP.readOf(bytes, id.sku + '.pdf'), c = read.charm;
    let thumb = c.thumb || ''; if (!thumb && typeof P.thumbnail === 'function') { try { thumb = await P.thumbnail(c, 168); } catch (_) { thumb = ''; } }
    return { charm: c, parsed: read.parsed, thumb, widthPt: c.widthPt, heightPt: c.heightPt, drawnPt: c.drawnPt || null };
  }

  /* one control per open workspace */
  function mount(ctx) {
    const rootEl = ctx && ctx.root; if (!rootEl) return;
    const head = rootEl.querySelector('.ce-head'); if (!head || head.querySelector('.ce-repo')) return;
    const id = identity(ctx.repo);
    css();
    const wrap = doc.createElement('div'); wrap.className = 'ce-repo';
    wrap.innerHTML = `<button type="button" class="ce-repo__b" aria-haspopup="dialog" aria-expanded="false"${id.ok ? '' : ' data-off="1"'} title="${esc(id.ok ? `Update ${id.sku}${id.size ? ' · ' + id.size : ''} in the charm repository with this drawing` : id.why)}">${ICON}<span>Update repository</span><i class="ce-repo__dot"></i></button>`;   // (the label says what it does: "Repository" alone read as a second Library button; greyed and dashed it still says why in its tip)
    const lib = head.querySelector('[data-xpanel="library"]'), hist = head.querySelector('.ce-hist');
    if (lib) lib.insertAdjacentElement('afterend', wrap); else if (hist) head.insertBefore(wrap, hist); else head.appendChild(wrap);
    const btn = wrap.querySelector('.ce-repo__b');
    const st = { id, open: false, busy: false, made: null, err: '', done: null, steps: [] };
    wrap._cnRepo = st;
    let card = null;
    const close = () => { st.open = false; btn.setAttribute('aria-expanded', 'false'); if (card) { card.remove(); card = null; } };
    const picOf = url => { const M = root.Master; if (!url) return '<span>no picture</span>'; let src = url; try { if (!/^data:/.test(url) && root.CharmNestAssets && typeof root.CharmNestAssets.url === 'function') src = root.CharmNestAssets.url(url); } catch (_) { src = url; } return `<img alt="" src="${esc(src)}">`; };
    function paint() {
      if (!card) return;
      const { entry, geom } = entryOf(st.id), name = `${esc(st.id.sku)}${st.id.size ? ' · ' + esc(st.id.size) : ''}`;
      let h = `<div class="ce-repo__k">${esc(WORDS.title)}</div>`;
      if (!st.id.ok) { h += `<div class="ce-repo__why">${esc(st.id.why)}</div><div class="ce-repo__note">Open a charm from the Library (Charm library) to edit the repository's own drawing.</div>`; card.innerHTML = h; return; }
      h += `<div class="ce-repo__sku">${name}</div><div class="ce-repo__file">${esc(entry && entry.masterName ? 'from ' + entry.masterName : entry ? '' : 'not in the library this page holds')}${entry && entry.edited && entry.edited.by ? ` · last updated here by ${esc(entry.edited.by)}` : ''}</div>`;
      const thumbNow = entry ? (geom && geom.thumbUrl) || entry.thumbUrl || '' : '';
      h += `<div class="ce-repo__cmp"><div><div class="ce-repo__pic">${picOf(thumbNow)}</div><div class="ce-repo__cap">In the repository${sizeWords(geom)}</div></div><div class="ce-repo__arrow">→</div>`
        + `<div><div class="ce-repo__pic" data-new>${st.made ? picOf(st.made.thumb) : st.err ? '<span>—</span>' : '<span>Reading your drawing…</span>'}</div><div class="ce-repo__cap">Your drawing${st.made ? sizeWords(st.made) : ''}</div></div></div>`;
      if (st.done) {
        h += `<div class="ce-repo__ok"><b>${st.done.same ? 'Nothing to update' : 'Repository updated'}</b><br>${esc(st.done.text)}${st.done.same ? '' : `<br>${esc(st.orders != null ? st.orders : st.steps[st.steps.length - 1] || '')}`}</div>`;
        if (st.done.closeOffer) h += `<button type="button" class="ce-repo__go" data-go="close">Close the editor</button><button type="button" class="ce-repo__go ce-repo__go--plain" data-go="stay">Keep editing</button>`;
        card.innerHTML = h; wire(); return;
      }
      h += `<div class="ce-repo__say">${esc(st.id.kind === 'order' ? WORDS.order : WORDS.repository)}</div>`;
      const n = piecesOf(st.id);
      h += `<div class="ce-repo__note">${esc(WORDS.remake)}${n ? ` (${n} pieces now)` : ''} ${esc(WORDS.cut)}</div>`;
      if (st.err) h += `<div class="ce-repo__bad">${esc(st.err)}</div>`;
      if (st.busy) h += `<ul class="ce-repo__steps">${st.steps.slice(-4).map(t => `<li>${esc(t)}</li>`).join('')}</ul><div class="ce-repo__bar"><i style="width:${Math.min(96, 12 + st.steps.length * 16)}%"></i></div>`;
      const who = whoNow();
      h += `<button type="button" class="ce-repo__go" data-go="update"${st.busy || !st.made || !who ? ' disabled' : ''}>${st.busy ? 'Updating…' : esc(WORDS.go)}</button>`;
      if (!who) h += `<div class="ce-repo__note">Sign in to update the repository.</div>`;
      card.innerHTML = h; wire();
    }
    function wire() {
      const go = card.querySelector('[data-go="update"]'); if (go) go.addEventListener('click', () => { update(); });
      const cl = card.querySelector('[data-go="close"]'); if (cl) cl.addEventListener('click', () => { close(); closeEditor(rootEl); });
      const stay = card.querySelector('[data-go="stay"]'); if (stay) stay.addEventListener('click', () => { st.done = null; close(); });
    }
    let reading = 0, readT = 0;
    async function read() {
      const my = ++reading; st.made = null; st.err = ''; paint();
      try { const cur = root.CharmEditor && root.CharmEditor.current; if (!cur) throw new Error('the editor is closed'); const made = await drawingNow(cur, st.id); if (my !== reading) return; st.made = made; }
      catch (e) { if (my !== reading) return; st.err = 'This drawing cannot be the repository\'s charm yet · ' + String((e && e.message) || e); }
      paint();
    }
    async function update() {
      if (st.busy || !st.made) return;
      // (an untouched drawing is not written: the file is written again by this page's writer, so even an unchanged drawing would come back with another hash and every open piece would be made again for nothing)
      if (wrap.dataset.dirty !== '1' && !ownEdit(st.id)) { st.err = 'Not updated · nothing has changed since this drawing was opened here (or last updated): change the drawing first'; paint(); return; }
      const Ms = root.Master; if (!Ms || typeof Ms.replaceDrawing !== 'function') { st.err = 'The page has not finished loading: reload it'; paint(); return; }
      st.busy = true; st.err = ''; st.steps = ['Checking the drawing…']; paint();
      const say = t => { if (!t) return; st.steps.push(String(t)); paint(); };
      let r = null;
      try {
        // the drawing as it stands this moment (a change made while the card was open is in it)
        const cur = root.CharmEditor && root.CharmEditor.current; if (!cur) throw new Error('the editor is closed');
        st.made = await drawingNow(cur, st.id);
        r = await Ms.replaceDrawing({ sku: st.id.sku, size: st.id.size || '', expect: st.id.charmHash, charm: st.made.charm, parsed: st.made.parsed, by: whoNow(), piece: st.id.piece, order: st.id.order }, { say, orders: a => { st.orders = a ? (a.ok ? a.text : `The open orders: ${a.text || 'the cloud did not answer'}`) : ''; st.ordersBad = !!(a && !a.ok); if (a && !a.ok) { try { root.toast(`${st.id.sku}: its open orders were not all made again · ${a.text || 'no answer'}`, 'bad', 12000); } catch (_) { /* the card says it */ } } paint(); } });
      } catch (e) { r = { ok: false, reason: 'error', text: String((e && e.message) || e) }; }
      st.busy = false;
      if (!r || !r.ok) { st.err = `Not updated · ${(r && r.text) || 'the repository did not answer'}`; log('repoUpdateRefused', { sku: st.id.sku, size: st.id.size || null, reason: r && r.reason, by: whoNow() }); paint(); return; }
      // the next update is measured from what the repository draws now
      try { const g = entryOf(st.id).geom; if (g && g.charmHash) st.id.charmHash = String(g.charmHash); } catch (_) { /* kept */ }
      /* REPOLISTING (Paul, 22 Nov 2026: "This type of update must also be the case for all listings/orders updated in the Charm Editor when the charm repo is being updated in the
         Charm Editor"): updated from an order's piece, the charm is the one the order's LISTING is cut as from now on: its link is saved (Etsy is not changed) and its other open
         orders are made again with it (charm-nest-bridge.js listingFollow) */
      let lf = null;
      if (st.id.kind === 'order' && st.id.order && typeof root.CNListingFollow === 'function') {
        st.busy = true; paint();
        try { lf = await root.CNListingFollow({ order: st.id.order, piece: st.id.piece, sku: st.id.sku, by: whoNow(), say }); } catch (e) { lf = { ok: false, why: String((e && e.message) || e) }; }
        st.busy = false;
        log('repoListing', { sku: st.id.sku, order: st.id.order, ok: !!(lf && lf.ok), lid: lf && lf.lid, from: lf && lf.from, n: lf && lf.n, why: lf && !lf.ok ? lf.why : null });
      }
      wrap.dataset.dirty = '0'; wrap.dataset.done = '1'; base = sigOf();
      const order = st.id.kind === 'order' && !r.same;
      st.done = { same: !!r.same, closeOffer: order,
        text: r.same ? `${st.id.sku} already draws exactly this in the repository.`
          : `${st.id.sku}${st.id.size ? ' · ' + st.id.size : ''} now draws this on every computer. Every open order of it is being made again, each piece in its own place.${order ? ' This order\'s piece is one of them: its work is in the repository now, so the editor can close.' : ''}` };
      if (lf) st.done.text += lf.ok ? ` ${lf.text || ''}`.replace(/\s+$/, '') + '.' : ` The listing was not changed: ${lf.why}.`;
      log('repoUpdate', { sku: st.id.sku, size: st.id.size || null, by: whoNow(), from: r.was || null, to: (r.entry && r.entry.edited && r.entry.edited.to) || null, kind: st.id.kind, order: st.id.order || null, same: !!r.same });
      try { if (typeof root.toast === 'function') root.toast(r.same ? `${st.id.sku}: nothing to update` : `${st.id.sku} updated in the charm repository`, 'good', 5000); } catch (_) { /* the card says it */ }
      paint();
    }
    btn.addEventListener('click', ev => {
      ev.stopPropagation();
      if (st.open) { close(); return; }
      st.open = true; btn.setAttribute('aria-expanded', 'true');
      card = doc.createElement('div'); card.className = 'ce-repo__card'; card.setAttribute('role', 'dialog'); card.setAttribute('aria-label', 'Charm repository');
      card.addEventListener('pointerdown', e => e.stopPropagation()); card.addEventListener('keydown', e => { if (e.key === 'Escape') { e.stopPropagation(); close(); btn.focus(); } else e.stopPropagation(); });
      // (the card is put in the workspace's own root, not in the head: the head is a layer under the right-hand dock (Properties, Layers), which covered most of the card. Placed under the button, kept inside the window)
      rootEl.appendChild(card);
      try { const b = btn.getBoundingClientRect(), vw = root.innerWidth || 1200, w = Math.min(340, vw - 16); card.style.top = Math.round(b.bottom + 6) + 'px'; card.style.left = Math.max(8, Math.min(Math.round(b.left), vw - w - 8)) + 'px'; } catch (_) { /* as placed */ }
      paint(); if (st.id.ok && !st.done) read();
    });
    const away = ev => { if (st.open && !st.busy && !wrap.contains(ev.target) && !(card && card.contains(ev.target))) close(); };
    doc.addEventListener('pointerdown', away, true);
    // the dot: the work differs from what was opened (or from the last update)
    const sigOf = () => { try { const cur = root.CharmEditor && root.CharmEditor.current; return cur && typeof cur.dirty === 'function' ? (cur.dirty() ? 'd' + (st.tick || 0) : 'c') : ''; } catch (_) { return ''; } };
    let base = 'c';
    st.onChange = () => { st.tick = (st.tick || 0) + 1; const s = sigOf(); wrap.dataset.dirty = st.id.ok && s !== base && s !== 'c' ? '1' : '0'; if (wrap.dataset.dirty === '1') {
        wrap.dataset.done = '0';
        // (a change after an update, or while the card is open: the card shows the drawing as it is now, and offers the next update, which is measured from what the repository draws now)
        if (!st.busy) { st.done = null; if (card) { clearTimeout(readT); readT = setTimeout(() => { if (card && !st.busy) read(); }, 350); } else st.err = ''; }
      } };
    st.unmount = () => { try { doc.removeEventListener('pointerdown', away, true); } catch (_) { /* gone */ } close(); };
    live = st;
  }
  /** The editor closed as its Cancel closes it; the question about unsaved work is answered "Discard" (the person asked to close after the repository took the work). */
  function closeEditor(rootEl) {
    try {
      const b = rootEl.querySelector('.ce-actions [data-act="cancel"]'); if (!b) return;
      b.click();
      setTimeout(() => { try { const ask = rootEl.querySelector('.ce-ask'); if (!ask) return; const d = [...ask.querySelectorAll('button')].find(x => /discard/i.test(x.textContent || '')); if (d) d.click(); } catch (_) { /* the person answers */ } }, 60);
    } catch (_) { /* the person closes it */ }
  }
  let live = null;
  const CE = root.CharmEditor || (root.CharmEditor = {});
  (CE.onOpen || (CE.onOpen = [])).push(ctx => { try { mount(ctx); } catch (e) { console.warn('[ce-repo] not mounted:', e && e.message); } });
  (CE.onChange || (CE.onChange = [])).push(() => { try { if (live && live.onChange) live.onChange(); } catch (_) { /* the dot only */ } });
  (CE.onClose || (CE.onClose = [])).push(() => { try { if (live && live.unmount) live.unmount(); } catch (_) { /* gone */ } live = null; });

  return { identity, charmParts, leavesToCharm, drawingNow, WORDS, _live: () => live, version: 1 };
});
