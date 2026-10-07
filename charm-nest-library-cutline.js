/* LibraryCutLine: what the Library does when a PARTIAL sheet is dragged (or moved with "Move to…") to Laser cutting
   (Paul, 7 Oct 2026, "Add the exact same Cut Sheet functionality to both the 10k Gold and 14k Gold sheet options ... just like the
   RG 14/20 when a user drags a given partial/unfinished sheet to the laser cutting station process than the system must prompt the
   user with the same type of popup ... informing the user of the automatic green dash line that the system will need to generate to
   complete a partial sheet. The user should have the option to not proceed. If the user proceeds the green cut dash line should be
   auto generated and clearly shown to the user visually on the sheet and then the sheet should visually animate and cleanly fly to
   the desired process.").

   The hands are charm-nest-library-dnd.js (its cutLineGate calls this); the engine is charm-nest-flow.js (LibraryFlow.plan says a
   `roseLine` confirm is needed, LibraryFlow.cutLine does the Cut Sheet press). This file is what the person sees:

     LibraryCutLine.guess(item, to, here)   -> null | {ids, hits}   sync, from the page's own records: a sheet of a metal that gets a green line, not cut,
                                               with no line yet, going forward to Laser cutting (or Completed). Only a hint that the plan is worth reading
                                               first; the plan (LibraryFlow.plan) has the last word.
     LibraryCutLine.ask(item, confirm, {from, dest, fromName, what}) -> Promise<true|false|null>
                                               the window (SharedOrdersModal.ask: the very same component and look as the shared-orders window) that says the
                                               system will generate the green dash line that completes the partial sheet: how many charms, what share of the sheet,
                                               that a recorded cut is permanent, with a clear "Cancel" that changes nothing. true only from a real press on the
                                               yes button; null when the window cannot be shown here (the caller keeps its old way).
     LibraryCutLine.make({ids, el, run})    -> Promise<result>
                                               after the yes: a small labelled spinner on the sheet's own card preview while the line is calculated; the moment it
                                               exists the dashed teal line is revealed on that preview (numbered, dated, as the Nest tab draws it) and held for a
                                               moment so it is SEEN; then the result of run(onStep) is returned (the caller flies the card after it). A line that
                                               could not be made is said on the card in plain words.
     LibraryCutLine.busy(ids, text, {el, delay}) -> {stop()}   a labelled spinner on the card preview(s) while something is read
     LibraryCutLine.restore(root)           the lines shown this session come back on a card the Library draws again (its saved picture is made when the sheet is
                                               next saved; until then the card shows the line from here)
   It never writes: the only write is run()'s, which the caller makes after the person's yes. Nothing here throws. */
(function () {
  'use strict';
  if (window.LibraryCutLine) return;
  const W = window, doc = document;
  const CODE = { rose: 'RG', gold10k: '10K', gold14k: '14K', gold: 'GF', silver: 'SS' };
  const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const wait = ms => new Promise(r => setTimeout(r, ms));
  const plural = (n, one, many) => `${n} ${n === 1 ? one : many || one + 's'}`;
  const reduced = () => { try { return !!((W.Motion && W.Motion.reduced && W.Motion.reduced()) || (W.matchMedia && W.matchMedia('(prefers-reduced-motion: reduce)').matches)); } catch (_) { return false; } };
  const when = at => Number.isFinite(+at) && +at > 0 ? new Date(+at).toLocaleString(undefined, { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' }) : '';
  const REVEAL = 900, READ = 800, KEEP = 12;
  const KEPT = new Map();    // sheetId -> { src, tag }: the lines shown this session

  /* ── the page's own records (read only) ── */
  const rows = () => { try { return (W.CN && W.CN.S && W.CN.S.library && W.CN.S.library.rows) || []; } catch (_) { return []; } };
  const rowOf = id => rows().find(r => r && r.id === id) || null;
  const liveOf = id => { try { const all = ((W.CN && W.CN.allSheets && W.CN.allSheets()) || []).filter(s => s.sheetId === id); return all.find(s => !s.recalled) || all[0] || null; } catch (_) { return null; } };
  const cardOf = id => [...doc.querySelectorAll('#libBody .libCard[data-id]')].find(c => c.dataset.id === id) || null;
  const setCardOf = setId => [...doc.querySelectorAll('#libBody .setCard')].find(c => c._laserSet && c._laserSet.setId === setId) || null;
  const inSet = r => !!(r && r.setId && !r.draft && r.solidIncluded !== false);
  /** The sheets a move of this item carries: a sheet of a set of several moves with its set. */
  function sheetIdsOf(item) {
    const out = new Set();
    const addSet = setId => { const sc = setCardOf(setId); for (const id of (sc && sc._laserSheets) || []) out.add(id); for (const r of rows()) if (r.setId === setId && inSet(r)) out.add(r.id); };
    if (item.kind === 'set') addSet(item.id);
    else { out.add(item.id); const r = rowOf(item.id); if (inSet(r)) addSet(r.setId); }
    return [...out];
  }
  function guess(item, to, here) {
    try {
      if (!item || !to || here !== 'progress' || (to.area !== 'laser' && to.area !== 'completed')) return null;
      const needs = W.LibraryFlow && W.LibraryFlow.core && W.LibraryFlow.core.needsCutLine;
      if (typeof needs !== 'function') return null;
      const ids = sheetIdsOf(item), hits = ids.filter(id => { const r = rowOf(id) || liveOf(id); return !!r && !!needs(r); });
      return hits.length ? { ids, hits } : null;
    } catch (_) { return null; }
  }

  /* ── looks ── */
  const CSS = `
.libCard.clHas{position:relative}
.clOv{position:absolute;z-index:3;box-sizing:border-box;overflow:hidden;pointer-events:none}
.clBusy{display:flex;align-items:center;justify-content:center;gap:9px;padding:8px;text-align:center;border-radius:8px;background:color-mix(in srgb,var(--card,#fffefb) 84%,transparent);-webkit-backdrop-filter:blur(2px);backdrop-filter:blur(2px);color:var(--ink70,#5b554c);font:600 12px/1.35 var(--sans,system-ui,sans-serif);animation:clFade .2s ease both}
.clSpin{width:14px;height:14px;flex:none;box-sizing:border-box;border:2px solid var(--line,#e4ddd0);border-top-color:#008974;border-radius:50%;animation:clSpin .7s linear infinite}
.clLine img{position:absolute;left:0;top:0;width:100%;height:100%;box-sizing:border-box;object-fit:contain;display:block;clip-path:inset(0 100% 0 0);transition:clip-path ${REVEAL}ms cubic-bezier(.25,.7,.2,1)}
.clLine.on img{clip-path:inset(0 0 0 0)}
.clLine.still img{transition:none}
.clTag{position:absolute;left:8px;bottom:8px;display:inline-flex;align-items:center;gap:6px;max-width:calc(100% - 16px);padding:3px 10px 3px 8px;border-radius:999px;background:var(--card,#fffefb);border:1px solid #008974;color:#00695a;font:650 11px/1.3 var(--sans,system-ui,sans-serif);white-space:nowrap;box-shadow:0 2px 8px rgba(30,26,20,.12);opacity:0;transform:translateY(3px);transition:opacity .3s ease .5s,transform .3s ease .5s}
.clLine.on .clTag{opacity:1;transform:none}
.clLine.still .clTag{transition:none}
.clTag b{display:inline-grid;place-items:center;min-width:15px;height:15px;border-radius:50%;background:#008974;color:#fff;font:700 10px/1 var(--sans,system-ui,sans-serif)}
.clTag svg{width:12px;height:12px;flex:none;color:#008974}
.clErr{display:flex;align-items:center;justify-content:center;padding:10px 12px;text-align:center;border-radius:8px;background:color-mix(in srgb,var(--claySoft,#f4e3dc) 94%,transparent);border:1px solid var(--clay,#b0563f);color:#7a3321;font:600 12px/1.4 var(--sans,system-ui,sans-serif);pointer-events:auto;cursor:pointer;animation:clFade .2s ease both;text-wrap:pretty}
@keyframes clSpin{to{transform:rotate(360deg)}}
@keyframes clFade{from{opacity:0}}
@media (prefers-reduced-motion:reduce){.clSpin{animation-duration:1.6s}.clLine img,.clTag{transition:none!important}.clBusy,.clErr{animation:none}}
`;
  function css() { if (doc.getElementById('clCss') || !doc.head) return; const s = doc.createElement('style'); s.id = 'clCss'; s.textContent = CSS; doc.head.appendChild(s); }
  const TICK = '<svg viewBox="0 0 16 16" aria-hidden="true"><path d="M3.4 8.5l3 3 6.2-6.4" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>';

  /* ── overlays on a card's preview ── */
  /** The element an overlay sits over: the card's sheet picture, else the card, else what the caller gave. */
  const anchorOf = (id, el) => { const c = cardOf(id); return (c && (c.querySelector('img.pv') || c)) || el || null; };
  function place(anchor, cls) {
    const ov = doc.createElement('div'); ov.className = 'clOv ' + cls; ov.dataset.cutline = '';
    const host = anchor.closest && anchor.closest('.libCard'); if (host) host.classList.add('clHas');
    Object.assign(ov.style, { left: anchor.offsetLeft + 'px', top: anchor.offsetTop + 'px', width: anchor.offsetWidth + 'px', height: anchor.offsetHeight + 'px' });
    anchor.parentNode.insertBefore(ov, anchor.nextSibling);
    return ov;
  }
  function busy(ids, text, o) {
    o = o || {}; css();
    const ovs = []; let stopped = false;
    const show = () => {
      if (stopped) return;
      for (const id of ids || []) {
        try { const a = anchorOf(id, o.el); if (!a || !a.parentNode) continue; const ov = place(a, 'clBusy'); ov.setAttribute('role', 'status'); ov.innerHTML = `<i class="clSpin" aria-hidden="true"></i><span>${esc(text)}</span>`; ovs.push(ov); } catch (_) { /* no spinner here */ }
      }
    };
    const t = o.delay ? setTimeout(show, o.delay) : (show(), 0);
    return { stop() { stopped = true; clearTimeout(t); for (const ov of ovs.splice(0)) { try { ov.remove(); } catch (_) { /* gone */ } } } };
  }
  /** The sheet's own picture as it is now (a clean render, as the saved previews are), as an image. */
  function paint(sh, w) {
    try {
      const C = W.CN; if (!C || typeof C.paintPreview !== 'function' || typeof C.stockFor !== 'function') return null;
      const st = C.stockFor(sh.metal, sh), cv = doc.createElement('canvas');
      cv.width = w; cv.height = Math.max(20, Math.round(w * st.hPt / st.wPt));
      C.paintPreview(cv, sh, true, 0);
      return cv.toDataURL('image/png');
    } catch (_) { return null; }
  }
  const lastStage = sh => { const st = sh && sh.rosePlan && sh.rosePlan.stages; return st && st.length ? st[st.length - 1] : sh && sh.rosePlan ? { n: 1, at: null } : null; };
  const tagHtml = (n, at, extra) => `<b>${esc(n || 1)}</b><span>Green line ${esc(n || 1)}${at ? ' · ' + esc(when(at)) : ''}</span>${extra || ''}`;
  /** Puts the picture with its line over the card's preview (animated: wiped in from the left, or still). Returns { ov, tag(html), end } or null. */
  function lay(id, src, o) {
    try {
      css();
      const a = anchorOf(id, o && o.el); if (!a || !a.parentNode || !src) return null;
      for (const old of a.parentNode.querySelectorAll(':scope > .clLine')) old.remove();
      const ov = place(a, 'clLine' + (o && o.still ? ' still' : '')), cs = W.getComputedStyle ? getComputedStyle(a) : null;
      const img = doc.createElement('img'); img.src = src; img.alt = 'Sheet preview with its green dash line';
      if (cs && a.matches && a.matches('img')) Object.assign(img.style, { padding: cs.padding, borderWidth: cs.borderTopWidth, borderStyle: 'solid', borderColor: 'transparent', borderRadius: cs.borderRadius, backgroundColor: cs.backgroundColor });
      const tag = doc.createElement('span'); tag.className = 'clTag'; tag.innerHTML = tagHtml(o.n, o.at, o.extra);
      ov.append(img, tag);
      const on = () => ov.classList.add('on');
      if (o && o.still || reduced()) on(); else requestAnimationFrame(() => requestAnimationFrame(on));
      return { ov, tag: html => { tag.innerHTML = html; } };
    } catch (_) { return null; }
  }
  function keep(id, src, tag) { KEPT.delete(id); KEPT.set(id, { src, tag }); while (KEPT.size > KEEP) KEPT.delete(KEPT.keys().next().value); watch(); }
  let mo = null, queued = 0;
  function watch() {
    const body = doc.getElementById('libBody'); if (mo || !body || !W.MutationObserver) return;
    mo = new MutationObserver(() => { if (queued || !KEPT.size) return; queued = requestAnimationFrame(() => { queued = 0; restore(body); }); });
    mo.observe(body, { childList: true, subtree: true });
  }
  function restore(root) {
    try {
      root = root && root.querySelectorAll ? root : doc;
      for (const [id, k] of KEPT) {
        const c = [...root.querySelectorAll('.libCard[data-id]')].find(x => x.dataset.id === id); if (!c || c.querySelector(':scope > .clLine')) continue;
        const pv = c.querySelector('img.pv'); if (!pv || !pv.parentNode) continue;
        const l = lay(id, k.src, { still: true, n: k.tag.n, at: k.tag.at, el: c }); if (l) l.tag(tagHtml(k.tag.n, k.tag.at, TICK));
      }
    } catch (_) { /* the picture stays as it was saved */ }
  }
  function fail(ids, text, el) {
    css();
    try { if (W.CN && typeof W.CN.toast === 'function') W.CN.toast(text, 'bad'); } catch (_) { /* the card says it too */ }
    for (const id of ids || []) {
      try {
        const a = anchorOf(id, el); if (!a || !a.parentNode) continue;
        const ov = place(a, 'clErr'); ov.setAttribute('role', 'alert'); ov.textContent = text; ov.title = 'Click to dismiss';
        const gone = () => { try { ov.remove(); } catch (_) { /* gone */ } };
        ov.addEventListener('click', gone); setTimeout(gone, 12000);
      } catch (_) { /* the caller also says it */ }
    }
  }

  /* ── the window ── */
  const labelOf = (id, c) => { const hit = c && (c.sheets || []).find(s => s && s.sheetId === id); if (hit && hit.label) return hit.label; const r = rowOf(id) || liveOf(id); return r ? `${CODE[r.metal] || ''} Sheet ${r.sheetIndex || r.page || 1}`.trim() : 'A sheet'; };
  function shotOf(id) {
    const c = cardOf(id), pv = c && c.querySelector('img.pv');
    if (!pv || !pv.getAttribute('src')) return null;
    const im = pv.cloneNode(false); im.removeAttribute('data-sheet-preview'); im.removeAttribute('loading'); im.removeAttribute('class'); im.alt = 'Sheet preview';
    im.style.width = '100%'; im.style.height = 'auto'; im.style.display = 'block';
    return im;
  }
  function share(id) {
    const r = rowOf(id), sh = liveOf(id);
    const d = r && +r.density > 0 ? +r.density : sh ? (sh.sat && +sh.sat.fullPct) || +sh.density || 0 : 0;
    return d > 0 ? Math.max(1, Math.round(d * 100)) : 0;
  }
  function charmsOf(id, c) {
    const hit = c && (c.sheets || []).find(s => s && s.sheetId === id); if (hit && +hit.charms > 0) return +hit.charms;
    const r = rowOf(id), sh = liveOf(id); return (r && +r.placedCount) || (sh && (sh.placements || []).length) || 0;
  }
  async function ask(item, confirm, o) {
    try {
      o = o || {};
      const SOM = W.SharedOrdersModal; if (!SOM || typeof SOM.ask !== 'function') return null;
      const c = confirm || {}, ids = (c.sheetIds && c.sheetIds.length ? c.sheetIds : (c.sheets || []).map(s => s.sheetId)).filter(Boolean);
      if (!ids.length) return null;
      const dest = o.dest || 'Laser cutting', from = o.fromName || 'In progress', n = ids.length;
      const cards = ids.map(id => {
        const k = charmsOf(id, c), p = share(id);
        return { key: id, label: labelOf(id, c), shot: shotOf(id), tag: 'partial sheet',
          facts: [[k ? plural(k, 'charm') : 'Its charms', p ? `${p}% of the sheet` : '']],
          lines: [{ icon: 'dash', text: 'The system generates the green dash line around them and dates it.' }, { icon: 'lock', text: 'The cut is recorded with your name. A recorded cut is permanent.' }] };
      });
      const names = cards.map(x => x.label);
      return await SOM.ask({
        title: n === 1 ? [{ b: names[0] }, ' needs its green dash line'] : [{ b: plural(n, 'sheet') }, ' need their green dash line'],
        sub: n === 1 ? `It is a partial sheet. To move it to ${dest}, the system will generate the green dash line that completes it.`
          : `${o.what ? o.what + ' has' : 'These are'} partial sheets. To move ${o.what ? 'it' : 'them'} to ${dest}, the system will generate the green dash line that completes each one.`,
        chips: names.slice(0, 5).map(l => ({ label: l, here: true })).concat([{ label: dest }]),
        count: plural(n, 'sheet'), cards,
        note: `Cancel changes nothing: ${n === 1 ? names[0] + ' stays' : 'the sheets stay'} in ${from}.`,
        yes: n === 1 ? 'Generate the green dash line' : 'Generate the green dash lines', no: 'Cancel', from: o.from || undefined
      });
    } catch (e) { try { console.warn('[cut line window]', e && e.message || e); } catch (_) { /* quiet */ } return null; }
  }

  /* ── after the yes ── */
  async function make(o) {
    o = o || {}; css();
    const ids = (o.ids || []).filter(Boolean), shown = new Map();
    let last = 0;
    // one spinner on each card: it goes when that card's own line is drawn
    const spinners = new Map(ids.map(id => [id, busy([id], 'Working out the green dash line…', { el: o.el })]));
    const reveal = id => {
      if (shown.has(id)) return;
      const sh = liveOf(id); if (!sh || !sh.rosePlan) return;
      const a = anchorOf(id, o.el), w = Math.max(240, Math.round(((a && a.clientWidth) || 340) * Math.min(2, W.devicePixelRatio || 1)));
      const src = paint(sh, w); if (!src) return;
      const st = lastStage(sh) || { n: 1, at: null };
      const l = lay(id, src, { n: st.n, at: st.at, el: o.el, extra: '<i class="clSpin" aria-hidden="true"></i><span>Recording the cut…</span>' });
      if (!l) return;
      shown.set(id, { l, n: st.n, at: st.at, src }); last = Date.now();
      const sp = spinners.get(id); if (sp) sp.stop();
    };
    let res;
    try { res = await o.run(ev => { try { if (ev && ev.key === 'drawn' && ev.sheetId) reveal(ev.sheetId); } catch (_) { /* the line is shown at the end instead */ } }); }
    catch (e) { res = { ok: false, error: (e && e.message) || String(e) }; }
    res = res && typeof res === 'object' ? res : { ok: false, error: 'No answer came back, nothing was changed.' };
    if (res.ok !== false) for (const id of ids) reveal(id);      // (a line drawn without its step being heard is shown now)
    for (const sp of spinners.values()) sp.stop();
    // what was made stays shown (it is on record), dated and numbered, with a tick
    for (const [id, v] of shown) { v.l.tag(tagHtml(v.n, v.at, TICK)); keep(id, v.src, { n: v.n, at: v.at }); }
    if (res.ok === false) {
      // the card says why the rest was not made, in plain words, and that nothing was moved
      const todo = ids.filter(id => !shown.has(id));
      fail(todo.length ? todo : ids.slice(0, 1), `${String(res.error || 'The green dash line could not be made.').replace(/[.\s]+$/, '')}. Nothing was moved.`, o.el);
      return res;
    }
    // the line is SEEN before anything moves: it is wiped in, then held a moment to be read
    if (shown.size) await wait(Math.max(0, (reduced() ? READ : REVEAL + READ) - (Date.now() - last)));
    return res;
  }

  W.LibraryCutLine = { guess, ask, make, busy, restore, fail, version: '20261007-1' };
})();
