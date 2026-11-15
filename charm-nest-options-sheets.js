/*  charm-nest-options-sheets.js — the Sheets card of the Options window (SHEETSLIST, Paul, 15 Nov 2026).
 *  "I should be able to easily control in this screen: 1. consolidating sheets by dragging and dropping one sheet onto another. 2. deleting unnecessary sheets and easily being able to choose
 *   which sheet is to be draft 1, then which sheet is to be draft 2 etc. 3. The user should be able to choose sheet 1 2 3 4 in whatever order they decide by dragging the sheets around ..."
 *  Lists every sheet of a 10K / 14K run (window.Gate.sheetsOf), and asks the Gate to do what is done: Gate.reorder (drag between cards, or Alt+Up / Alt+Down on a focused card), Gate.consolidate
 *  (drop on the middle of a card; a person presses Move to confirm) and Gate.deleteEmpty (an empty draft). This file draws and listens; it writes nothing itself.
 *  window.OptionsSheets: mount(M) -> element | null · paint(M) · destroy(M) */
(function (root) {
  'use strict';
  const doc = root.document; if (!doc || root.OptionsSheets) return;
  const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const G = () => root.Gate && typeof root.Gate.sheetsOf === 'function' ? root.Gate : null;
  const solid = m => m === 'gold10k' || m === 'gold14k';
  const CSS = `
.osSheets .osShList{list-style:none;margin:0;padding:0;display:flex;flex-direction:column;gap:8px}
.osSh{position:relative;display:flex;align-items:center;gap:12px;flex-wrap:wrap;padding:11px 14px;border:1px solid var(--line);border-radius:12px;background:var(--paper2,transparent);outline:none;transition:box-shadow .14s,border-color .14s,transform .14s}
.osSh:focus-visible{box-shadow:0 0 0 2px var(--gold2)}
.osShGrip{flex:0 0 auto;width:26px;height:26px;border:0;border-radius:8px;background:transparent;color:var(--ink45,#888);cursor:grab;display:inline-grid;place-items:center;touch-action:none;font-size:15px;line-height:1}
.osShGrip:hover{background:var(--card);color:var(--ink)}
.osSh.fixed .osShGrip{opacity:.25;cursor:not-allowed}
.osShName{font-weight:600;min-width:84px}
.osShMeta{font-size:12.5px;color:var(--ink45,#777);flex:1 1 auto}
.osShBadges{display:flex;gap:6px;flex-wrap:wrap}
.osShBadge{font-size:11px;padding:2px 8px;border-radius:99px;border:1px solid var(--line);color:var(--ink45,#777)}
.osShBar{flex:0 0 100%;height:4px;border-radius:3px;background:var(--line);overflow:hidden}
.osShBar i{display:block;height:100%;background:var(--gold2,#b8913f)}
.osShNote{flex:0 0 100%;font-size:12px;color:var(--ink45,#777)}
.osShNote.bad{color:#b3402e}
.osSh.dragging{opacity:.92;z-index:5;box-shadow:0 14px 34px rgba(30,24,16,.25);pointer-events:none;transition:none}
.osSh.onto{border-color:var(--gold2);box-shadow:0 0 0 2px var(--gold2)}
.osSh.onto.refused{border-color:#b3402e;box-shadow:0 0 0 2px #b3402e}
.osSh.before::before,.osSh.after::after{content:"";position:absolute;left:8px;right:8px;height:3px;border-radius:2px;background:var(--gold2)}
.osSh.before::before{top:-6px}.osSh.after::after{bottom:-6px}
.osShTag{position:absolute;right:12px;top:-10px;font-size:11.5px;font-weight:600;padding:2px 9px;border-radius:99px;background:var(--gold2,#b8913f);color:#fff;pointer-events:none}
.osShTag.refused{background:#b3402e}
.osShAsk{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-top:12px;font-size:13px}
.osShSaid{margin:12px 0 0;font-size:13px}
html.osShDrag,html.osShDrag *{cursor:grabbing!important;user-select:none!important}`;
  let styled = false;
  const style = () => { if (styled) return; styled = true; const s = doc.createElement('style'); s.textContent = CSS; doc.head.appendChild(s); };

  function mount(M) {
    if (!M || !solid(M.m) || !G()) return null;
    style();
    const el = doc.createElement('section'); el.className = 'osCard osSheets'; el.dataset.card = 'sheets'; el.setAttribute('aria-label', 'Sheets'); el.hidden = true;
    el.innerHTML = `<header class="osCardHead"><div><h3 class="osCardTitle">Sheets</h3><p class="osLead" data-sh="lead">Drag a sheet to put the sheets in the order you want (Draft 1 is the top one). Drop a sheet on the middle of another to move its pieces onto it.</p></div></header><ol class="osShList" data-sh="list"></ol><div class="osShAsk" data-sh="ask" hidden></div><p class="osShSaid" data-sh="said" role="status" hidden></p>`;
    M.sheetsUI = { el, sig: '', drag: null, ask: null, note: {} };
    const list = el.querySelector('[data-sh="list"]');
    list.addEventListener('pointerdown', e => down(M, e));
    list.addEventListener('keydown', e => key(M, e));
    list.addEventListener('click', e => { const b = e.target.closest && e.target.closest('[data-sh-del]'); if (b) del(M, b.closest('li').dataset.id); });
    return el;
  }
  const rowsOf = M => { try { return G().sheetsOf(M.m); } catch (_) { return []; } };
  function paint(M, force) {
    const S = M && M.sheetsUI; if (!S || !G()) return;
    if (S.drag || S.ask) return;
    const rows = rowsOf(M), said = G().sheetsSaid ? G().sheetsSaid(M.m) : null, showSaid = said && Date.now() - said.at < 600000 ? said.text : '';
    S.el.hidden = rows.length < 2;
    const sig = JSON.stringify([rows, showSaid, S.note]);
    if (!force && sig === S.sig) return; S.sig = sig;
    const arrange = rows.length ? rows[0].arrange : true;
    S.el.querySelector('[data-sh="lead"]').textContent = arrange ? 'Drag a sheet to put the sheets in the order you want (Draft 1 is the top one). Drop a sheet on the middle of another to move its pieces onto it; what does not fit goes on to the next sheet below.' : 'Sheets are arranged on the computer that carries the run.';
    const keepFocus = S.el.contains(doc.activeElement) && doc.activeElement.closest ? (doc.activeElement.closest('li') || {}).dataset : null;
    S.el.querySelector('[data-sh="list"]').innerHTML = rows.map(r => {
      const badges = [r.inSet ? 'In set' : '', r.cutLine ? 'Cut line drawn' : '', r.cut ? 'Cut' : '', r.locked ? 'Locked' : ''].filter(Boolean).map(b => `<span class="osShBadge">${b}</span>`).join('');
      const note = S.note[r.id] || (r.canDelete ? '' : r.whyDelete && /^Move its/.test(r.whyDelete) ? r.whyDelete : '');
      return `<li class="osSh${r.movable ? '' : ' fixed'}" data-id="${esc(r.id)}" tabindex="0" aria-label="${esc(r.name)}, ${r.pieces} pieces${r.pct == null ? '' : ', ' + r.pct + ' percent full'}. Alt plus Up or Down arrow moves it.">
        <button type="button" class="osShGrip" tabindex="-1" aria-hidden="true" title="${r.movable ? 'Drag to move' : 'This sheet keeps its place'}">&#8942;&#8942;</button>
        <span class="osShName">${esc(r.name)}</span><span class="osShMeta">${esc(r.size)} · ${r.placed} of ${r.pieces} pieces placed${r.pct == null ? '' : ' · ' + r.pct + '% full'}${r.busy ? ' · working…' : ''}</span>
        <span class="osShBadges">${badges}</span>${r.canDelete ? '<button type="button" class="btn ghost xs" data-sh-del>Delete</button>' : ''}
        <span class="osShBar" aria-hidden="true"><i style="width:${r.pct == null ? 0 : r.pct}%"></i></span>${note ? `<span class="osShNote${S.note[r.id] ? ' bad' : ''}">${esc(note)}</span>` : ''}</li>`;
    }).join('');
    const sd = S.el.querySelector('[data-sh="said"]'); sd.hidden = !showSaid; sd.textContent = showSaid;
    if (keepFocus && keepFocus.id) { const n = [...S.el.querySelectorAll('li')].find(x => x.dataset.id === keepFocus.id); if (n) { try { n.focus({ preventScroll: true }); } catch (_) {} } }
  }
  const fail = (M, id, why) => { const S = M.sheetsUI; S.note[id] = why; paint(M, true); setTimeout(() => { if (S.note[id] === why) { delete S.note[id]; paint(M, true); } }, 6000); };

  /* ── keyboard: Alt+ArrowUp / Alt+ArrowDown on a focused card ── */
  function key(M, e) {
    if (!e.altKey || (e.key !== 'ArrowUp' && e.key !== 'ArrowDown')) return;
    const li = e.target.closest && e.target.closest('li.osSh'); if (!li) return; e.preventDefault();
    const ids = [...M.sheetsUI.el.querySelectorAll('li.osSh')].map(x => x.dataset.id), i = ids.indexOf(li.dataset.id), j = i + (e.key === 'ArrowUp' ? -1 : 1);
    if (j < 0 || j >= ids.length) return;
    ids.splice(i, 1); ids.splice(j, 0, li.dataset.id);
    const r = G().reorder(M.m, ids); if (r && r.ok === false) return fail(M, li.dataset.id, r.why);
    paint(M, true); const n = [...M.sheetsUI.el.querySelectorAll('li')].find(x => x.dataset.id === li.dataset.id); if (n) n.focus({ preventScroll: true });
  }

  /* ── pointer: a grip drags at once; the card itself after 6 px ── */
  function down(M, e) {
    if (e.button != null && e.button > 0) return;
    const li = e.target.closest && e.target.closest('li.osSh'); if (!li || (e.target.closest && e.target.closest('[data-sh-del]'))) return;
    const row = rowsOf(M).find(r => r.id === li.dataset.id); if (!row || !row.movable) return;
    const S = M.sheetsUI, st = { li, id: row.id, x: e.clientX, y: e.clientY, on: false, rows: null, over: null, mode: null, pid: e.pointerId };
    const grip = !!(e.target.closest && e.target.closest('.osShGrip'));
    const move = ev => {
      if (!st.on) { if (!grip && Math.hypot(ev.clientX - st.x, ev.clientY - st.y) < 6) return; st.on = true; S.drag = st; st.rows = rowsOf(M); li.classList.add('dragging'); doc.documentElement.classList.add('osShDrag'); }
      li.style.transform = `translate(${ev.clientX - st.x}px,${ev.clientY - st.y}px)`; target(M, st, ev);
    };
    const end = ev => {
      doc.removeEventListener('pointermove', move, true); doc.removeEventListener('pointerup', end, true); doc.removeEventListener('pointercancel', end, true);
      if (!st.on) return; doc.documentElement.classList.remove('osShDrag'); li.classList.remove('dragging'); li.style.transform = ''; S.drag = null;
      const over = st.over, mode = st.mode; clear(M);
      if (ev.type === 'pointercancel' || !over) return paint(M, true);
      drop(M, st, over, mode);
    };
    doc.addEventListener('pointermove', move, true); doc.addEventListener('pointerup', end, true); doc.addEventListener('pointercancel', end, true);
  }
  const clear = M => { for (const n of M.sheetsUI.el.querySelectorAll('.onto,.before,.after,.refused')) n.classList.remove('onto', 'before', 'after', 'refused'); for (const t of M.sheetsUI.el.querySelectorAll('.osShTag')) t.remove(); };
  function target(M, st, ev) {
    clear(M); st.over = null; st.mode = null;
    const cards = [...M.sheetsUI.el.querySelectorAll('li.osSh')].filter(n => n !== st.li);
    const hit = cards.find(n => { const r = n.getBoundingClientRect(); return ev.clientY >= r.top - 4 && ev.clientY <= r.bottom + 4 && ev.clientX >= r.left && ev.clientX <= r.right; });
    if (!hit) return;
    const r = hit.getBoundingClientRect(), f = (ev.clientY - r.top) / Math.max(1, r.height), row = st.rows.find(x => x.id === hit.dataset.id), me = st.rows.find(x => x.id === st.id);
    if (f > 0.28 && f < 0.72) {
      const why = !me.canSource ? `${me.name} ${me.whySource}` : !row.canTarget ? `${row.name} ${row.whyTarget}` : '';
      hit.classList.add('onto'); if (why) hit.classList.add('refused');
      const tag = doc.createElement('span'); tag.className = 'osShTag' + (why ? ' refused' : ''); tag.textContent = why || `Move onto ${row.name}`; hit.appendChild(tag);
      st.over = hit.dataset.id; st.mode = why ? 'refused:' + why : 'onto';
    } else if (row.movable || true) {
      const before = f <= 0.28; hit.classList.add(before ? 'before' : 'after'); st.over = hit.dataset.id; st.mode = before ? 'before' : 'after';
    }
  }
  function drop(M, st, overId, mode) {
    const S = M.sheetsUI;
    if (mode && mode.startsWith('refused:')) return fail(M, st.id, mode.slice(8));
    if (mode === 'onto') {
      const src = st.rows.find(r => r.id === st.id), tgt = st.rows.find(r => r.id === overId);
      S.ask = { src, tgt }; const ask = S.el.querySelector('[data-sh="ask"]'); ask.hidden = false;
      ask.innerHTML = `<span>Move ${src.pieces} piece${src.pieces === 1 ? '' : 's'} from ${esc(src.name)} onto ${esc(tgt.name)}? What does not fit goes on to the next sheets below it, then stays on ${esc(src.name)}.</span><button type="button" class="btn ghost xs" data-sh-no>Cancel</button><button type="button" class="btn sage xs" data-sh-yes>Move</button>`;
      ask.querySelector('[data-sh-no]').onclick = () => closeAsk(M);
      ask.querySelector('[data-sh-yes]').onclick = async () => { const a = S.ask; closeAsk(M); const r = await G().consolidate(a.src.id, a.tgt.id); if (r && r.ok === false && r.why) fail(M, a.src.id, r.why); paint(M, true); };
      ask.querySelector('[data-sh-yes]').focus();
      return;
    }
    const ids = [...S.el.querySelectorAll('li.osSh')].map(x => x.dataset.id).filter(id => id !== st.id), at = ids.indexOf(overId);
    ids.splice(mode === 'before' ? at : at + 1, 0, st.id);
    const r = G().reorder(M.m, ids); if (r && r.ok === false) fail(M, st.id, r.why); else paint(M, true);
  }
  function closeAsk(M) { const S = M.sheetsUI; S.ask = null; const ask = S.el.querySelector('[data-sh="ask"]'); ask.hidden = true; ask.innerHTML = ''; paint(M, true); }

  /* ── delete (an empty sheet only; one confirmation) ── */
  function del(M, id) {
    const S = M.sheetsUI, row = rowsOf(M).find(r => r.id === id); if (!row || !row.canDelete) return;
    S.ask = { del: id }; const ask = S.el.querySelector('[data-sh="ask"]'); ask.hidden = false;
    ask.innerHTML = `<span>Delete ${esc(row.name)}? It is empty. The sheets after it move up one.</span><button type="button" class="btn ghost xs" data-sh-no>Cancel</button><button type="button" class="btn sage xs" data-sh-yes>Delete</button>`;
    ask.querySelector('[data-sh-no]').onclick = () => closeAsk(M);
    ask.querySelector('[data-sh-yes]').onclick = async () => { closeAsk(M); const r = await G().deleteEmpty(id); if (r && r.ok === false) fail(M, id, r.why); paint(M, true); };
    ask.querySelector('[data-sh-yes]').focus();
  }
  function destroy(M) { try { if (M && M.sheetsUI) { doc.documentElement.classList.remove('osShDrag'); M.sheetsUI.el.remove(); M.sheetsUI = null; } } catch (_) {} }
  root.OptionsSheets = { mount, paint, destroy };
})(window);
